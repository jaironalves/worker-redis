using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Globalization;
using System.Text.Json;
using Worker.Redis.SiloService.Redis;

namespace Worker.Redis.SiloService.Services;

public sealed class RedisService(
    IConnectionMultiplexer multiplexer,
    IOptions<RedisServiceOptions> options,
    ILogger<RedisService> logger) : IRedisService
{
    private readonly IDatabase _database = multiplexer.GetDatabase();
    private readonly RedisServiceOptions _options = options.Value;
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public TimeSpan LeaderRenewInterval =>
        _options.LeaderRenewInterval;

    // =========================================================
    // LEADER
    // =========================================================

    public async Task<long?> LeaderTryAcquireAsync(string token,
        int revision)
    {
        var result = await _database.ScriptEvaluateAsync(
            RedisScripts.AcquireLeaderScript,
            [
                RedisKeys.Leader,
                RedisKeys.LeaderGeneration
            ],
            [
                token,
                revision,
                (long)_options.LeaderLeaseDuration.TotalMilliseconds
            ]);

        var generation = (long)result;

        if (generation == 0)
            return null;

        logger.LogInformation(
            "Liderança adquirida. Generation: {Generation}",
            generation);

        return generation;
    }

    public async Task<bool> LeaderRenewAsync(string token)
    {
        var result = await _database.ScriptEvaluateAsync(
            RedisScripts.RenewLeaderScript,
            [
                RedisKeys.Leader
            ],
            [
                token,
                (long)_options.LeaderLeaseDuration.TotalMilliseconds
            ]);

        return (long)result == 1;
    }

    public async Task<bool> LeaderReleaseAsync(string token)
    {
        var result = await _database.ScriptEvaluateAsync(
            RedisScripts.ReleaseLeaderScript,
            [
                RedisKeys.Leader
            ],
            [
                token
            ]);

        return (long)result == 1;
    }

    // =========================================================
    // WORKERS
    // =========================================================

    public async Task RegisterWorkerAsync(string workerId)
    {
        var workerKey = RedisKeys.Worker(workerId);

        await _database.StringSetAsync(
            workerKey,
            workerId,
            _options.WorkerHeartbeatTtl);

        await _database.SetAddAsync(
            RedisKeys.AliveWorkers,
            workerId);
    }

    public async Task SetWorkerReadyAsync(string workerId)
    {
        var workerKey = RedisKeys.Worker(workerId);

        var exists = await _database.KeyExistsAsync(workerKey);

        if (!exists)
        {
            throw new InvalidOperationException(
                $"Worker {workerId} não possui heartbeat ativo.");
        }

        await _database.SetAddAsync(
            RedisKeys.ReadyWorkers,
            workerId);

        logger.LogInformation(
            "Worker {WorkerId} está pronto.",
            workerId);
    }

    public async Task<IReadOnlyList<string>> GetAliveWorkersAsync()
    {
        var workers = await _database.SetMembersAsync(
            RedisKeys.AliveWorkers);

        var alive = new List<string>();

        foreach (var worker in workers)
        {
            var workerId = worker.ToString();

            if (await _database.KeyExistsAsync(
                    RedisKeys.Worker(workerId)))
            {
                alive.Add(workerId);
            }
            else
            {
                await RemoveDeadWorkerAsync(workerId);
            }
        }

        return alive;
    }

    public async Task<IReadOnlyList<string>> GetReadyWorkersAsync()
    {
        var workers = await _database.SetMembersAsync(
            RedisKeys.ReadyWorkers);

        if (workers.Length == 0)
            return [];

        var checks = workers.Select(async worker =>
        {
            var workerId = worker.ToString();

            var exists = await _database.KeyExistsAsync(
                RedisKeys.Worker(workerId));

            return (WorkerId: workerId, Alive: exists);
        });

        var results = await Task.WhenAll(checks);

        var alive = new List<string>();

        foreach (var result in results)
        {
            if (result.Alive)
            {
                alive.Add(result.WorkerId);
            }
            else
            {
                await RemoveDeadWorkerAsync(result.WorkerId);
            }
        }

        return alive;
    }

    private async Task RemoveDeadWorkerAsync(string workerId)
    {
        await _database.SetRemoveAsync(
            RedisKeys.AliveWorkers,
            workerId);

        await _database.SetRemoveAsync(
            RedisKeys.ReadyWorkers,
            workerId);

        logger.LogInformation(
            "Worker {WorkerId} removido por ausência de heartbeat.",
            workerId);
    }

    // =========================================================
    // ASSETS
    // =========================================================

    public async Task<IReadOnlyList<string>> GetAssetsAsync()
    {
        var assets = await _database.SetMembersAsync(
            RedisKeys.Assets);

        return assets
            .Select(x => x.ToString())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .ToArray();
    }

    // =========================================================
    // PARTITIONS
    // =========================================================

    public async Task<IReadOnlyList<AssetPartition>> GetPartitionsAsync()
    {
        var entries = await _database.HashGetAllAsync(
            RedisKeys.Catalog);

        var partitions = new List<AssetPartition>(entries.Length);

        foreach (var entry in entries)
        {
            var partition = JsonSerializer.Deserialize<AssetPartition>(
                entry.Value.ToString(),
                JsonOptions);

            if (partition is not null)
                partitions.Add(partition);
        }

        return partitions;
    }

    public async Task SavePartitionAsync(
        AssetPartition partition,
        LeadershipSession session)
    {
        var json = JsonSerializer.Serialize(partition, JsonOptions);

        var result = await _database.ScriptEvaluateAsync(
            RedisScripts.SavePartitionScript,
            [
                RedisKeys.Leader,
                RedisKeys.LeaderGeneration,
                RedisKeys.Catalog
            ],
            [
                session.Token,
                GenerationValue(session),
                partition.Id,
                json
            ]);

        EnsureLeadership(result, session);

        logger.LogDebug(
            "Partição {PartitionId} salva. Version: {Version}",
            partition.Id,
            partition.Version);
    }

    public async Task RemovePartitionAsync(
        string partitionId,
        LeadershipSession session)
    {
        var result = await _database.ScriptEvaluateAsync(
            RedisScripts.RemovePartitionScript,
            [
                RedisKeys.Leader,
                RedisKeys.LeaderGeneration,
                RedisKeys.Catalog
            ],
            [
                session.Token,
                GenerationValue(session),
                partitionId
            ]);

        EnsureLeadership(result, session);

        logger.LogDebug(
            "Partição {PartitionId} removida.",
            partitionId);
    }

    // =========================================================
    // ASSIGNMENTS
    // =========================================================

    public async Task<IReadOnlyList<PartitionAssignment>> GetAssignmentsAsync()
    {
        var entries = await _database.HashGetAllAsync(
            RedisKeys.Assignments);

        var assignments = new List<PartitionAssignment>(
            entries.Length);

        foreach (var entry in entries)
        {
            var assignment =
                JsonSerializer.Deserialize<PartitionAssignment>(
                    entry.Value.ToString(),
                    JsonOptions);

            if (assignment is not null)
                assignments.Add(assignment);
        }

        return assignments;
    }

    public async Task SaveAssignmentAsync(
        PartitionAssignment assignment,
        LeadershipSession session)
    {
        var json = JsonSerializer.Serialize(
            assignment,
            JsonOptions);

        var result = await _database.ScriptEvaluateAsync(
            RedisScripts.SaveAssignmentScript,
            [
                RedisKeys.Leader,
                RedisKeys.LeaderGeneration,
                RedisKeys.Assignments
            ],
            [
                session.Token,
                GenerationValue(session),
                assignment.PartitionId,
                json
            ]);

        EnsureLeadership(result, session);

        logger.LogDebug(
            "Assignment da partição {PartitionId} salvo para worker {WorkerId}.",
            assignment.PartitionId,
            assignment.WorkerId);
    }

    public async Task RemoveAssignmentAsync(
        string partitionId,
        LeadershipSession session)
    {
        var result = await _database.ScriptEvaluateAsync(
            RedisScripts.RemoveAssignmentScript,
            [
                RedisKeys.Leader,
                RedisKeys.LeaderGeneration,
                RedisKeys.Assignments
            ],
            [
                session.Token,
                GenerationValue(session),
                partitionId
            ]);

        EnsureLeadership(result, session);

        logger.LogDebug(
            "Assignment da partição {PartitionId} removido.",
            partitionId);
    }

    public async Task<IReadOnlyList<PartitionAssignment>>
        GetWorkerAssignmentsAsync(string workerId)
    {
        var assignments = await GetAssignmentsAsync();

        return assignments
            .Where(x => string.Equals(
                x.WorkerId,
                workerId,
                StringComparison.OrdinalIgnoreCase))
            .ToArray();
    }

    // =========================================================
    // PUB/SUB
    // =========================================================

    public async Task PublishAssignmentChangedAsync(
        string partitionId,
        LeadershipSession session)
    {
        return Task.CompletedTask;

        var result = await _database.ScriptEvaluateAsync(
            RedisScripts.PublishAssignmentScript,
            [
                RedisKeys.Leader,
                RedisKeys.LeaderGeneration
            ],
            [
                session.Token,
                GenerationValue(session),
                RedisKeys.AssignmentChannel,
                partitionId
            ]);

        EnsureLeadership(result, session);
    }

    // =========================================================
    // HELPERS
    // =========================================================

    private static RedisValue GenerationValue(
        LeadershipSession session)
    {
        return session.Generation.ToString(
            CultureInfo.InvariantCulture);
    }

    private static void EnsureLeadership(
        RedisResult result,
        LeadershipSession session)
    {
        var value = (long)result;

        if (value == -1)
        {
            throw new LeadershipLostException(
                $"Gravação rejeitada. " +
                $"Token ou Generation inválido. " +
                $"Generation da sessão: {session.Generation}.");
        }

        if (value != 1 && value < 0)
        {
            throw new InvalidOperationException(
                $"Resultado inesperado do Redis: {value}.");
        }
    }
}
