using StackExchange.Redis;
using System.Text.Json;

namespace Worker.Redis.SiloService.Services;

internal sealed class RedisService(
    IConnectionMultiplexer multiplexer) : IRedisService
{
    private readonly IDatabase _redisDb = multiplexer.GetDatabase();
    private readonly ISubscriber _subscriber = multiplexer.GetSubscriber();

    private const string LeaderKey = "md:leader";

    private const string WorkerKeyPrefix = "md:worker:";
    private const string WorkersAliveKey = "md:workers:alive";
    private const string WorkersReadyKey = "md:workers:ready";

    private const string CatalogKey = "md:catalog:partitions";
    private const string AssignmentsKey = "md:assignments";

    private const string AssignmentChannel = "md:assignment:changed";

    private static readonly TimeSpan LeaderLeaseTime =
        TimeSpan.FromSeconds(15);

    private static readonly TimeSpan WorkerTtl =
        TimeSpan.FromSeconds(15);

    private static readonly JsonSerializerOptions JsonOptions =
        new(JsonSerializerDefaults.Web);

    public TimeSpan LeaderRenewInterval =>
        TimeSpan.FromSeconds(5);

    // =========================================================
    // LEADER
    // =========================================================

    public Task<bool> LeaderTryAcquireAsync(string token)
    {
        return _redisDb.LockTakeAsync(
            LeaderKey,
            token,
            LeaderLeaseTime);
    }

    public Task<bool> LeaderRenewAsync(string token)
    {
        return _redisDb.LockExtendAsync(
            LeaderKey,
            token,
            LeaderLeaseTime);
    }

    public Task<bool> LeaderReleaseAsync(string token)
    {
        return _redisDb.LockReleaseAsync(
            LeaderKey,
            token);
    }

    // =========================================================
    // WORKERS
    // =========================================================

    public async Task RegisterWorkerAsync(string workerId)
    {
        var batch = _redisDb.CreateBatch();

        var workerTask = batch.StringSetAsync(
            $"{WorkerKeyPrefix}{workerId}",
            "alive",
            WorkerTtl);

        var setTask = batch.SetAddAsync(
            WorkersAliveKey,
            workerId);

        batch.Execute();

        await Task.WhenAll(workerTask, setTask);
    }

    public Task SetWorkerReadyAsync(string workerId)
    {
        return _redisDb.SetAddAsync(
            WorkersReadyKey,
            workerId);
    }

    public Task<IReadOnlyList<string>> GetAliveWorkersAsync()
    {
        return GetWorkersFromSetAsync(
            WorkersAliveKey,
            removeFromReadySet: false);
    }

    public Task<IReadOnlyList<string>> GetReadyWorkersAsync()
    {
        return GetWorkersFromSetAsync(
            WorkersReadyKey,
            removeFromReadySet: true);
    }

    public async Task<IReadOnlyList<PartitionAssignment>>    GetWorkerAssignmentsAsync(string workerId)
    {
        var assignments = await GetAssignmentsAsync();

        return assignments
            .Where(x => x.WorkerId == workerId)
            .ToList();
    }

    private async Task<IReadOnlyList<string>> GetWorkersFromSetAsync(
        string setKey,
        bool removeFromReadySet)
    {
        var workers = await _redisDb.SetMembersAsync(setKey);

        if (workers.Length == 0)
            return [];

        var batch = _redisDb.CreateBatch();

        var tasks = workers
            .Select(worker =>
                batch.KeyExistsAsync(
                    $"{WorkerKeyPrefix}{worker}"))
            .ToArray();

        batch.Execute();

        var exists = await Task.WhenAll(tasks);

        var result = new List<string>();

        for (var i = 0; i < workers.Length; i++)
        {
            var workerId = workers[i].ToString();

            if (exists[i])
            {
                result.Add(workerId);
                continue;
            }

            await _redisDb.SetRemoveAsync(
                setKey,
                workers[i]);

            if (removeFromReadySet)
            {
                await _redisDb.SetRemoveAsync(
                    WorkersAliveKey,
                    workers[i]);
            }
        }

        return result;
    }

    // =========================================================
    // CATALOG
    // =========================================================

    public async Task<IReadOnlyList<AssetPartition>> GetPartitionsAsync()
    {
        var entries = await _redisDb.HashGetAllAsync(CatalogKey);

        return [.. entries
            .Where(x => x.Value.HasValue)
            .Select(x =>
                JsonSerializer.Deserialize<AssetPartition>(
                    x.Value.ToString(),
                    JsonOptions))
            .Where(x => x is not null)
            .Cast<AssetPartition>()];
    }

    public Task SavePartitionAsync(AssetPartition partition)
    {
        var json = JsonSerializer.Serialize(
            partition,
            JsonOptions);

        return _redisDb.HashSetAsync(
            CatalogKey,
            partition.Id,
            json);
    }

    public Task RemovePartitionAsync(string partitionId)
    {
        return _redisDb.HashDeleteAsync(
            CatalogKey,
            partitionId);
    }

    // =========================================================
    // ASSIGNMENTS
    // =========================================================

    public async Task<IReadOnlyList<PartitionAssignment>> GetAssignmentsAsync()
    {
        var entries = await _redisDb.HashGetAllAsync(
            AssignmentsKey);

        return entries
            .Where(x => x.Value.HasValue)
            .Select(x =>
                JsonSerializer.Deserialize<PartitionAssignment>(
                    x.Value.ToString(),
                    JsonOptions))
            .Where(x => x is not null)
            .Cast<PartitionAssignment>()
            .ToArray();
    }

    public Task SaveAssignmentAsync(
        PartitionAssignment assignment)
    {
        var json = JsonSerializer.Serialize(
            assignment,
            JsonOptions);

        return _redisDb.HashSetAsync(
            AssignmentsKey,
            assignment.PartitionId,
            json);
    }

    public Task RemoveAssignmentAsync(string partitionId)
    {
        return _redisDb.HashDeleteAsync(
            AssignmentsKey,
            partitionId);
    }

    // =========================================================
    // NOTIFICATIONS
    // =========================================================

    public Task PublishAssignmentChangedAsync(string partitionId)
    {
        return _subscriber.PublishAsync(
            AssignmentChannel,
            partitionId);
    }
}
