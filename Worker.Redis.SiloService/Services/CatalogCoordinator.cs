using System.Security.Cryptography;
using System.Text;
using Worker.Redis.SiloService;

namespace Worker.Redis.SiloService.Services;

internal sealed class CatalogCoordinator(
    IRedisService redisService,
    IAssetCache assetCache,
    ILogger<CatalogCoordinator> logger)
{
    private const int MaxAssetsPerPartition = 4;

    // Limite saudável: apenas para alertas.
    private const int HealthyPartitionsPerWorker = 100;

    // Limite de transferências por execução.
    private const int MaxPartitionsToMovePerCycle = 2;

    private static long _lastGuidTimestamp = DateTime.UtcNow.Ticks;

    private long? _lastAssetVersion;
    private string? _lastCatalogSignature;

    public async Task ReconcileAsync(
        LeadershipSession session,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var snapshot = assetCache.GetSnapshot();

        var assetsChanged =
            _lastAssetVersion != snapshot.Version;

        IReadOnlyList<AssetPartition> partitions;

        if (assetsChanged)
        {
            partitions = await ReconcilePartitionsAsync(
                session,
                snapshot.Assets,
                cancellationToken);

            _lastAssetVersion = snapshot.Version;
        }
        else
        {
            partitions = await redisService.GetPartitionsAsync();
        }

        cancellationToken.ThrowIfCancellationRequested();

        var workers = await redisService.GetReadyWorkersAsync();
        await ReconcileAssignmentsAsync(
            session,
            partitions,
            workers,            
            cancellationToken);
    }

    private static string CalculateCatalogSignature(
        IReadOnlyList<string> assets)
    {
        var normalized = assets
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim().ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x, StringComparer.OrdinalIgnoreCase);

        var input = $"{MaxAssetsPerPartition}|{string.Join("|", normalized)}";

        var hash = SHA256.HashData(
            Encoding.UTF8.GetBytes(input));

        return Convert.ToHexString(hash);
    }

    // -------------------------------------------------------
    // Catálogo de slots
    // -------------------------------------------------------

    private static string NewSequentialGuid()
    {
        long timestamp = DateTime.UtcNow.Ticks;

        if (timestamp <= _lastGuidTimestamp)
        {
            timestamp = _lastGuidTimestamp + 1;
        }

        _lastGuidTimestamp = timestamp;

        var guidBytes = new byte[16];
        Array.Copy(BitConverter.GetBytes(timestamp), 0, guidBytes, 0, 8);
        Array.Copy(Guid.NewGuid().ToByteArray(), 0, guidBytes, 8, 8);

        return new Guid(guidBytes).ToString("N");
    }

    private async Task<IReadOnlyList<AssetPartition>> ReconcilePartitionsAsync(
    LeadershipSession leadershipSession,
    IReadOnlyList<string> assets,
    CancellationToken cancellationToken)
    {
        var existing = await redisService.GetPartitionsAsync();
                
        var existingPartitions = existing
            .OrderBy(x => x.Id, StringComparer.Ordinal)
            .ToList();

        var desiredAssets = assets
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        var desiredSet = desiredAssets.ToHashSet(
            StringComparer.OrdinalIgnoreCase);

        var retainedAssets = new HashSet<string>(
            StringComparer.OrdinalIgnoreCase);

        var compositions = new List<(AssetPartition Original, List<string> Assets)>();

        // 1. Preserva os ativos existentes.
        foreach (var partition in existingPartitions)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var retained = partition.Assets
                .Where(asset =>
                    desiredSet.Contains(asset) &&
                    retainedAssets.Add(asset))
                .ToList();

            compositions.Add((partition, retained));
        }

        // 2. Se alguma partição ultrapassou a capacidade,
        // seus excedentes ficam disponíveis para realocação.
        var overflow = new Queue<string>();

        foreach (var composition in compositions)
        {
            if (composition.Assets.Count <= MaxAssetsPerPartition)
                continue;

            var excess = composition.Assets
                .Skip(MaxAssetsPerPartition)
                .ToList();

            composition.Assets.RemoveRange(
                MaxAssetsPerPartition,
                composition.Assets.Count - MaxAssetsPerPartition);

            foreach (var asset in excess)
                overflow.Enqueue(asset);
        }

        // 3. Compacta: as primeiras partições puxam ativos
        // das últimas partições.
        for (var i = 0; i < compositions.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var destination = compositions[i].Assets;

            var sourceIndex = compositions.Count - 1;

            while (destination.Count < MaxAssetsPerPartition &&
                   sourceIndex > i)
            {
                var source = compositions[sourceIndex].Assets;

                if (source.Count == 0)
                {
                    sourceIndex--;
                    continue;
                }

                var asset = source[^1];

                source.RemoveAt(source.Count - 1);
                destination.Add(asset);
            }
        }

        // 4. Ativos excedentes e ativos novos.
        var remaining = new Queue<string>(
            overflow.Concat(
                desiredAssets.Where(x => !retainedAssets.Contains(x))));

        var result = new List<AssetPartition>();

        // 5. Preenche eventuais espaços restantes.
        foreach (var composition in compositions)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var finalAssets = composition.Assets;

            while (finalAssets.Count < MaxAssetsPerPartition &&
                   remaining.Count > 0)
            {
                finalAssets.Add(remaining.Dequeue());
            }

            if (finalAssets.Count == 0)
            {
                await redisService.RemovePartitionAsync(
                    composition.Original.Id, leadershipSession);

                logger.LogInformation(
                    "Slot {PartitionId} removido após compactação.",
                    composition.Original.Id);

                continue;
            }

            var changed = !composition.Original.Assets.SequenceEqual(
                finalAssets,
                StringComparer.OrdinalIgnoreCase);

            var partition = changed
                ? composition.Original with
                {
                    Version = composition.Original.Version + 1,
                    Assets = finalAssets
                }
                : composition.Original;

            if (changed)
            {
                await redisService.SavePartitionAsync(partition, leadershipSession);

                logger.LogInformation(
                    "Slot {PartitionId} atualizado. Ativos: {Count}. Versão: {Version}.",
                    partition.Id,
                    partition.Assets.Count,
                    partition.Version);
            }

            result.Add(partition);
        }

        // 6. Cria novas partições se ainda houver ativos.
        while (remaining.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var newAssets = new List<string>(MaxAssetsPerPartition);

            while (newAssets.Count < MaxAssetsPerPartition &&
                   remaining.Count > 0)
            {
                newAssets.Add(remaining.Dequeue());
            }

            var partition = new AssetPartition
            {
                Id = NewSequentialGuid(),
                Version = 1,
                Assets = newAssets
            };

            await redisService.SavePartitionAsync(partition, leadershipSession);

            result.Add(partition);

            logger.LogInformation(
                "Novo slot {PartitionId} criado com {Count} ativos.",
                partition.Id,
                partition.Assets.Count);
        }

        return result;
    }

    // -------------------------------------------------------
    // Distribuição de slots
    // -------------------------------------------------------

    private async Task ReconcileAssignmentsAsync(
        LeadershipSession session,
        IReadOnlyList<AssetPartition> partitions,
        IReadOnlyList<string> workers,
        CancellationToken cancellationToken)
    {
        var existing = await redisService.GetAssignmentsAsync();

        var partitionIds = partitions
            .Select(x => x.Id)
            .ToHashSet();

        var assignmentsByPartition = existing
            .ToDictionary(x => x.PartitionId);

        // Remove atribuições de slots que não existem mais.
        foreach (var assignment in existing)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!partitionIds.Contains(assignment.PartitionId))
            {
                await redisService.RemoveAssignmentAsync(
                    assignment.PartitionId, session);
            }
        }

        if (workers.Count == 0)
        {
            logger.LogWarning(
                "Nenhum worker elegível para receber slots.");

            return;
        }

        var orderedWorkers = workers
            .Distinct()
            .OrderBy(x => x)
            .ToList();

        var readyWorkers = orderedWorkers.ToHashSet();

        // Calcula a distribuição-alvo.
        var baseCount = partitions.Count / orderedWorkers.Count;
        var remainder = partitions.Count % orderedWorkers.Count;

        var targetLoads = new Dictionary<string, int>();

        for (var i = 0; i < orderedWorkers.Count; i++)
        {
            targetLoads[orderedWorkers[i]] =
                baseCount + (i < remainder ? 1 : 0);
        }

        // Conta somente atribuições pertencentes a workers elegíveis.
        var currentLoads = orderedWorkers.ToDictionary(
            x => x,
            _ => 0);

        foreach (var partition in partitions)
        {
            if (assignmentsByPartition.TryGetValue(
                    partition.Id,
                    out var assignment) &&
                readyWorkers.Contains(assignment.WorkerId))
            {
                currentLoads[assignment.WorkerId]++;
            }
        }

        var finalAssignments = new Dictionary<string, PartitionAssignment>();

        // Preserva as atribuições atuais inicialmente.
        foreach (var partition in partitions)
        {
            if (assignmentsByPartition.TryGetValue(
                    partition.Id,
                    out var assignment))
            {
                finalAssignments[partition.Id] = assignment;
            }
        }

        // ---------------------------------------------------
        // 1. Resolve slots sem atribuição ou com worker
        //    que não está mais elegível.
        // ---------------------------------------------------

        foreach (var partition in partitions)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (assignmentsByPartition.TryGetValue(
                    partition.Id,
                    out var current) &&
                readyWorkers.Contains(current.WorkerId))
            {
                continue;
            }

            var destination = GetWorkerWithDeficit(
                currentLoads,
                targetLoads);

            if (destination is null)
                break;

            var generation =
                assignmentsByPartition.TryGetValue(
                    partition.Id,
                    out var previous)
                    ? previous.Generation + 1
                    : 1;

            finalAssignments[partition.Id] = new PartitionAssignment
            {
                PartitionId = partition.Id,
                WorkerId = destination,
                PartitionVersion = partition.Version,
                Generation = generation
            };

            currentLoads[destination]++;
        }

        // ---------------------------------------------------
        // 2. Rebalanceamento gradual dos workers excedentes.
        // ---------------------------------------------------

        var moved = 0;

        while (moved < MaxPartitionsToMovePerCycle)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var source = currentLoads
                .Where(x => x.Value > targetLoads[x.Key])
                .OrderByDescending(x =>
                    x.Value - targetLoads[x.Key])
                .ThenBy(x => x.Key)
                .FirstOrDefault();

            if (source.Key is null)
                break;

            var destination = GetWorkerWithDeficit(
                currentLoads,
                targetLoads);

            if (destination is null)
                break;

            var partitionToMove = partitions
                .Where(p =>
                    finalAssignments.TryGetValue(
                        p.Id,
                        out var assignment) &&
                    assignment.WorkerId == source.Key)
                .OrderBy(p => p.Id)
                .FirstOrDefault();

            if (partitionToMove is null)
                break;

            var previous = finalAssignments[
                partitionToMove.Id];

            finalAssignments[partitionToMove.Id] =
                previous with
                {
                    WorkerId = destination,
                    Generation = previous.Generation + 1
                };

            currentLoads[source.Key]--;
            currentLoads[destination]++;

            moved++;
        }

        // ---------------------------------------------------
        // 3. Persiste somente o que mudou.
        // ---------------------------------------------------

        foreach (var partition in partitions)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!finalAssignments.TryGetValue(
                    partition.Id,
                    out var assignment))
            {
                continue;
            }

            var changed =
                !assignmentsByPartition.TryGetValue(
                    partition.Id,
                    out var previous) ||
                previous.WorkerId != assignment.WorkerId ||
                previous.PartitionVersion != partition.Version ||
                previous.Generation != assignment.Generation;

            if (!changed)
                continue;

            assignment = assignment with
            {
                PartitionVersion = partition.Version
            };

            await redisService.SaveAssignmentAsync(assignment, session);

            await redisService.PublishAssignmentChangedAsync(
                assignment.PartitionId, session);

            finalAssignments[partition.Id] = assignment;

            logger.LogInformation(
                "Slot {PartitionId}: worker {WorkerId}, " +
                "versão {Version}, geração {Generation}.",
                assignment.PartitionId,
                assignment.WorkerId,
                assignment.PartitionVersion,
                assignment.Generation);
        }

        // ---------------------------------------------------
        // 4. Alertas de capacidade saudável.
        // ---------------------------------------------------

        foreach (var worker in currentLoads)
        {
            if (worker.Value > HealthyPartitionsPerWorker)
            {
                logger.LogWarning(
                    "Worker {WorkerId} possui {Count} slots. " +
                    "Limite saudável: {Limit}.",
                    worker.Key,
                    worker.Value,
                    HealthyPartitionsPerWorker);
            }
        }

        if (moved > 0)
        {
            logger.LogInformation(
                "Rebalanceamento executado: {Moved} slots transferidos.",
                moved);
        }
    }

    private static string? GetWorkerWithDeficit(
        IReadOnlyDictionary<string, int> currentLoads,
        IReadOnlyDictionary<string, int> targetLoads)
    {
        return currentLoads
            .Where(x => x.Value < targetLoads[x.Key])
            .OrderBy(x => x.Value - targetLoads[x.Key])
            .ThenBy(x => x.Key)
            .Select(x => x.Key)
            .FirstOrDefault();
    }
}