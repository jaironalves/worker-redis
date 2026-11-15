using Worker.Redis.SiloService;

namespace Worker.Redis.SiloService.Services;

public sealed class SseOrchestrator(ILogger<SseOrchestrator> logger) : ISseOrchestrator
{
    private readonly Dictionary<string, ActiveSlot> _activeSlots = [];

    public async Task ReconcileAsync(
        IReadOnlyCollection<AssetPartition> desiredSlots,
        IReadOnlyCollection<PartitionAssignment> assignments,
        CancellationToken cancellationToken)
    {
        var desiredById = desiredSlots.ToDictionary(x => x.Id);
        var assignmentsById = assignments.ToDictionary(x => x.PartitionId);

        // 1. Encerrar slots que não pertencem mais ao worker.
        foreach (var activeSlot in _activeSlots.Values.ToList())
        {
            if (!desiredById.ContainsKey(activeSlot.PartitionId))
            {
                await StopSlotAsync(
                    activeSlot.PartitionId,
                    cancellationToken);
            }
        }

        // 2. Iniciar novos slots ou atualizar os que mudaram.
        foreach (var partition in desiredSlots)
        {
            var assignment = assignmentsById[partition.Id];

            if (!_activeSlots.TryGetValue(
                    partition.Id, out var activeSlot))
            {
                await StartSlotAsync(
                    partition,
                    assignment,
                    cancellationToken);

                continue;
            }

            var compositionChanged =
                activeSlot.PartitionVersion != partition.Version;

            var assignmentChanged =
                activeSlot.Generation != assignment.Generation;

            if (compositionChanged || assignmentChanged)
            {
                await StopSlotAsync(
                    partition.Id,
                    cancellationToken);

                await StartSlotAsync(
                    partition,
                    assignment,
                    cancellationToken);
            }
        }
    }

    private async Task StartSlotAsync(
        AssetPartition partition,
        PartitionAssignment assignment,
        CancellationToken cancellationToken)
    {
        // Aqui entra sua implementação atual de abertura SSE.

        _activeSlots[partition.Id] = new ActiveSlot(
            partition.Id,
            partition.Version,
            assignment.Generation);

        logger.LogInformation("Slot {PartitionId} iniciado com versão {Version}, geração {Generation} e ativos {Assets}.",
            partition.Id,
            partition.Version,
            assignment.Generation,
            string.Join(", ", partition.Assets));
    }

    private async Task StopSlotAsync(
        string partitionId,
        CancellationToken cancellationToken)
    {
        // Aqui entra sua implementação atual de encerramento SSE.

        _activeSlots.Remove(partitionId);

        logger.LogInformation("Slot {PartitionId} encerrado.", partitionId);
    }    
}
