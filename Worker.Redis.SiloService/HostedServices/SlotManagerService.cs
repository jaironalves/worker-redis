using StackExchange.Redis;
using Worker.Redis.SiloService.Services;

namespace Worker.Redis.SiloService.HostedServices;

internal class SlotManagerService(
    IRedisService redis,
    WorkerState workerState,
    ISseOrchestrator orchestrator,
    ILogger<SlotManagerService> logger)
    : BackgroundService
{
    private static readonly TimeSpan ReconcileInterval =
        TimeSpan.FromSeconds(5);

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        logger.LogInformation(
            "WorkerSlotManager iniciado. WorkerId: {WorkerId}",
            workerState.WorkerId);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ReconcileAsync(stoppingToken);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Erro ao reconciliar os slots do worker.");
            }

            await Task.Delay(ReconcileInterval, stoppingToken);
        }
    }

    private async Task ReconcileAsync(
        CancellationToken cancellationToken)
    {
        var workerId = workerState.WorkerId;

        var assignments =
            await redis.GetWorkerAssignmentsAsync(workerId);

        var partitions =
            await redis.GetPartitionsAsync();

        cancellationToken.ThrowIfCancellationRequested();

        var partitionsById = partitions.ToDictionary(x => x.Id);

        var desiredSlots = assignments
            .Where(x => partitionsById.ContainsKey(x.PartitionId))
            .Select(x => partitionsById[x.PartitionId])
            .ToList();

        await orchestrator.ReconcileAsync(
            desiredSlots,
            assignments,
            cancellationToken);
    }
}