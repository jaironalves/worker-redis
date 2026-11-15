using Worker.Redis.SiloService.Services;

namespace Worker.Redis.SiloService.HostedServices;

internal sealed class LeaderCoordinatorService(
    WorkerState state,
    CatalogCoordinator coordinator,
    ILogger<LeaderCoordinatorService> logger)
    : BackgroundService
{
    private static readonly TimeSpan InitialGracePeriod =
        TimeSpan.FromMinutes(2);

    private static readonly TimeSpan ReconcileInterval =
        TimeSpan.FromSeconds(5);

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var leadership = await state
                .WaitForLeadershipAsync(stoppingToken);

            try
            {
                logger.LogInformation(
                    "Leader aguardando período de carência de {Minutes} minutos.",
                    InitialGracePeriod.TotalMinutes);

                // Continua sendo Leader, mas ainda não distribui slots.
                await Task.Delay(
                    InitialGracePeriod,
                    leadership.TokenCancellation);

                logger.LogInformation(
                    "Período de carência encerrado. Iniciando reconciliação.");

                while (!leadership.TokenCancellation.IsCancellationRequested)
                {
                    await coordinator.ReconcileAsync(
                        leadership.TokenCancellation);

                    await Task.Delay(
                        ReconcileInterval,
                        leadership.TokenCancellation);
                }
            }
            catch (OperationCanceledException)
                when (leadership.TokenCancellation.IsCancellationRequested)
            {
                logger.LogWarning(
                    "Reconciliação interrompida porque o worker perdeu a liderança.");
            }
        }
    }
}