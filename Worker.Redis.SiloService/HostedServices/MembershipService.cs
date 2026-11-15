using Worker.Redis.SiloService.Services;

namespace Worker.Redis.SiloService.HostedServices;

internal sealed class MembershipService(
    WorkerState state,
    IRedisService redisService,
    ILogger<MembershipService> logger) : BackgroundService
{
    private static readonly TimeSpan HeartbeatInterval =
        TimeSpan.FromSeconds(5);

    private static readonly TimeSpan ReadyDelay =
        TimeSpan.FromMinutes(1);

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var workerId = state.WorkerId;

        // Registra imediatamente.
        await redisService.RegisterWorkerAsync(workerId);

        var readyAt = DateTimeOffset.UtcNow + ReadyDelay;
        var isReady = false;

        logger.LogInformation(
            "Worker {WorkerId} registrado. Aguardando aquecimento.",
            workerId);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(
                HeartbeatInterval,
                stoppingToken);

            // Continua renovando o heartbeat durante o aquecimento.
            await redisService.RegisterWorkerAsync(workerId);

            if (!isReady && DateTimeOffset.UtcNow >= readyAt)
            {
                await redisService.SetWorkerReadyAsync(workerId);

                isReady = true;

                logger.LogInformation(
                    "Worker {WorkerId} está pronto para receber slots.",
                    workerId);
            }
        }
    }
}
