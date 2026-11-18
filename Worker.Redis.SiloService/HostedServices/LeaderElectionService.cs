using System.Diagnostics;
using Worker.Redis.SiloService.Services;

namespace Worker.Redis.SiloService.HostedServices;

internal sealed class LeaderElectionService(
    WorkerState state,
    IRedisService redisService,    
    ILogger<LeaderElectionService> logger)
    : BackgroundService
{
    private static readonly TimeSpan RetryInterval =
        TimeSpan.FromSeconds(2);

    private static readonly Semaphore DebugLeaderSemaphore =
        new(1, 1, "AppHost2.SiloService.DebugLeader");

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var ownsDebugSemaphore = false;

            try
            {
                //if (Debugger.IsAttached)
                //{
                //    ownsDebugSemaphore = TryAcquireDebugSemaphore();

                //    if (!ownsDebugSemaphore)
                //    {
                //        await Task.Delay(
                //            RetryInterval,
                //            stoppingToken);

                //        continue;
                //    }
                //}

                var token = Guid.NewGuid().ToString("N");

                var generation = await redisService.LeaderTryAcquireAsync(token, 1);

                if (generation is null)
                    return;               

                var session = state.BecomeLeader(token, generation.Value);

                logger.LogInformation(
                    "Worker {WorkerId} assumiu a liderança.",
                    state.WorkerId);

                try
                {
                    await MaintainLeadershipAsync(
                        token,
                        stoppingToken);
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    // Encerramento do processo.
                }
                catch (Exception ex)
                {
                    logger.LogError(
                        ex,
                        "Erro ao renovar liderança.");
                }
                finally
                {
                    state.LoseLeadership(session);

                    try
                    {
                        await redisService.LeaderReleaseAsync(token);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(
                            ex,
                            "Não foi possível liberar o lock. O TTL resolverá.");
                    }

                    logger.LogWarning(
                        "Worker {WorkerId} perdeu a liderança.",
                        state.WorkerId);
                }
            }
            finally
            {
                if (ownsDebugSemaphore)
                    DebugLeaderSemaphore.Release();
            }
        }
    }

    private static bool TryAcquireDebugSemaphore() =>
        DebugLeaderSemaphore.WaitOne(0);

    private async Task MaintainLeadershipAsync(
        string token,
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {

            await Task.Delay(
                redisService.LeaderRenewInterval,
                stoppingToken);


            var renewed = await redisService.LeaderRenewAsync(token);

            if (!renewed)
            {
                logger.LogWarning(
                    "A renovação da liderança falhou.");              

                await Task.Delay(
                    RetryInterval,
                    stoppingToken);

                await redisService.LeaderTryAcquireAsync(token, 1);
            }
        }
    }
}
