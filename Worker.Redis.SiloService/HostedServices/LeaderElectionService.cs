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

    private static readonly Mutex DebugLeaderMutex =
        new(false, "AppHost2.SiloService.DebugLeader");

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var ownsDebugMutex = false;

            try
            {
                if (Debugger.IsAttached)
                {
                    ownsDebugMutex = TryAcquireDebugMutex();

                    if (!ownsDebugMutex)
                    {
                        await Task.Delay(
                            RetryInterval,
                            stoppingToken);

                        continue;
                    }
                }

                var token = Guid.NewGuid().ToString("N");

                var acquired = await redisService.LeaderTryAcquireAsync(token);

                if (!acquired)
                {
                    await Task.Delay(
                        RetryInterval,
                        stoppingToken);

                    continue;
                }

                var session = state.BecomeLeader(token);

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
                if (ownsDebugMutex)
                    DebugLeaderMutex.ReleaseMutex();
            }
        }
    }

    private static bool TryAcquireDebugMutex()
    {
        try
        {
            return DebugLeaderMutex.WaitOne(0);
        }
        catch (AbandonedMutexException)
        {
            return true;
        }
    }

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

                await redisService.LeaderTryAcquireAsync(token);
            }
        }
    }
}
