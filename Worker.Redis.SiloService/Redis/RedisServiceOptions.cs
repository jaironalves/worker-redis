using System;
using System.Collections.Generic;
using System.Text;

namespace Worker.Redis.SiloService.Redis;

public sealed class RedisServiceOptions
{
    public TimeSpan LeaderLeaseDuration { get; set; }
        = TimeSpan.FromSeconds(15);

    public TimeSpan LeaderRenewInterval { get; set; }
        = TimeSpan.FromSeconds(5);

    public TimeSpan WorkerHeartbeatTtl { get; set; }
        = TimeSpan.FromSeconds(15);
}
