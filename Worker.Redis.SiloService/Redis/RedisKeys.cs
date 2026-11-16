using System;
using System.Collections.Generic;
using System.Text;

namespace Worker.Redis.SiloService.Redis;

public static class RedisKeys
{
    private const string Prefix = "md:";

    // Todas as chaves usadas juntas nos scripts Lua
    // precisam ter o mesmo Hash Slot.
    private const string CoordinationPrefix = "md:{marketdata}:";

    public const string Leader =
        CoordinationPrefix + "leader";

    public const string LeaderGeneration =
        CoordinationPrefix + "leader:generation";

    public const string Catalog =
        CoordinationPrefix + "catalog:partitions";

    public const string Assignments =
        CoordinationPrefix + "assignments";

    // Estruturas dos workers
    public const string AliveWorkers =
        Prefix + "workers:alive";

    public const string ReadyWorkers =
        Prefix + "workers:ready";

    public const string WorkerPrefix =
        Prefix + "worker:";

    // Ajuste para o nome do seu Redis Set atual.
    public const string Assets =
        Prefix + "assets";

    // Pub/Sub
    public const string AssignmentChannel =
        Prefix + "assignment:changed";

    public static string Worker(string workerId)
        => WorkerPrefix + workerId;
}
