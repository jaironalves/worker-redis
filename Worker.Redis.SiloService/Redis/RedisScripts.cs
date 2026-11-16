using System;
using System.Collections.Generic;
using System.Text;

namespace Worker.Redis.SiloService.Redis;

internal static class RedisScripts
{
    public const string AcquireLeaderScript = """
    if redis.call('EXISTS', KEYS[1]) == 1 then
        return 0
    end

    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])

    return redis.call('INCR', KEYS[2])
    """;

    public const string RenewLeaderScript = """
    if redis.call('GET', KEYS[1]) ~= ARGV[1] then
        return 0
    end

    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
    """;

    public const string ReleaseLeaderScript = """
    if redis.call('GET', KEYS[1]) ~= ARGV[1] then
        return 0
    end

    return redis.call('DEL', KEYS[1])
    """;

    public const string SavePartitionScript = """
    if redis.call('GET', KEYS[1]) ~= ARGV[1] then
        return -1
    end

    if redis.call('GET', KEYS[2]) ~= ARGV[2] then
        return -1
    end

    redis.call('HSET', KEYS[3], ARGV[3], ARGV[4])

    return 1
    """;

    public const string RemovePartitionScript = """
    if redis.call('GET', KEYS[1]) ~= ARGV[1] then
        return -1
    end

    if redis.call('GET', KEYS[2]) ~= ARGV[2] then
        return -1
    end

    redis.call('HDEL', KEYS[3], ARGV[3])

    return 1
    """;

    public const string SaveAssignmentScript = """
    if redis.call('GET', KEYS[1]) ~= ARGV[1] then
        return -1
    end

    if redis.call('GET', KEYS[2]) ~= ARGV[2] then
        return -1
    end

    redis.call('HSET', KEYS[3], ARGV[3], ARGV[4])

    return 1
    """;

    public const string RemoveAssignmentScript = """
    if redis.call('GET', KEYS[1]) ~= ARGV[1] then
        return -1
    end

    if redis.call('GET', KEYS[2]) ~= ARGV[2] then
        return -1
    end

    redis.call('HDEL', KEYS[3], ARGV[3])

    return 1
    """;

    public const string PublishAssignmentScript = """
    if redis.call('GET', KEYS[1]) ~= ARGV[1] then
        return -1
    end

    if redis.call('GET', KEYS[2]) ~= ARGV[2] then
        return -1
    end

    return redis.call('PUBLISH', ARGV[3], ARGV[4])
    """;
}
