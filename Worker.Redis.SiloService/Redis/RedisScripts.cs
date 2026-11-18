using System;
using System.Collections.Generic;
using System.Text;

namespace Worker.Redis.SiloService.Redis;

internal static class RedisScripts
{
    public const string AcquireLeaderScript = """
        local current = redis.call('GET', KEYS[1])

        if current then
            local leader = cjson.decode(current)

            local currentRevision = tonumber(leader.revision)
            local candidateRevision = tonumber(ARGV[2])

            if candidateRevision <= currentRevision then
                return 0
            end
        end

        local newLeader = cjson.encode({
            revision = tonumber(ARGV[2]),
            token = ARGV[1]
        })

        redis.call('SET', KEYS[1], newLeader, 'PX', ARGV[3])

        return redis.call('INCR', KEYS[2])
        """;

    private const string ValidateLeadership = """
        local current = redis.call('GET', KEYS[1])

        if not current then
            return -1
        end

        local leader = cjson.decode(current)

        if leader.token ~= ARGV[1] then
            return -1
        end

        if redis.call('GET', KEYS[2]) ~= ARGV[2] then
            return -1
        end
        """;

    public const string RenewLeaderScript = """
        local current = redis.call('GET', KEYS[1])

        if not current then
            return 0
        end

        local leader = cjson.decode(current)

        if leader.token ~= ARGV[1] then
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

    public const string SavePartitionScript = $$"""
        {{ValidateLeadership}}

        redis.call('HSET', KEYS[3], ARGV[3], ARGV[4])

        return 1
        """;

    public const string RemovePartitionScript = $$"""
        {{ValidateLeadership}}

        redis.call('HDEL', KEYS[3], ARGV[3])

        return 1
        """;

    public const string SaveAssignmentScript = $$"""
        {{ValidateLeadership}}

        redis.call('HSET', KEYS[3], ARGV[3], ARGV[4])

        return 1
        """;

    public const string RemoveAssignmentScript = $$"""
        {{ValidateLeadership}}

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
