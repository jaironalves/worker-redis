using Worker.Redis.SiloService;

namespace Worker.Redis.SiloService.Services;

public interface IRedisService
{
    TimeSpan LeaderRenewInterval { get; }

    // Leader
    Task<long?> LeaderTryAcquireAsync(string token, int revision);

    Task<bool> LeaderRenewAsync(string token);

    Task<bool> LeaderReleaseAsync(string token);

    // Workers
    Task RegisterWorkerAsync(string workerId);

    Task SetWorkerReadyAsync(string workerId);

    Task<IReadOnlyList<string>> GetAliveWorkersAsync();

    Task<IReadOnlyList<string>> GetReadyWorkersAsync();

    // Assets
    Task<IReadOnlyList<string>> GetAssetsAsync();

    // Partitions
    Task<IReadOnlyList<AssetPartition>> GetPartitionsAsync();

    Task SavePartitionAsync(
        AssetPartition partition,
        LeadershipSession session);

    Task RemovePartitionAsync(
        string partitionId,
        LeadershipSession session);

    // Assignments
    Task<IReadOnlyList<PartitionAssignment>> GetAssignmentsAsync();

    Task SaveAssignmentAsync(
        PartitionAssignment assignment,
        LeadershipSession session);

    Task RemoveAssignmentAsync(
        string partitionId,
        LeadershipSession session);

    Task<IReadOnlyList<PartitionAssignment>> GetWorkerAssignmentsAsync(
        string workerId);

    // Notifications
    Task PublishAssignmentChangedAsync(
        string partitionId,
        LeadershipSession session);
}