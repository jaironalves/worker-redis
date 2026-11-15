using Worker.Redis.SiloService;

namespace Worker.Redis.SiloService.Services;

public interface IRedisService
{
    // Leader
    TimeSpan LeaderRenewInterval { get; }

    Task<bool> LeaderTryAcquireAsync(string token);

    Task<bool> LeaderRenewAsync(string token);

    Task<bool> LeaderReleaseAsync(string token);

    // Workers
    Task RegisterWorkerAsync(string workerId);

    Task SetWorkerReadyAsync(string workerId);

    Task<IReadOnlyList<string>> GetAliveWorkersAsync();

    Task<IReadOnlyList<string>> GetReadyWorkersAsync();

    Task<IReadOnlyList<PartitionAssignment>> GetWorkerAssignmentsAsync(string workerId);

    // Catalog
    Task<IReadOnlyList<AssetPartition>> GetPartitionsAsync();

    Task SavePartitionAsync(AssetPartition partition);

    Task RemovePartitionAsync(string partitionId);

    // Assignments
    Task<IReadOnlyList<PartitionAssignment>> GetAssignmentsAsync();

    Task SaveAssignmentAsync(PartitionAssignment assignment);

    Task RemoveAssignmentAsync(string partitionId);    

    // Notifications
    Task PublishAssignmentChangedAsync(string partitionId);
}