namespace Worker.Redis.SiloService;

public sealed record AssetPartition
{
    public required string Id { get; init; }
    public required long Version { get; init; }
    public required List<string> Assets { get; init; }
}

public sealed record PartitionAssignment
{
    public required string PartitionId { get; init; }
    public required string WorkerId { get; init; }

    // Versão da composição do slot.
    public required long PartitionVersion { get; init; }

    // Incrementa quando o slot muda de worker.
    public required long Generation { get; init; }
}

public sealed record ActiveSlot(
        string PartitionId,
        long PartitionVersion,
        long Generation);

public sealed record LeadershipSession(
    string Token,
    CancellationToken TokenCancellation);

public sealed record PartitionComposition(
        AssetPartition Original,
        List<string> Assets);

public sealed record WorkerInfo(
    string WorkerId,
    int Capacity);

public sealed record WorkerAssignment(
    string WorkerId,
    IReadOnlyList<string> PartitionIds);

public sealed record ClusterAssignment(
    long Version,
    IReadOnlyDictionary<string, WorkerAssignment> Workers);
