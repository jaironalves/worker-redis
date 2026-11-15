using System;
using System.Collections.Generic;
using System.Text;
using Worker.Redis.SiloService;

namespace Worker.Redis.SiloService.Services;

internal interface ISseOrchestrator
{
    Task ReconcileAsync(
        IReadOnlyCollection<AssetPartition> desiredSlots,
        IReadOnlyCollection<PartitionAssignment> assignments,
        CancellationToken cancellationToken);
}
