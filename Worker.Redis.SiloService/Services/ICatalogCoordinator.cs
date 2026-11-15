using System;
using System.Collections.Generic;
using System.Text;

namespace Worker.Redis.SiloService.Services;

public interface ICatalogCoordinator
{
    Task ReconcileAsync(CancellationToken ct);
}
