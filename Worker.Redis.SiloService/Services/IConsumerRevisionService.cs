using System;
using System.Collections.Generic;
using System.Text;

namespace Worker.Redis.SiloService.Services;

public interface IConsumerRevisionService
{
    Task<int> GetRevisionAsync(
        CancellationToken cancellationToken = default);
}
