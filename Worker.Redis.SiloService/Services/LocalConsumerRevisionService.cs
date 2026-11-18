using System;
using System.Collections.Generic;
using System.Text;

namespace Worker.Redis.SiloService.Services;

public sealed class LocalConsumerRevisionService
    : IConsumerRevisionService
{
    public Task<int> GetRevisionAsync(
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(1);
    }
}
