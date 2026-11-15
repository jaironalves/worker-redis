using System;
using System.Collections.Generic;
using System.Text;

namespace Worker.Redis.SiloService.Services;

public interface ILeaderElection
{
    bool IsLeader { get; }

    CancellationToken LeaderToken { get; }
}
