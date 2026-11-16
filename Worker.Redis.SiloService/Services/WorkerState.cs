using Worker.Redis.SiloService;

namespace Worker.Redis.SiloService.Services;

public sealed class WorkerState
{
    private readonly object _lock = new();

    private CancellationTokenSource? _leadershipCts;
    private LeadershipSession? _currentSession;

    private TaskCompletionSource<LeadershipSession> _nextLeadership =
        NewLeadershipSource();

    public string WorkerId { get; } =
        Guid.NewGuid().ToString("N");

    public bool IsLeader
    {
        get
        {
            lock (_lock)
                return _currentSession is not null;
        }
    }

    public LeadershipSession BecomeLeader(string token, long generation)
    {
        lock (_lock)
        {
            if (_currentSession is not null)
                throw new InvalidOperationException(
                    "Worker já é Leader.");

            _leadershipCts = new CancellationTokenSource();

            _currentSession = new LeadershipSession(
                token, generation,
                _leadershipCts.Token);

            _nextLeadership.TrySetResult(_currentSession);

            return _currentSession;
        }
    }

    public void LoseLeadership(LeadershipSession session)
    {
        lock (_lock)
        {
            if (_currentSession != session)
                return;

            _leadershipCts?.Cancel();
            _leadershipCts?.Dispose();

            _leadershipCts = null;
            _currentSession = null;

            _nextLeadership = NewLeadershipSource();
        }
    }

    public Task<LeadershipSession> WaitForLeadershipAsync(
        CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            return _nextLeadership.Task.WaitAsync(
                cancellationToken);
        }
    }

    private static TaskCompletionSource<LeadershipSession> NewLeadershipSource()
    {
        return new TaskCompletionSource<LeadershipSession>(
            TaskCreationOptions.RunContinuationsAsynchronously);
    }
}