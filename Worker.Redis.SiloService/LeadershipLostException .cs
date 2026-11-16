namespace Worker.Redis.SiloService;

public sealed class LeadershipLostException(string message) : Exception(message)
{
}
