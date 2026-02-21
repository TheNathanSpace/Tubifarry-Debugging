namespace Tubifarry.Download.Clients.Lucida;

public sealed record LucidaWorkerState
{
    public string Name { get; init; } = string.Empty;
    public bool IsAvailable { get; init; }
    public DateTime? RateLimitedUntil { get; init; }
    public int ActiveRequests { get; init; }
    public int ActivePolling { get; init; }
    public DateTime? LastSuccessfulRequest { get; init; }
}
