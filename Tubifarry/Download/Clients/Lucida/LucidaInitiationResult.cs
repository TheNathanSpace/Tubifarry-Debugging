namespace Tubifarry.Download.Clients.Lucida;

public sealed record LucidaInitiationResult
{
    public required string HandoffId { get; init; }
    public required string ServerName { get; init; }
}
