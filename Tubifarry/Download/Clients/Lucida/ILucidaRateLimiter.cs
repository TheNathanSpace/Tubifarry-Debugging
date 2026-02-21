namespace Tubifarry.Download.Clients.Lucida;

public interface ILucidaRateLimiter
{
    #region Constants
    public const string WORKER_MAUS = "maus";
    public const string WORKER_HUND = "hund";
    public const string WORKER_KATZE = "katze";

    public const int COOLDOWN_SECONDS = 60;
    public const int MIN_REQUEST_DELAY_MS = 4000;
    public const int BUCKET_SIZE_ESTIMATE = 15;

    public const int MAX_CONCURRENT_PER_WORKER = 2;
    public const int MAX_WORKER_RETRIES = 3;
    public const int MAX_WAIT_RETRIES = 2;
    public const int WAIT_ON_ALL_BLOCKED_MS = 60_000;

    public const int POLLING_MIN_DELAY_MS = 500;
    public const int POLLING_MAX_CONCURRENT = 5;
    #endregion

    IReadOnlyCollection<string> Workers { get; }

    Task<string> WaitForAvailableWorkerAsync(CancellationToken cancellationToken);

    void MarkWorkerRateLimited(string workerName);

    void ReleaseWorker(string workerName);

    void EnsureWorkerRegistered(string workerName);

    bool IsRateLimitedResponse(string responseContent);

    IReadOnlyDictionary<string, LucidaWorkerState> GetWorkerStates();

    Task AcquirePollingSlotAsync(string workerName, CancellationToken cancellationToken);

    void ReleasePollingSlot(string workerName);
}
