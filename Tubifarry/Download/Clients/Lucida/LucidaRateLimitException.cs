namespace Tubifarry.Download.Clients.Lucida
{
    public class LucidaRateLimitException : Exception
    {
        public string? WorkerName { get; }
        public LucidaRateLimitException(string message, string? workerName = null)
            : base(message) => WorkerName = workerName;

        public LucidaRateLimitException(string message, Exception innerException, string? workerName = null)
            : base(message, innerException) => WorkerName = workerName;
    }
}
