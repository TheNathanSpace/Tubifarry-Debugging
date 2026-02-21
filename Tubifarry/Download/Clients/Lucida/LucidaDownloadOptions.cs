using Tubifarry.Download.Base;

namespace Tubifarry.Download.Clients.Lucida
{
    public record LucidaDownloadOptions : BaseDownloadOptions
    {
        public ILucidaRateLimiter RateLimiter { get; init; } = null!;

        public LucidaDownloadOptions() { }

        public LucidaDownloadOptions(LucidaDownloadOptions options) : base(options)
        {
            RateLimiter = options.RateLimiter;
        }
    }
}
