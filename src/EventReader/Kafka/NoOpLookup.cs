using Event.Streaming.Processing.Runtime;

namespace EventReader.Kafka;

internal sealed class NoOpLookup : IUsernameIdentityLookup
{
    public Task<long?> ResolveNedbankIdAsync(string username, CancellationToken ct) =>
        Task.FromResult<long?>(null);
}
