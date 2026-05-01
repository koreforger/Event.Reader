using Microsoft.Extensions.Logging;

namespace EventReader.Kafka;

public sealed class LoggingOutputMessagePublisher : IOutputMessagePublisher
{
    private readonly ILogger<LoggingOutputMessagePublisher> _log;

    public LoggingOutputMessagePublisher(ILogger<LoggingOutputMessagePublisher> log)
    {
        _log = log;
    }

    public Task<OutputPublishResult> PublishAsync(byte[] payload, CancellationToken ct)
    {
        _log.LogInformation("Output publish: {Length} bytes", payload.Length);
        return Task.FromResult(new OutputPublishResult(true, null, 0, 0, null));
    }
}
