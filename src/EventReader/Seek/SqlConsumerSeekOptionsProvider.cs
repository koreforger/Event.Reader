using Event.Streaming.In.Seek;
using Microsoft.Extensions.Configuration;

namespace EventReader.Seek;

/// <summary>
/// Reads EventReader:Seek:* settings from IConfiguration.
/// Settings are kept in the SQL-backed KFSettings store and refreshed via the live-reload
/// polling mechanism — no restart required to change seek behaviour.
/// </summary>
public sealed class SqlConsumerSeekOptionsProvider : IConsumerSeekOptionsProvider
{
    private readonly IConfiguration _config;

    public SqlConsumerSeekOptionsProvider(IConfiguration config) => _config = config;

    public ConsumerSeekOptions GetOptions()
    {
        var modeStr = _config["EventReader:Seek:Mode"] ?? "None";

        if (!Enum.TryParse<SeekMode>(modeStr, ignoreCase: true, out var mode))
            mode = SeekMode.None;

        return new ConsumerSeekOptions
        {
            Mode = mode,
            StartOffsetOrTimestamp = _config["EventReader:Seek:StartOffsetOrTimestamp"],
            StopOffsetOrTimestamp = _config["EventReader:Seek:StopOffsetOrTimestamp"],
        };
    }
}
