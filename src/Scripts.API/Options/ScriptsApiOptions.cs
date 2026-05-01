namespace EventReader.Scripts.API.Options;

/// <summary>
/// Configuration options for the KafkaProcessor Scripts API.
/// </summary>
public sealed class ScriptsApiOptions
{
    public string ConnectionString { get; set; } = string.Empty;
    public int MaxShadowTestSessions { get; set; } = 5;
    public TimeSpan ShadowTestTimeout { get; set; } = TimeSpan.FromMinutes(5);
}
