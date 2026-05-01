namespace Event.Data.Entities;

/// <summary>Global configuration key-value pairs for the EventReader runtime model.</summary>
public sealed class ErConfig
{
    public string ConfigKey { get; set; } = string.Empty;
    public string ConfigValue { get; set; } = string.Empty;
}
