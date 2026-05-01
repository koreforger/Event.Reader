namespace Event.Data;

/// <summary>Connection options for the EventReader data store.</summary>
public sealed class EventDataOptions
{
    public const string SectionName = "ConnectionStrings:EventData";

    public string ConnectionString { get; set; } = string.Empty;
}
