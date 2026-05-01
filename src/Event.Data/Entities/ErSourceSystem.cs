namespace Event.Data.Entities;

/// <summary>Kafka source system / topic definition.</summary>
public sealed class ErSourceSystem
{
    public string SourceSystemId { get; set; } = string.Empty;
    public string KafkaTopic { get; set; } = string.Empty;
    public string ProviderName { get; set; } = string.Empty;
    public bool IsEnabled { get; set; } = true;

    public ICollection<ErSourceSystemTag> Tags { get; set; } = [];
    public ICollection<ErFunctionSourceOverride> FunctionOverrides { get; set; } = [];
}
