namespace Event.Data.Entities;

/// <summary>Arbitrary tags attached to a source system.</summary>
public sealed class ErSourceSystemTag
{
    public string SourceSystemId { get; set; } = string.Empty;
    public string TagKey { get; set; } = string.Empty;
    public string TagValue { get; set; } = string.Empty;

    public ErSourceSystem SourceSystem { get; set; } = null!;
}
