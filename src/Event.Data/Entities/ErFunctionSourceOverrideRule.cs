namespace Event.Data.Entities;

/// <summary>A rule within a source-system-specific function override rule set.</summary>
public sealed class ErFunctionSourceOverrideRule
{
    public int FunctionId { get; set; }
    public string SourceSystemId { get; set; } = string.Empty;
    public int RuleId { get; set; }
    public long RuleVersion { get; set; }
    public string RuleName { get; set; } = string.Empty;

    public ErFunctionSourceOverride Override { get; set; } = null!;
}
