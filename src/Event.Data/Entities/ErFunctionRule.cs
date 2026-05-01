namespace Event.Data.Entities;

/// <summary>A rule within a function's global rule set.</summary>
public sealed class ErFunctionRule
{
    public int FunctionId { get; set; }
    public int RuleId { get; set; }
    public long RuleVersion { get; set; }
    public string RuleName { get; set; } = string.Empty;

    public ErFunction Function { get; set; } = null!;
}
