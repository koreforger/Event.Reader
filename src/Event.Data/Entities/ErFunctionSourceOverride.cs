namespace Event.Data.Entities;

/// <summary>
/// Source-system-specific override for a function plan.
/// When present, replaces the global script / ruleset / output route for that (Function, SourceSystem) pair.
/// </summary>
public sealed class ErFunctionSourceOverride
{
    public int FunctionId { get; set; }
    public string SourceSystemId { get; set; } = string.Empty;

    public string ScriptName { get; set; } = string.Empty;
    public long ScriptVersion { get; set; }

    public long RuleSetVersion { get; set; }

    public long OutputRouteVersion { get; set; }
    public string OutputRouteName { get; set; } = "default";
    public string OutputTopic { get; set; } = string.Empty;

    public bool FailWhenNedbankIdMissing { get; set; }
    public int MaxProcessingAttempts { get; set; } = 3;
    public int MaxOutputAttempts { get; set; } = 3;

    public ErFunction Function { get; set; } = null!;
    public ErSourceSystem SourceSystem { get; set; } = null!;
    public ICollection<ErFunctionSourceOverrideRule> Rules { get; set; } = [];
}
