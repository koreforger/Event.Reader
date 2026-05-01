namespace Event.Data.Entities;

/// <summary>
/// EventReader function definition — the global (non-source-specific) plan.
/// Source-system overrides are in <see cref="ErFunctionSourceOverride"/>.
/// </summary>
public sealed class ErFunction
{
    public int FunctionId { get; set; }
    public long FunctionVersion { get; set; }
    public string Name { get; set; } = string.Empty;
    public bool IsEnabled { get; set; } = true;

    // Extraction script reference (name resolves to dbo.Scripts)
    public string ScriptName { get; set; } = string.Empty;
    public long ScriptVersion { get; set; }

    // Rule set
    public long RuleSetVersion { get; set; }

    // Output route
    public long OutputRouteVersion { get; set; }
    public string OutputRouteName { get; set; } = "default";
    public string OutputTopic { get; set; } = string.Empty;

    // Failure policy
    public bool FailWhenNedbankIdMissing { get; set; }
    public int MaxProcessingAttempts { get; set; } = 3;
    public int MaxOutputAttempts { get; set; } = 3;

    public ICollection<ErFunctionRule> Rules { get; set; } = [];
    public ICollection<ErFunctionMatcher> Matchers { get; set; } = [];
    public ICollection<ErFunctionSourceOverride> SourceOverrides { get; set; } = [];
}
