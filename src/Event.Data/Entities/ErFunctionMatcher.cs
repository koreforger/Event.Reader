namespace Event.Data.Entities;

/// <summary>
/// Prefix + regex matcher that maps a discriminator value to a function.
/// <see cref="SourceSystemId"/> = null means the matcher applies globally to all source systems.
/// </summary>
public sealed class ErFunctionMatcher
{
    public int FunctionMatcherId { get; set; }
    public string? SourceSystemId { get; set; }
    public int FunctionId { get; set; }
    public int Priority { get; set; } = 100;
    public string Prefix { get; set; } = string.Empty;
    public string RegexPattern { get; set; } = string.Empty;

    public ErFunction Function { get; set; } = null!;
}
