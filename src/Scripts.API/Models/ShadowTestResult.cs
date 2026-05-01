namespace EventReader.Scripts.API.Models;

/// <summary>
/// Result of a single shadow test comparison for one message.
/// </summary>
public sealed class ShadowTestResult
{
    public required string SessionId { get; init; }
    public required int MessageIndex { get; init; }
    public required string InputSnippet { get; init; }
    public required DateTime Timestamp { get; init; }
    public required Dictionary<string, object?> CurrentOutput { get; init; }
    public required Dictionary<string, object?> CandidateOutput { get; init; }
    public required IReadOnlyList<FieldDiff> Diffs { get; init; }
    public required double CurrentTimeMs { get; init; }
    public required double CandidateTimeMs { get; init; }
}

/// <summary>
/// A single field-level diff between current and candidate output.
/// </summary>
public sealed class FieldDiff
{
    public required string Field { get; init; }
    public object? CurrentValue { get; init; }
    public object? CandidateValue { get; init; }
    public required string Status { get; init; } // "matched", "changed", "added", "removed"
}
