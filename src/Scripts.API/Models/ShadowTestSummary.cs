namespace EventReader.Scripts.API.Models;

/// <summary>
/// Summary of a completed shadow test session.
/// </summary>
public sealed class ShadowTestSummary
{
    public required string SessionId { get; init; }
    public required long FunctionId { get; init; }
    public required int TotalProcessed { get; init; }
    public required int MatchCount { get; init; }
    public required int DiffCount { get; init; }
    public required double AvgCurrentTimeMs { get; init; }
    public required double AvgCandidateTimeMs { get; init; }
    public required DateTime CompletedAt { get; init; }
    public required string Reason { get; init; } // "sample_size_reached", "timeout", "cancelled"
}
