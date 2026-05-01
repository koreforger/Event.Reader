using KoreForge.Jex;

namespace EventReader.Scripts.API.Models;

/// <summary>
/// An active shadow test session comparing a candidate script against the current live script.
/// </summary>
public sealed class ShadowTestSession
{
    public required string SessionId { get; init; }
    public required long FunctionId { get; init; }
    public required string FunctionName { get; init; }
    public required IJexProgram? CurrentProgram { get; init; }
    public required IJexProgram CandidateProgram { get; init; }
    public required string CandidateContent { get; init; }
    public long? CandidateScriptId { get; init; }
    public int SampleSize { get; init; }
    public int Remaining { get; set; }
    public DateTime TimeoutAt { get; init; }
    public DateTime StartedAt { get; init; }
}
