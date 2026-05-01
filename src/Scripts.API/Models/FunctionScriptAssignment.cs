namespace EventReader.Scripts.API.Models;

/// <summary>
/// Represents a script assignment to a function, from the FunctionScripts junction table.
/// </summary>
public sealed class FunctionScriptAssignment
{
    public long FunctionId { get; init; }
    public long ScriptId { get; init; }
    public string Role { get; init; } = "extract";
    public int Ordinal { get; init; }
    public bool IsEnabled { get; init; } = true;
    public DateTime CreatedDate { get; init; }
}
