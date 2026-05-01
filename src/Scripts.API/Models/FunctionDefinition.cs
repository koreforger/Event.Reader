namespace EventReader.Scripts.API.Models;

/// <summary>
/// Defines a function that processes a specific type of Kafka message,
/// identified by matching the Action field against a regex pattern.
/// </summary>
public sealed class FunctionDefinition
{
    public long FunctionId { get; init; }
    public string FunctionName { get; init; } = string.Empty;
    public string ActionRegex { get; init; } = string.Empty;
    public int GroupId { get; init; }
    public string GroupName { get; init; } = string.Empty;
    public string Description { get; init; } = string.Empty;
    public string DescriptionFormat { get; init; } = "markdown";
    public bool IsEnabled { get; init; } = true;
    public DateTime CreatedDate { get; init; }
    public DateTime ModifiedDate { get; init; }
}
