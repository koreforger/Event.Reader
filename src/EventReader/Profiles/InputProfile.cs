namespace EventReader.Profiles;

/// <summary>
/// Describes how to decode, classify, and route a class of Kafka messages.
/// Profiles are loaded from SQL at startup and refreshed live.
/// </summary>
public sealed class InputProfile
{
    /// <summary>Unique name identifying this profile (used in SQL settings and log tokens).</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>Human-readable description.</summary>
    public string? Description { get; init; }

    /// <summary>Whether this profile is currently active.</summary>
    public bool IsActive { get; init; } = true;

    /// <summary>
    /// Content-type expected in the envelope payload (e.g. "application/json").
    /// Determines decoder selection.
    /// </summary>
    public string PayloadContentType { get; init; } = "application/json";

    /// <summary>
    /// Optional JEX classification expression evaluated against the raw payload.
    /// Returns a classification label string.
    /// </summary>
    public string? ClassificationExpression { get; init; }

    /// <summary>
    /// JEX parse/transform expression applied after classification.
    /// Transforms the raw payload into the canonical OperationalEnvelope Payload.
    /// </summary>
    public string? ParseExpression { get; init; }

    /// <summary>Names of output routes this profile writes to.</summary>
    public IReadOnlyList<string> OutputRouteNames { get; init; } = Array.Empty<string>();

    /// <summary>Optional routing tag overrides applied when this profile matches.</summary>
    public IReadOnlyDictionary<string, string> RoutingTagOverrides { get; init; }
        = new Dictionary<string, string>();

    /// <summary>
    /// Whether to apply schema validation after parsing.
    /// Validation failures add a ProcessingWarning but do not block routing.
    /// </summary>
    public bool ValidateSchema { get; init; }
}

/// <summary>
/// Defines a named output route used by a profile.
/// </summary>
public sealed class OutputRoute
{
    public string RouteName { get; init; } = string.Empty;
    public string TargetTopic { get; init; } = string.Empty;
    public bool IsRequired { get; init; } = true;
}
