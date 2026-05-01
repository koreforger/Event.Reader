using Event.Streaming.Processing.Runtime;
using Microsoft.Extensions.Configuration;

namespace EventReader.Profiles;

/// <summary>
/// Provides active EventReader processing profiles from live-reload configuration.
/// </summary>
public sealed class EventReaderProfileCatalog : IProcessingProfileCatalog
{
    private readonly IConfiguration _configuration;

    public EventReaderProfileCatalog(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public IReadOnlyList<ProcessingProfile> GetActiveProfiles()
    {
        var section = _configuration.GetSection("EventReader:Profiles");
        var fromConfig = section.Get<List<EventReaderProfileSettings>>() ?? [];

        if (fromConfig.Count == 0)
        {
            return
            [
                new ProcessingProfile
                {
                    Name = "default",
                    Description = "Catch-all profile used when no explicit profile is configured",
                    IsActive = true,
                    ClassificationExpression = "regex:.*",
                    ParseExpression = null,
                    OutputRoutes =
                    [
                        new ProcessingOutputRoute
                        {
                            RouteName = "default-out",
                            TargetTopic = _configuration["EventReader:DefaultOutputTopic"]
                                ?? _configuration["EventReader:OutputTopics:FraudCandidate"]
                                ?? "fraud.candidates",
                            IsRequired = true,
                        }
                    ]
                }
            ];
        }

        return fromConfig
            .Where(p => p.IsActive)
            .Select(p => new ProcessingProfile
            {
                Name = p.Name,
                Description = p.Description,
                IsActive = p.IsActive,
                ClassificationExpression = p.ClassificationExpression,
                ParseExpression = p.ParseExpression,
                RoutingTagOverrides = p.RoutingTagOverrides ?? new Dictionary<string, string>(),
                OutputRoutes = (p.OutputRoutes ?? [])
                    .Select(r => new ProcessingOutputRoute
                    {
                        RouteName = r.RouteName,
                        TargetTopic = r.TargetTopic,
                        IsRequired = r.IsRequired,
                    })
                    .ToArray(),
            })
            .ToArray();
    }
}

public sealed class EventReaderProfileSettings
{
    public string Name { get; init; } = string.Empty;
    public string? Description { get; init; }
    public bool IsActive { get; init; } = true;
    public string? ClassificationExpression { get; init; }
    public string? ParseExpression { get; init; }
    public Dictionary<string, string>? RoutingTagOverrides { get; init; }
    public List<EventReaderOutputRouteSettings>? OutputRoutes { get; init; }
}

public sealed class EventReaderOutputRouteSettings
{
    public string RouteName { get; init; } = string.Empty;
    public string TargetTopic { get; init; } = string.Empty;
    public bool IsRequired { get; init; } = true;
}
