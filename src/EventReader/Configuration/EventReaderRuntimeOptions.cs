using Event.Streaming.Processing.Pipeline;

namespace EventReader.Configuration;

public sealed class EventReaderRuntimeOptions
{
    public DiagnosticStage DiagnosticStage { get; set; } = DiagnosticStage.FullPipeline;
    public bool EnableDlq { get; set; }
    public string DlqTopic { get; set; } = "stream.dlq";
    public string ApplicationRole { get; set; } = "reader";
    public string InstanceId { get; set; } = "1";
    public string Version { get; set; } = "1.0.0";

    public static EventReaderRuntimeOptions FromConfiguration(IConfiguration configuration)
    {
        var stageText = configuration["EventReader:DiagnosticStage"] ?? nameof(DiagnosticStage.FullPipeline);
        if (!Enum.TryParse<DiagnosticStage>(stageText, ignoreCase: true, out var stage))
        {
            stage = DiagnosticStage.FullPipeline;
        }

        return new EventReaderRuntimeOptions
        {
            DiagnosticStage = stage,
            EnableDlq = bool.TryParse(configuration["EventReader:EnableDlq"], out var enableDlq) && enableDlq,
            DlqTopic = configuration["EventReader:DlqTopic"] ?? "stream.dlq",
            ApplicationRole = configuration["EventReader:ApplicationRole"] ?? "reader",
            InstanceId = configuration["KoreForgeSettings:InstanceId"] ?? configuration["KoreForge:Settings:Instance"] ?? "1",
            Version = configuration["KoreForgeSettings:ClientAppVersion"] ?? configuration["KoreForge:Settings:ClientAppVersion"] ?? "1.0.0",
        };
    }

    public static void ApplyConfiguration(EventReaderRuntimeOptions options, IConfiguration configuration)
    {
        var resolved = FromConfiguration(configuration);
        options.DiagnosticStage = resolved.DiagnosticStage;
        options.EnableDlq = resolved.EnableDlq;
        options.DlqTopic = resolved.DlqTopic;
        options.ApplicationRole = resolved.ApplicationRole;
        options.InstanceId = resolved.InstanceId;
        options.Version = resolved.Version;
    }
}

public interface IEventReaderRuntimeOptionsProvider
{
    EventReaderRuntimeOptions Current { get; }
}

public sealed class ConfigurationEventReaderRuntimeOptionsProvider : IEventReaderRuntimeOptionsProvider
{
    private readonly Microsoft.Extensions.Options.IOptionsMonitor<EventReaderRuntimeOptions> _options;

    public ConfigurationEventReaderRuntimeOptionsProvider(Microsoft.Extensions.Options.IOptionsMonitor<EventReaderRuntimeOptions> options)
    {
        _options = options;
    }

    public EventReaderRuntimeOptions Current => _options.CurrentValue;
}
