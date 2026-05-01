using System.Text.Json;
using EventReader.Api;
using EventReader.Configuration;
using EventReader.Monitoring;
using Event.Streaming.Processing.Pipeline;
using Microsoft.AspNetCore.Mvc;

namespace EventReader.Tests;

public sealed class RuntimeControllerTests
{
    [Fact]
    public void RuntimeState_ProjectsCurrentOptionsEvenWhenSnapshotIsStale()
    {
        var snapshot = new EventReaderMonitoringSnapshot
        {
            Instance = "stale-instance",
            Version = "0.0.0",
            DiagnosticStage = "FullPipeline",
        };
        var options = new EventReaderRuntimeOptions
        {
            DiagnosticStage = DiagnosticStage.KafkaOnly,
            InstanceId = "reader-live",
            Version = "2.0.0",
            ApplicationRole = "reader",
        };
        var controller = new RuntimeController(snapshot, new StaticOptionsProvider(options));

        var result = Assert.IsType<OkObjectResult>(controller.GetState());
        using var document = JsonDocument.Parse(JsonSerializer.Serialize(result.Value));
        var root = document.RootElement;

        Assert.Equal("reader-live", root.GetProperty("InstanceId").GetString());
        Assert.Equal("2.0.0", root.GetProperty("Version").GetString());
        Assert.Equal("KafkaOnly", root.GetProperty("DiagnosticStage").GetString());
        Assert.Equal("reader-live", snapshot.Instance);
        Assert.Equal("KafkaOnly", snapshot.DiagnosticStage);
    }

    private sealed class StaticOptionsProvider(EventReaderRuntimeOptions options) : IEventReaderRuntimeOptionsProvider
    {
        public EventReaderRuntimeOptions Current { get; } = options;
    }
}
