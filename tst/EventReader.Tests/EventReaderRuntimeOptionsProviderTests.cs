using EventReader.Configuration;
using Event.Streaming.Processing.Pipeline;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace EventReader.Tests;

public sealed class EventReaderRuntimeOptionsProviderTests
{
    [Fact]
    public void Current_UsesOptionsMonitorAfterConfigurationReload()
    {
        var configuration = new ConfigurationManager();
        configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["EventReader:DiagnosticStage"] = "FullPipeline",
            ["EventReader:EnableDlq"] = "false",
            ["KoreForgeSettings:InstanceId"] = "reader-a",
            ["KoreForgeSettings:ClientAppVersion"] = "1.0.0",
        });

        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(configuration);
        services
            .AddOptions<EventReaderRuntimeOptions>()
            .BindConfiguration("EventReader")
            .PostConfigure<IConfiguration>((options, config) =>
                EventReaderRuntimeOptions.ApplyConfiguration(options, config));
        services.AddSingleton<IEventReaderRuntimeOptionsProvider, ConfigurationEventReaderRuntimeOptionsProvider>();

        using var provider = services.BuildServiceProvider();
        var optionsProvider = provider.GetRequiredService<IEventReaderRuntimeOptionsProvider>();

        Assert.Equal(DiagnosticStage.FullPipeline, optionsProvider.Current.DiagnosticStage);

        configuration["EventReader:DiagnosticStage"] = "KafkaOnly";
        ((IConfigurationRoot)configuration).Reload();

        Assert.Equal(DiagnosticStage.KafkaOnly, optionsProvider.Current.DiagnosticStage);
    }
}
