using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventReader.Configuration;

/// <summary>
/// Eagerly hydrates the <see cref="EventReaderRuntimeModelProvider"/> from the database at
/// host startup, before any hosted service begins consuming Kafka messages.
/// <para>
/// Without this, the provider sits at version 0 (empty model) until the first KoreForge.Settings
/// poll-cycle fires a config-reload token — which can be seconds to minutes after launch.
/// By implementing <see cref="IHostedService"/> directly (not <see cref="BackgroundService"/>),
/// <see cref="StartAsync"/> is awaited by the host before any subsequent service starts.
/// </para>
/// </summary>
internal sealed class EventReaderModelStartupService(
    EventReaderRuntimeModelProvider provider,
    ILogger<EventReaderModelStartupService> log) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        log.LogInformation("Performing initial EventReader runtime model load from database...");

        try
        {
            await provider.ReloadAsync(cancellationToken).ConfigureAwait(false);
            log.LogInformation(
                "EventReader runtime model loaded: version={Version}, functions={FunctionCount}, sourceSystems={SourceSystemCount}",
                provider.CurrentVersion,
                provider.Current.Functions.Count,
                provider.Current.SourceSystems.Count);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Log but do not rethrow — the system can start with an empty model and reload
            // via KoreForge.Settings polling. Failing here would block the entire host from starting.
            log.LogError(
                ex,
                "EventReader runtime model initial load failed; starting with empty model (version 0). " +
                "Kafka consumption will be paused until the model is loaded via KoreForge.Settings reload.");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
