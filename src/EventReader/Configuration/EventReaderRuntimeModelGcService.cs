using Event.Streaming.Processing.WorkStore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventReader.Configuration;

/// <summary>
/// Background service that periodically collects stale runtime model versions.
/// A version is collected when no non-terminal work items reference it.
/// </summary>
public sealed class EventReaderRuntimeModelGcService : BackgroundService
{
    private readonly IEventReaderRuntimeModelProvider _modelProvider;
    private readonly IEventReaderWorkStore _workStore;
    private readonly ILogger<EventReaderRuntimeModelGcService> _log;
    private readonly TimeSpan _interval;

    public EventReaderRuntimeModelGcService(
        IEventReaderRuntimeModelProvider modelProvider,
        IEventReaderWorkStore workStore,
        ILogger<EventReaderRuntimeModelGcService> log,
        TimeSpan? interval = null)
    {
        _modelProvider = modelProvider;
        _workStore = workStore;
        _log = log;
        _interval = interval ?? TimeSpan.FromMinutes(5);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _log.LogInformation("Runtime model GC service started (interval={Interval})", _interval);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_interval, stoppingToken).ConfigureAwait(false);

                var activeVersions = _workStore.GetActiveRuntimeModelVersions();
                var result = await _modelProvider
                    .CollectGarbageAsync(activeVersions, stoppingToken)
                    .ConfigureAwait(false);

                if (result.VersionsCollected > 0 || result.VersionsRetained > 1)
                {
                    _log.LogInformation(
                        "Runtime model GC: collected={Collected} retained={Retained} activeWorkStoreVersions={Active}",
                        result.VersionsCollected,
                        result.VersionsRetained,
                        activeVersions.Count);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Runtime model GC encountered an error");
            }
        }

        _log.LogInformation("Runtime model GC service stopped");
    }
}
