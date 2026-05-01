using Microsoft.AspNetCore.SignalR;
using System.Runtime.CompilerServices;
using EventReader.Configuration;
using EventReader.Monitoring;

namespace EventReader.Hubs;

/// <summary>
/// Pushes settings-change notifications to connected clients in real time.
/// Clients subscribe and receive SettingsRefreshed events whenever SQL settings are reloaded.
/// </summary>
public sealed class SettingsHub : Hub
{
    /// <summary>Broadcast a settings-refresh notification to all connected clients.</summary>
    public static async Task BroadcastRefreshAsync(IHubContext<SettingsHub> context, string version)
        => await context.Clients.All.SendAsync("SettingsRefreshed", new { Version = version, RefreshedAt = DateTimeOffset.UtcNow });
}

/// <summary>
/// Pushes monitoring snapshots and incident notifications to connected clients.
/// </summary>
public sealed class MonitoringHub : Hub
{
    private readonly EventReaderMonitoringSnapshot _snapshot;
    private readonly IEventReaderRuntimeOptionsProvider _optionsProvider;

    public MonitoringHub(
        EventReaderMonitoringSnapshot snapshot,
        IEventReaderRuntimeOptionsProvider optionsProvider)
    {
        _snapshot = snapshot;
        _optionsProvider = optionsProvider;
    }

    public async IAsyncEnumerable<EventReaderMonitoringSnapshot> StreamSnapshots(
        int intervalMs = 1000,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var delay = TimeSpan.FromMilliseconds(Math.Clamp(intervalMs, 500, 30_000));

        while (!cancellationToken.IsCancellationRequested)
        {
            yield return _snapshot.ApplyRuntimeOptions(_optionsProvider.Current, DateTimeOffset.UtcNow);
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Broadcast a monitoring snapshot to all connected clients.</summary>
    public static async Task BroadcastSnapshotAsync(IHubContext<MonitoringHub> context, object snapshot)
        => await context.Clients.All.SendAsync("MonitoringSnapshot", snapshot);

    /// <summary>Broadcast a new incident to all connected clients.</summary>
    public static async Task BroadcastIncidentAsync(IHubContext<MonitoringHub> context, object incident)
        => await context.Clients.All.SendAsync("NewIncident", incident);
}
