using Microsoft.AspNetCore.SignalR;

namespace EventReader.Scripts.API.Hubs;

/// <summary>
/// SignalR hub for streaming shadow test results to connected clients.
/// Clients join/leave session groups to receive scoped events.
/// </summary>
public sealed class ShadowTestHub : Hub
{
    public Task JoinSession(string sessionId)
    {
        return Groups.AddToGroupAsync(Context.ConnectionId, sessionId);
    }

    public Task LeaveSession(string sessionId)
    {
        return Groups.RemoveFromGroupAsync(Context.ConnectionId, sessionId);
    }
}
