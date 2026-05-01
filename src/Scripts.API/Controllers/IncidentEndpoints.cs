using EventReader.Scripts.API.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace EventReader.Scripts.API.Controllers;

public static class IncidentEndpoints
{
    public static IEndpointRouteBuilder MapIncidentEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/incidents").WithTags("Incidents");

        group.MapGet("/lag", GetPartitionLag);
        group.MapGet("/rebalances", GetRebalances);
        group.MapGet("/dlq", GetDlq);
        group.MapGet("/consumer-health", GetConsumerHealth);

        return app;
    }

    private static async Task<IResult> GetPartitionLag(
        IIncidentTelemetryProvider provider,
        CancellationToken ct)
    {
        var lag = await provider.GetPartitionLagAsync(ct);
        return Results.Ok(lag);
    }

    private static async Task<IResult> GetRebalances(
        IIncidentTelemetryProvider provider,
        CancellationToken ct)
    {
        var rebalances = await provider.GetRecentRebalancesAsync(ct);
        return Results.Ok(rebalances);
    }

    private static async Task<IResult> GetDlq(
        IIncidentTelemetryProvider provider,
        CancellationToken ct)
    {
        var dlqSnapshots = await provider.GetDlqSnapshotsAsync(ct);
        return Results.Ok(dlqSnapshots);
    }

    private static async Task<IResult> GetConsumerHealth(
        IIncidentTelemetryProvider provider,
        CancellationToken ct)
    {
        var snapshot = await provider.GetConsumerHealthAsync(ct);
        return Results.Ok(snapshot);
    }
}
