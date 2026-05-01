using EventReader.Scripts.API.Hubs;
using EventReader.Scripts.API.Models;
using EventReader.Scripts.API.Services;
using KoreForge.Jex;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.SignalR;

namespace EventReader.Scripts.API.Controllers;

public static class ShadowTestEndpoints
{
    public static IEndpointRouteBuilder MapShadowTestEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/shadow-test").WithTags("ShadowTest");

        group.MapGet("/", ListActiveSessions);
        group.MapPost("/", StartShadowTest);
        group.MapGet("/{sessionId}", GetShadowTestStatus);
        group.MapDelete("/{sessionId}", StopShadowTest);
        group.MapPost("/{sessionId}/promote", PromoteShadowTest);

        return app;
    }

    private static IResult ListActiveSessions(ShadowTestService shadowService)
    {
        var sessions = shadowService.ActiveSessions.Select(s => new
        {
            s.SessionId,
            s.FunctionId,
            s.FunctionName,
            s.SampleSize,
            Remaining = s.Remaining,
            s.StartedAt,
            s.TimeoutAt,
            Status = shadowService.IsSessionComplete(s.SessionId) ? "complete" : "active",
        });
        return Results.Ok(sessions);
    }

    private static async Task<IResult> StartShadowTest(
        StartShadowTestDto dto,
        ShadowTestService shadowService,
        ScriptReloadService reloadService,
        FunctionScriptRepository scriptRepo,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(dto.CandidateContent) && dto.CandidateScriptId is null)
            return Results.BadRequest("Either candidateContent or candidateScriptId must be provided.");

        // Resolve current live extract program for the function.
        IJexProgram? currentProgram = null;
        var assignments = await scriptRepo.GetAssignmentsForFunctionAsync(dto.FunctionId, ct);
        var liveExtract = assignments
            .Where(a => a.IsEnabled && string.Equals(a.Role, "extract", StringComparison.OrdinalIgnoreCase))
            .OrderBy(a => a.Ordinal)
            .FirstOrDefault();

        if (liveExtract is not null)
        {
            var compiled = reloadService.GetCompiledScript(liveExtract.ScriptId);
            currentProgram = compiled?.Program;
        }

        try
        {
            var session = shadowService.StartSession(
                dto.FunctionId,
                dto.FunctionName ?? $"Function-{dto.FunctionId}",
                dto.CandidateContent ?? string.Empty,
                dto.CandidateScriptId,
                currentProgram,
                dto.SampleSize,
                dto.TimeoutSeconds);

            return Results.Ok(new
            {
                session.SessionId,
                session.FunctionId,
                session.FunctionName,
                session.SampleSize,
                TimeoutSeconds = (int)(session.TimeoutAt - session.StartedAt).TotalSeconds,
                session.StartedAt,
                Status = "active",
            });
        }
        catch (JexCompileException ex)
        {
            return Results.BadRequest(new { Error = "Compilation failed", ex.Message });
        }
        catch (InvalidOperationException ex)
        {
            return Results.Conflict(new { ex.Message });
        }
    }

    private static IResult GetShadowTestStatus(
        string sessionId,
        ShadowTestService shadowService)
    {
        var session = shadowService.GetSession(sessionId);
        if (session is null) return Results.NotFound();

        return Results.Ok(new
        {
            session.SessionId,
            session.FunctionId,
            session.FunctionName,
            session.SampleSize,
            Remaining = session.Remaining,
            session.StartedAt,
            session.TimeoutAt,
            Status = shadowService.IsSessionComplete(sessionId) ? "complete" : "active",
        });
    }

    private static IResult StopShadowTest(
        string sessionId,
        ShadowTestService shadowService,
        IHubContext<ShadowTestHub> hubContext)
    {
        var summary = shadowService.StopSession(sessionId, "cancelled");
        if (summary is null) return Results.NotFound();
        var payload = ToSummaryPayload(summary);

        // Notify connected clients with both canonical and legacy event names.
        _ = Task.WhenAll(
            hubContext.Clients.Group(sessionId).SendAsync("shadow-complete", payload),
            hubContext.Clients.Group(sessionId).SendAsync("ShadowTestComplete", payload));

        return Results.Ok(payload);
    }

    private static async Task<IResult> PromoteShadowTest(
        string sessionId,
        ShadowTestService shadowService,
        KF.Scripts.Interfaces.IScriptStore scriptStore,
        CancellationToken ct)
    {
        var session = shadowService.GetSession(sessionId);
        if (session is null) return Results.NotFound();

        // Save the candidate content as a new script or update existing
        if (session.CandidateScriptId.HasValue)
        {
            var existing = await scriptStore.GetByIdAsync(session.CandidateScriptId.Value, ct);
            if (existing is not null)
            {
                var updated = await scriptStore.UpdateAsync(new KF.Scripts.Models.UpdateScriptRequest(
                    existing.ScriptId,
                    session.CandidateContent,
                    existing.Description,
                    existing.IsEnabled,
                    existing.RowVersion,
                    "shadow-test",
                    $"Promoted from shadow test session {sessionId}"), ct);

                shadowService.StopSession(sessionId, "promoted");
                return Results.Ok(new { Script = updated, PreviousContent = existing.Content });
            }
        }

        // Create a new script if no existing one
        var created = await scriptStore.CreateAsync(new KF.Scripts.Models.CreateScriptRequest(
            $"shadow-promoted-{session.FunctionId}-{DateTime.UtcNow:yyyyMMddHHmmss}",
            "extract",
            "jex",
            session.CandidateContent,
            $"Promoted from shadow test session {sessionId}",
            "shadow-test",
            $"Promoted from shadow test session {sessionId}"), ct);

        shadowService.StopSession(sessionId, "promoted");
        return Results.Ok(new { Script = created });
    }

    private static object ToSummaryPayload(ShadowTestSummary summary)
    {
        var mismatchCount = Math.Max(0, summary.TotalProcessed - summary.MatchCount);
        var matchPercentage = summary.TotalProcessed == 0
            ? 0
            : (summary.MatchCount / (double)summary.TotalProcessed) * 100;

        return new
        {
            summary.SessionId,
            summary.FunctionId,
            summary.TotalProcessed,
            summary.MatchCount,
            summary.DiffCount,
            summary.AvgCurrentTimeMs,
            summary.AvgCandidateTimeMs,
            summary.CompletedAt,
            summary.Reason,
            TotalSamples = summary.TotalProcessed,
            MismatchCount = mismatchCount,
            MatchPercentage = matchPercentage,
            FieldDiffSummary = new Dictionary<string, int>(),
        };
    }
}

public sealed record StartShadowTestDto(
    long FunctionId,
    string? FunctionName,
    string? CandidateContent,
    long? CandidateScriptId,
    int SampleSize = 20,
    int? TimeoutSeconds = null);
