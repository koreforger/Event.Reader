using EventReader.Scripts.API.Models;
using EventReader.Scripts.API.Services;
using KF.Scripts.Interfaces;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;

namespace EventReader.Scripts.API.Controllers;

public static class FunctionEndpoints
{
    public static IEndpointRouteBuilder MapFunctionEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/functions").WithTags("Functions");

        group.MapGet("/", ListFunctions);
        group.MapGet("/{id:long}", GetFunction);
        group.MapPost("/", CreateFunction);
        group.MapPut("/{id:long}", UpdateFunction);
        group.MapDelete("/{id:long}", DeleteFunction);
        group.MapGet("/{id:long}/scripts", GetFunctionScripts);
        group.MapPut("/{id:long}/scripts", AssignFunctionScripts);
        group.MapGet("/by-script/{scriptId:long}", GetFunctionsByScript);

        return app;
    }

    private static async Task<IResult> ListFunctions(
        FunctionDefinitionRepository repo,
        FunctionScriptRepository scriptRepo,
        CancellationToken ct)
    {
        var functions = await repo.LoadAllAsync(ct);
        var scriptCounts = await scriptRepo.GetScriptCountsByFunctionAsync(ct);

        var result = functions.Select(f => new
        {
            f.FunctionId,
            f.FunctionName,
            f.ActionRegex,
            f.GroupId,
            f.GroupName,
            f.Description,
            f.DescriptionFormat,
            f.IsEnabled,
            f.CreatedDate,
            f.ModifiedDate,
            ScriptCount = scriptCounts.GetValueOrDefault(f.FunctionId, 0),
        });

        return Results.Ok(result);
    }

    private static async Task<IResult> GetFunction(
        long id,
        FunctionDefinitionRepository functionRepo,
        FunctionScriptRepository scriptRepo,
        IScriptStore scriptStore,
        CancellationToken ct)
    {
        var function = await functionRepo.GetByIdAsync(id, ct);
        if (function is null) return Results.NotFound();

        var assignments = await scriptRepo.GetAssignmentsForFunctionAsync(id, ct);

        // Batch-fetch script names
        var scriptIds = assignments.Select(a => a.ScriptId).Distinct().ToList();
        var scriptNames = new Dictionary<long, string>();
        foreach (var sid in scriptIds)
        {
            var script = await scriptStore.GetByIdAsync(sid, ct);
            if (script is not null) scriptNames[sid] = script.Name;
        }

        var scripts = assignments.Select(a => new
        {
            a.FunctionId,
            a.ScriptId,
            a.Role,
            a.Ordinal,
            a.IsEnabled,
            a.CreatedDate,
            ScriptName = scriptNames.GetValueOrDefault(a.ScriptId, $"Script #{a.ScriptId}"),
        });

        return Results.Ok(new
        {
            function.FunctionId,
            function.FunctionName,
            function.ActionRegex,
            function.GroupId,
            function.GroupName,
            function.Description,
            function.DescriptionFormat,
            function.IsEnabled,
            function.CreatedDate,
            function.ModifiedDate,
            Scripts = scripts,
        });
    }

    private static async Task<IResult> GetFunctionsByScript(
        long scriptId,
        FunctionScriptRepository scriptRepo,
        FunctionDefinitionRepository functionRepo,
        CancellationToken ct)
    {
        var assignments = await scriptRepo.GetAssignmentsForScriptAsync(scriptId, ct);
        var functionIds = assignments.Select(a => a.FunctionId).Distinct().ToList();

        var allFunctions = await functionRepo.LoadAllAsync(ct);
        var functionMap = allFunctions.ToDictionary(f => f.FunctionId);

        var result = assignments.Select(a => new
        {
            a.FunctionId,
            FunctionName = functionMap.TryGetValue(a.FunctionId, out var f) ? f.FunctionName : $"Function #{a.FunctionId}",
            a.ScriptId,
            a.Role,
            a.Ordinal,
            a.IsEnabled,
        });

        return Results.Ok(result);
    }

    private static async Task<IResult> CreateFunction(
        CreateFunctionDto dto,
        FunctionDefinitionRepository repo,
        CancellationToken ct)
    {
        var function = new FunctionDefinition
        {
            FunctionId = dto.FunctionId,
            FunctionName = dto.FunctionName,
            ActionRegex = dto.ActionRegex,
            GroupId = dto.GroupId,
            GroupName = dto.GroupName ?? string.Empty,
            Description = dto.Description ?? string.Empty,
            DescriptionFormat = dto.DescriptionFormat ?? "markdown",
            IsEnabled = dto.IsEnabled ?? true,
        };

        try
        {
            var created = await repo.CreateAsync(function, ct);
            return Results.Created($"/api/functions/{created.FunctionId}", created);
        }
        catch (InvalidOperationException ex)
        {
            return Results.Conflict(new { error = ex.Message });
        }
    }

    private static async Task<IResult> UpdateFunction(
        [FromRoute] long id,
        [FromBody] UpdateFunctionDto dto,
        FunctionDefinitionRepository repo,
        CancellationToken ct)
    {
        try
        {
            var function = new FunctionDefinition
            {
                FunctionId = id,
                FunctionName = dto.FunctionName,
                ActionRegex = dto.ActionRegex,
                GroupId = dto.GroupId,
                GroupName = dto.GroupName ?? string.Empty,
                Description = dto.Description ?? string.Empty,
                DescriptionFormat = dto.DescriptionFormat ?? "markdown",
                IsEnabled = dto.IsEnabled ?? true,
            };

            var updated = await repo.UpdateAsync(function, ct);
            return Results.Ok(updated);
        }
        catch (KeyNotFoundException)
        {
            return Results.NotFound();
        }
    }

    private static async Task<IResult> DeleteFunction(
        long id,
        FunctionDefinitionRepository repo,
        CancellationToken ct)
    {
        try
        {
            await repo.DeleteAsync(id, ct);
            return Results.NoContent();
        }
        catch (KeyNotFoundException)
        {
            return Results.NotFound();
        }
    }

    private static async Task<IResult> GetFunctionScripts(
        long id,
        FunctionScriptRepository repo,
        CancellationToken ct)
    {
        var assignments = await repo.GetAssignmentsForFunctionAsync(id, ct);
        return Results.Ok(assignments);
    }

    private static async Task<IResult> AssignFunctionScripts(
        [FromRoute] long id,
        [FromBody] AssignFunctionScriptsDto dto,
        FunctionScriptRepository repo,
        CancellationToken ct)
    {
        var requests = dto.Assignments
            .Select(a => new AssignmentRequest(a.ScriptId, a.Role, a.Ordinal))
            .ToList();

        await repo.ReplaceAssignmentsAsync(id, requests, ct);
        return Results.NoContent();
    }
}

public sealed record CreateFunctionDto(
    long FunctionId,
    string FunctionName,
    string ActionRegex,
    int GroupId,
    string? GroupName,
    string? Description,
    string? DescriptionFormat,
    bool? IsEnabled);

public sealed record UpdateFunctionDto(
    string FunctionName,
    string ActionRegex,
    int GroupId,
    string? GroupName,
    string? Description,
    string? DescriptionFormat,
    bool? IsEnabled);

public sealed record AssignFunctionScriptsDto(
    IReadOnlyList<ScriptAssignmentDto> Assignments);

public sealed record ScriptAssignmentDto(
    long ScriptId,
    string Role,
    int Ordinal);
