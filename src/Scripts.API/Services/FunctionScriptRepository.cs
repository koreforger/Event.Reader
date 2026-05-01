using EventReader.Scripts.API.Data;
using EventReader.Scripts.API.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace EventReader.Scripts.API.Services;

/// <summary>
/// Repository for the FunctionScripts junction table — manages script assignments to functions.
/// </summary>
internal sealed class FunctionScriptRepository
{
    private readonly IDbContextFactory<KafkaProcessorScriptsDbContext> _dbFactory;
    private readonly ILogger<FunctionScriptRepository> _logger;

    public FunctionScriptRepository(
        IDbContextFactory<KafkaProcessorScriptsDbContext> dbFactory,
        ILogger<FunctionScriptRepository> logger)
    {
        _dbFactory = dbFactory;
        _logger = logger;
    }

    /// <summary>
    /// Returns all enabled extract-role assignments across all functions, ordered by FunctionId then Ordinal.
    /// Used at startup to map each function to its compiled extraction script.
    /// </summary>
    public async Task<IReadOnlyList<FunctionScriptAssignment>> GetAllExtractAssignmentsAsync(CancellationToken ct = default)
    {
        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        return await db.FunctionScripts
            .AsNoTracking()
            .Where(e => e.IsEnabled && e.Role == "extract")
            .OrderBy(e => e.FunctionId)
            .ThenBy(e => e.Ordinal)
            .Select(e => MapToModel(e))
            .ToListAsync(ct);
    }

    public async Task<IReadOnlyList<FunctionScriptAssignment>> GetAssignmentsForFunctionAsync(
        long functionId, CancellationToken ct = default)
    {
        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        return await db.FunctionScripts
            .AsNoTracking()
            .Where(e => e.FunctionId == functionId)
            .OrderBy(e => e.Ordinal)
            .Select(e => MapToModel(e))
            .ToListAsync(ct);
    }

    public async Task<IReadOnlyList<FunctionScriptAssignment>> GetAssignmentsForScriptAsync(
        long scriptId, CancellationToken ct = default)
    {
        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        return await db.FunctionScripts
            .AsNoTracking()
            .Where(e => e.ScriptId == scriptId)
            .Select(e => MapToModel(e))
            .ToListAsync(ct);
    }

    public async Task<Dictionary<long, int>> GetScriptCountsByFunctionAsync(CancellationToken ct = default)
    {
        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        return await db.FunctionScripts
            .AsNoTracking()
            .GroupBy(e => e.FunctionId)
            .ToDictionaryAsync(g => g.Key, g => g.Count(), ct);
    }

    public async Task ReplaceAssignmentsAsync(
        long functionId,
        IReadOnlyList<AssignmentRequest> assignments,
        CancellationToken ct = default)
    {
        await using var db = await _dbFactory.CreateDbContextAsync(ct);

        var existing = await db.FunctionScripts
            .Where(e => e.FunctionId == functionId)
            .ToListAsync(ct);

        db.FunctionScripts.RemoveRange(existing);

        var now = DateTime.UtcNow;
        foreach (var a in assignments)
        {
            db.FunctionScripts.Add(new FunctionScriptEntity
            {
                FunctionId = functionId,
                ScriptId = a.ScriptId,
                Role = a.Role,
                Ordinal = a.Ordinal,
                IsEnabled = true,
                CreatedDate = now,
            });
        }

        await db.SaveChangesAsync(ct);

        _logger.LogInformation(
            "Replaced assignments for function {FunctionId}: {Count} scripts assigned",
            functionId, assignments.Count);
    }

    private static FunctionScriptAssignment MapToModel(FunctionScriptEntity e) => new()
    {
        FunctionId = e.FunctionId,
        ScriptId = e.ScriptId,
        Role = e.Role,
        Ordinal = e.Ordinal,
        IsEnabled = e.IsEnabled,
        CreatedDate = e.CreatedDate,
    };
}

/// <summary>
/// Request body for assigning a script to a function.
/// </summary>
public sealed record AssignmentRequest(long ScriptId, string Role, int Ordinal);
