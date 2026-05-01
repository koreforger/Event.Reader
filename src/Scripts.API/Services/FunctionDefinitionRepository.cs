using EventReader.Scripts.API.Data;
using EventReader.Scripts.API.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace EventReader.Scripts.API.Services;

/// <summary>
/// Repository for FunctionDefinitions. Reads/writes function metadata from the database.
/// </summary>
internal sealed class FunctionDefinitionRepository
{
    private readonly IDbContextFactory<KafkaProcessorScriptsDbContext> _dbFactory;
    private readonly ILogger<FunctionDefinitionRepository> _logger;

    public FunctionDefinitionRepository(
        IDbContextFactory<KafkaProcessorScriptsDbContext> dbFactory,
        ILogger<FunctionDefinitionRepository> logger)
    {
        _dbFactory = dbFactory;
        _logger = logger;
    }

    public async Task<IReadOnlyList<FunctionDefinition>> LoadAllAsync(CancellationToken ct = default)
    {
        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        return await db.FunctionDefinitions
            .AsNoTracking()
            .Select(e => MapToModel(e))
            .ToListAsync(ct);
    }

    public async Task<FunctionDefinition?> GetByIdAsync(long functionId, CancellationToken ct = default)
    {
        await using var db = await _dbFactory.CreateDbContextAsync(ct);
        var entity = await db.FunctionDefinitions.AsNoTracking()
            .FirstOrDefaultAsync(e => e.FunctionId == functionId, ct);
        return entity is null ? null : MapToModel(entity);
    }

    public async Task<FunctionDefinition> CreateAsync(FunctionDefinition function, CancellationToken ct = default)
    {
        await using var db = await _dbFactory.CreateDbContextAsync(ct);

        if (await db.FunctionDefinitions.AnyAsync(x => x.FunctionId == function.FunctionId, ct))
            throw new InvalidOperationException($"FunctionId {function.FunctionId} already exists.");

        if (await db.FunctionDefinitions.AnyAsync(x => x.FunctionName == function.FunctionName, ct))
            throw new InvalidOperationException($"FunctionName '{function.FunctionName}' already exists.");

        var entity = new FunctionDefinitionEntity
        {
            FunctionId = function.FunctionId,
            FunctionName = function.FunctionName,
            ActionRegex = function.ActionRegex,
            GroupId = function.GroupId,
            GroupName = function.GroupName,
            Description = function.Description,
            DescriptionFormat = function.DescriptionFormat,
            IsEnabled = function.IsEnabled,
            CreatedDate = DateTime.UtcNow,
            ModifiedDate = DateTime.UtcNow,
        };

        db.FunctionDefinitions.Add(entity);
        await db.SaveChangesAsync(ct);

        _logger.LogInformation("Created function {FunctionId} '{FunctionName}'", entity.FunctionId, entity.FunctionName);
        return MapToModel(entity);
    }

    public async Task<FunctionDefinition> UpdateAsync(FunctionDefinition function, CancellationToken ct = default)
    {
        await using var db = await _dbFactory.CreateDbContextAsync(ct);

        var entity = await db.FunctionDefinitions.FindAsync([function.FunctionId], ct)
            ?? throw new KeyNotFoundException($"Function {function.FunctionId} not found.");

        entity.FunctionName = function.FunctionName;
        entity.ActionRegex = function.ActionRegex;
        entity.GroupId = function.GroupId;
        entity.GroupName = function.GroupName;
        entity.Description = function.Description;
        entity.DescriptionFormat = function.DescriptionFormat;
        entity.IsEnabled = function.IsEnabled;
        entity.ModifiedDate = DateTime.UtcNow;

        await db.SaveChangesAsync(ct);

        _logger.LogInformation("Updated function {FunctionId} '{FunctionName}'", entity.FunctionId, entity.FunctionName);
        return MapToModel(entity);
    }

    public async Task DeleteAsync(long functionId, CancellationToken ct = default)
    {
        await using var db = await _dbFactory.CreateDbContextAsync(ct);

        var entity = await db.FunctionDefinitions.FindAsync([functionId], ct)
            ?? throw new KeyNotFoundException($"Function {functionId} not found.");

        db.FunctionDefinitions.Remove(entity);
        await db.SaveChangesAsync(ct);

        _logger.LogInformation("Deleted function {FunctionId} '{FunctionName}'", entity.FunctionId, entity.FunctionName);
    }

    private static FunctionDefinition MapToModel(FunctionDefinitionEntity e) => new()
    {
        FunctionId = e.FunctionId,
        FunctionName = e.FunctionName,
        ActionRegex = e.ActionRegex,
        GroupId = e.GroupId,
        GroupName = e.GroupName,
        Description = e.Description,
        DescriptionFormat = e.DescriptionFormat,
        IsEnabled = e.IsEnabled,
        CreatedDate = e.CreatedDate,
        ModifiedDate = e.ModifiedDate,
    };
}
