using EventReader.Scripts.API.Data;
using EventReader.Scripts.API.Options;
using EventReader.Scripts.API.Services;
using KF.Scripts.Core;
using KF.Scripts.Interfaces;
using KoreForge.Jex;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EventReader.Scripts.API;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers all KafkaProcessor Scripts API services, including
    /// JEX script compilation, function management, shadow testing, and script reload.
    /// </summary>
    public static IServiceCollection AddKafkaProcessorScriptsApi(
        this IServiceCollection services,
        Action<ScriptsApiOptions> configure)
    {
        var opts = new ScriptsApiOptions();
        configure(opts);

        services.TryAddSingleton(opts);

        // DbContext for FunctionDefinitions + FunctionScripts
        services.AddDbContextFactory<KafkaProcessorScriptsDbContext>(o =>
            o.UseSqlServer(opts.ConnectionString));

        // JEX compiler engine
        services.TryAddSingleton<IJexCompiler>(new Jex());

        // Register JexScriptCompiler as an IScriptCompiler for the Scripts library
        services.AddScriptCompiler<JexScriptCompiler>("jex");

        // Repositories
        services.TryAddSingleton<FunctionDefinitionRepository>();
        services.TryAddSingleton<FunctionScriptRepository>();

        // Script reload
        services.TryAddSingleton<ScriptReloadService>();
        services.AddHostedService(sp => sp.GetRequiredService<ScriptReloadService>());

        // Shadow testing
        services.TryAddSingleton<ShadowTestService>();
        services.TryAddSingleton<IIncidentTelemetryProvider, NoOpIncidentTelemetryProvider>();

        return services;
    }
}
