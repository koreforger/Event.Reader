using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Event.Data;

public static class EventDataServiceCollectionExtensions
{
    /// <summary>
    /// Registers an <see cref="IDbContextFactory{EventDataContext}"/> (singleton) for use from
    /// background services and hosted services that operate outside request scope.
    /// </summary>
    public static IServiceCollection AddEventDataFactory(
        this IServiceCollection services,
        string connectionString)
    {
        services.AddDbContextFactory<EventDataContext>(db =>
            db.UseSqlServer(connectionString));

        return services;
    }

    /// <summary>
    /// Registers an <see cref="IDbContextFactory{EventDataContext}"/> using an options delegate.
    /// </summary>
    public static IServiceCollection AddEventDataFactory(
        this IServiceCollection services,
        Action<EventDataOptions> configure)
    {
        var options = new EventDataOptions();
        configure(options);
        return services.AddEventDataFactory(options.ConnectionString);
    }

    /// <summary>
    /// Registers an <see cref="IDbContextFactory{EventDataContext}"/> resolving the connection
    /// string from the DI container at the time the factory is first requested.
    /// </summary>
    public static IServiceCollection AddEventDataFactory(
        this IServiceCollection services,
        Func<IServiceProvider, string> connectionStringFactory)
    {
        services.AddDbContextFactory<EventDataContext>((sp, db) =>
            db.UseSqlServer(connectionStringFactory(sp)));

        return services;
    }
}

