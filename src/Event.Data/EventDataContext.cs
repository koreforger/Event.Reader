using Event.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Event.Data;

public sealed class EventDataContext : DbContext
{
    public EventDataContext(DbContextOptions<EventDataContext> options) : base(options)
    {
    }

    public DbSet<ErConfig> Configs { get; set; } = null!;
    public DbSet<ErSourceSystem> SourceSystems { get; set; } = null!;
    public DbSet<ErSourceSystemTag> SourceSystemTags { get; set; } = null!;
    public DbSet<ErFunction> Functions { get; set; } = null!;
    public DbSet<ErFunctionRule> FunctionRules { get; set; } = null!;
    public DbSet<ErFunctionMatcher> FunctionMatchers { get; set; } = null!;
    public DbSet<ErFunctionSourceOverride> FunctionSourceOverrides { get; set; } = null!;
    public DbSet<ErFunctionSourceOverrideRule> FunctionSourceOverrideRules { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema("EventReader");

        // ErConfig
        modelBuilder.Entity<ErConfig>(e =>
        {
            e.ToTable("Config");
            e.HasKey(x => x.ConfigKey);
            e.Property(x => x.ConfigKey).HasMaxLength(100);
            e.Property(x => x.ConfigValue).HasMaxLength(500).IsRequired();
        });

        // ErSourceSystem
        modelBuilder.Entity<ErSourceSystem>(e =>
        {
            e.ToTable("SourceSystem");
            e.HasKey(x => x.SourceSystemId);
            e.Property(x => x.SourceSystemId).HasMaxLength(100);
            e.Property(x => x.KafkaTopic).HasMaxLength(200).IsRequired();
            e.Property(x => x.ProviderName).HasMaxLength(200).IsRequired();
            e.Property(x => x.IsEnabled).HasDefaultValue(true);
        });

        // ErSourceSystemTag
        modelBuilder.Entity<ErSourceSystemTag>(e =>
        {
            e.ToTable("SourceSystemTag");
            e.HasKey(x => new { x.SourceSystemId, x.TagKey });
            e.Property(x => x.SourceSystemId).HasMaxLength(100);
            e.Property(x => x.TagKey).HasMaxLength(100);
            e.Property(x => x.TagValue).HasMaxLength(500).IsRequired().HasDefaultValue(string.Empty);
            e.HasOne(x => x.SourceSystem)
             .WithMany(x => x.Tags)
             .HasForeignKey(x => x.SourceSystemId)
             .OnDelete(DeleteBehavior.Cascade);
        });

        // ErFunction
        modelBuilder.Entity<ErFunction>(e =>
        {
            e.ToTable("Function");
            e.HasKey(x => x.FunctionId);
            e.Property(x => x.FunctionId).ValueGeneratedNever();
            e.Property(x => x.Name).HasMaxLength(200).IsRequired();
            e.Property(x => x.IsEnabled).HasDefaultValue(true);
            e.Property(x => x.ScriptName).HasMaxLength(200).IsRequired().HasDefaultValue(string.Empty);
            e.Property(x => x.OutputRouteName).HasMaxLength(200).IsRequired().HasDefaultValue("default");
            e.Property(x => x.OutputTopic).HasMaxLength(500).IsRequired().HasDefaultValue(string.Empty);
            e.Property(x => x.MaxProcessingAttempts).HasDefaultValue(3);
            e.Property(x => x.MaxOutputAttempts).HasDefaultValue(3);
        });

        // ErFunctionRule
        modelBuilder.Entity<ErFunctionRule>(e =>
        {
            e.ToTable("FunctionRule");
            e.HasKey(x => new { x.FunctionId, x.RuleId });
            e.Property(x => x.RuleName).HasMaxLength(200).IsRequired();
            e.HasOne(x => x.Function)
             .WithMany(x => x.Rules)
             .HasForeignKey(x => x.FunctionId)
             .OnDelete(DeleteBehavior.Cascade);
        });

        // ErFunctionMatcher
        modelBuilder.Entity<ErFunctionMatcher>(e =>
        {
            e.ToTable("FunctionMatcher");
            e.HasKey(x => x.FunctionMatcherId);
            e.Property(x => x.SourceSystemId).HasMaxLength(100);
            e.Property(x => x.Priority).HasDefaultValue(100);
            e.Property(x => x.Prefix).HasMaxLength(100).IsRequired();
            e.Property(x => x.RegexPattern).HasMaxLength(1000).IsRequired();
            e.HasOne(x => x.Function)
             .WithMany(x => x.Matchers)
             .HasForeignKey(x => x.FunctionId)
             .OnDelete(DeleteBehavior.Cascade);
        });

        // ErFunctionSourceOverride
        modelBuilder.Entity<ErFunctionSourceOverride>(e =>
        {
            e.ToTable("FunctionSourceOverride");
            e.HasKey(x => new { x.FunctionId, x.SourceSystemId });
            e.Property(x => x.SourceSystemId).HasMaxLength(100);
            e.Property(x => x.ScriptName).HasMaxLength(200).IsRequired().HasDefaultValue(string.Empty);
            e.Property(x => x.OutputRouteName).HasMaxLength(200).IsRequired().HasDefaultValue("default");
            e.Property(x => x.OutputTopic).HasMaxLength(500).IsRequired().HasDefaultValue(string.Empty);
            e.Property(x => x.MaxProcessingAttempts).HasDefaultValue(3);
            e.Property(x => x.MaxOutputAttempts).HasDefaultValue(3);
            e.HasOne(x => x.Function)
             .WithMany(x => x.SourceOverrides)
             .HasForeignKey(x => x.FunctionId)
             .OnDelete(DeleteBehavior.Cascade);
            e.HasOne(x => x.SourceSystem)
             .WithMany(x => x.FunctionOverrides)
             .HasForeignKey(x => x.SourceSystemId)
             .OnDelete(DeleteBehavior.Restrict);
        });

        // ErFunctionSourceOverrideRule
        modelBuilder.Entity<ErFunctionSourceOverrideRule>(e =>
        {
            e.ToTable("FunctionSourceOverrideRule");
            e.HasKey(x => new { x.FunctionId, x.SourceSystemId, x.RuleId });
            e.Property(x => x.SourceSystemId).HasMaxLength(100);
            e.Property(x => x.RuleName).HasMaxLength(200).IsRequired();
            e.HasOne(x => x.Override)
             .WithMany(x => x.Rules)
             .HasForeignKey(x => new { x.FunctionId, x.SourceSystemId })
             .OnDelete(DeleteBehavior.Cascade);
        });
    }
}
