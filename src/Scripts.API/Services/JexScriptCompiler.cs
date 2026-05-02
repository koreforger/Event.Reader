using KoreForge.Scripts.Interfaces;
using KoreForge.Scripts.Models;
using KoreForge.Jex;
using Microsoft.Extensions.Logging;

namespace EventReader.Scripts.API.Services;

/// <summary>
/// Bridges <see cref="IScriptCompiler"/> with the KoreForge.Jex engine.
/// Compiles JEX script content and returns diagnostics with line/column information.
/// </summary>
internal sealed class JexScriptCompiler : IScriptCompiler
{
    private readonly IJexCompiler _jex;
    private readonly ILogger<JexScriptCompiler> _logger;

    public JexScriptCompiler(IJexCompiler jex, ILogger<JexScriptCompiler> logger)
    {
        _jex = jex;
        _logger = logger;
    }

    public string Language => "jex";

    public Task<CompilationResult> CompileAsync(string content, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(content))
        {
            return Task.FromResult(new CompilationResult(
                false,
                new[] { new CompilationDiagnostic(DiagnosticSeverity.Error, "Script content is empty.", null, null) }));
        }

        try
        {
            _ = _jex.Compile(content);

            _logger.LogDebug("JEX compilation succeeded ({Length} chars)", content.Length);
            return Task.FromResult(new CompilationResult(true, Array.Empty<CompilationDiagnostic>()));
        }
        catch (JexCompileException ex)
        {
            int? line = ex.Span?.Start.Line;
            int? column = ex.Span?.Start.Column;

            _logger.LogWarning(ex, "JEX compilation failed at ({Line},{Column})", line, column);

            var diagnostic = new CompilationDiagnostic(
                DiagnosticSeverity.Error,
                ex.Message,
                line,
                column);

            return Task.FromResult(new CompilationResult(false, new[] { diagnostic }));
        }
    }
}
