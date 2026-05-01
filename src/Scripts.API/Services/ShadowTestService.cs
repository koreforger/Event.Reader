using System.Collections.Concurrent;
using System.Diagnostics;
using EventReader.Scripts.API.Models;
using EventReader.Scripts.API.Options;
using KoreForge.Jex;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace EventReader.Scripts.API.Services;

/// <summary>
/// Manages active shadow test sessions. Compares candidate scripts against the current live script
/// for incoming messages and produces diff results.
/// </summary>
public sealed class ShadowTestService
{
    private readonly ConcurrentDictionary<string, ShadowTestSession> _sessions = new();
    private readonly ConcurrentDictionary<long, string> _functionSessions = new(); // FunctionId → SessionId
    private readonly ScriptsApiOptions _options;
    private readonly IJexCompiler _jex;
    private readonly ILogger<ShadowTestService> _logger;

    public ShadowTestService(
        ScriptsApiOptions options,
        IJexCompiler jex,
        ILogger<ShadowTestService> logger)
    {
        _options = options;
        _jex = jex;
        _logger = logger;
    }

    /// <summary>Gets all active sessions.</summary>
    public IReadOnlyCollection<ShadowTestSession> ActiveSessions => _sessions.Values.ToList();

    /// <summary>Gets a session by ID, or null.</summary>
    public ShadowTestSession? GetSession(string sessionId)
    {
        return _sessions.TryGetValue(sessionId, out var session) ? session : null;
    }

    /// <summary>Gets the active session for a function, if any.</summary>
    public ShadowTestSession? GetSessionForFunction(long functionId)
    {
        if (_functionSessions.TryGetValue(functionId, out var sessionId))
            return GetSession(sessionId);
        return null;
    }

    /// <summary>
    /// Starts a new shadow test session. Compiles the candidate script and registers the session.
    /// </summary>
    public ShadowTestSession StartSession(
        long functionId,
        string functionName,
        string candidateContent,
        long? candidateScriptId,
        IJexProgram? currentProgram,
        int sampleSize,
        int? timeoutSeconds)
    {
        if (_sessions.Count >= _options.MaxShadowTestSessions)
            throw new InvalidOperationException(
                $"Maximum shadow test sessions ({_options.MaxShadowTestSessions}) reached.");

        if (_functionSessions.ContainsKey(functionId))
            throw new InvalidOperationException(
                $"A shadow test is already active for function {functionId}.");

        // Compile the candidate — throws JexCompileException on failure
        var candidateProgram = _jex.Compile(candidateContent);

        var timeout = timeoutSeconds.HasValue
            ? TimeSpan.FromSeconds(timeoutSeconds.Value)
            : _options.ShadowTestTimeout;

        var session = new ShadowTestSession
        {
            SessionId = Guid.NewGuid().ToString("N"),
            FunctionId = functionId,
            FunctionName = functionName,
            CurrentProgram = currentProgram,
            CandidateProgram = candidateProgram,
            CandidateContent = candidateContent,
            CandidateScriptId = candidateScriptId,
            SampleSize = sampleSize,
            Remaining = sampleSize,
            TimeoutAt = DateTime.UtcNow.Add(timeout),
            StartedAt = DateTime.UtcNow,
        };

        if (!_sessions.TryAdd(session.SessionId, session))
            throw new InvalidOperationException("Failed to register shadow test session.");

        _functionSessions.TryAdd(functionId, session.SessionId);

        _logger.LogInformation(
            "Shadow test session {SessionId} started for function {FunctionId} '{FunctionName}' — {SampleSize} samples, timeout {Timeout}s",
            session.SessionId, functionId, functionName, sampleSize, timeout.TotalSeconds);

        return session;
    }

    /// <summary>
    /// Executes both current and candidate programs against the input and produces a diff result.
    /// Returns null if no active session for the function.
    /// </summary>
    public ShadowTestResult? ExecuteComparison(long functionId, JObject input)
    {
        var session = GetSessionForFunction(functionId);
        if (session is null || session.Remaining <= 0 || DateTime.UtcNow >= session.TimeoutAt)
            return null;

        var messageIndex = session.SampleSize - session.Remaining;

        // Run current script
        Dictionary<string, object?> currentOutput;
        double currentMs;
        if (session.CurrentProgram is not null)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                var result = session.CurrentProgram.Execute(input);
                sw.Stop();
                currentMs = sw.Elapsed.TotalMilliseconds;
                currentOutput = JObjectToDict(result as JObject);
            }
            catch
            {
                sw.Stop();
                currentMs = sw.Elapsed.TotalMilliseconds;
                currentOutput = new Dictionary<string, object?>();
            }
        }
        else
        {
            currentOutput = new Dictionary<string, object?>();
            currentMs = 0;
        }

        // Run candidate script
        Dictionary<string, object?> candidateOutput;
        double candidateMs;
        {
            var sw = Stopwatch.StartNew();
            try
            {
                var result = session.CandidateProgram.Execute(input);
                sw.Stop();
                candidateMs = sw.Elapsed.TotalMilliseconds;
                candidateOutput = JObjectToDict(result as JObject);
            }
            catch
            {
                sw.Stop();
                candidateMs = sw.Elapsed.TotalMilliseconds;
                candidateOutput = new Dictionary<string, object?>();
            }
        }

        // Compute diffs
        var diffs = ComputeDiffs(currentOutput, candidateOutput);

        session.Remaining--;

        var inputSnippet = input.ToString(Newtonsoft.Json.Formatting.None);
        if (inputSnippet.Length > 500)
            inputSnippet = inputSnippet[..500] + "...";

        return new ShadowTestResult
        {
            SessionId = session.SessionId,
            MessageIndex = messageIndex,
            InputSnippet = inputSnippet,
            Timestamp = DateTime.UtcNow,
            CurrentOutput = currentOutput,
            CandidateOutput = candidateOutput,
            Diffs = diffs,
            CurrentTimeMs = currentMs,
            CandidateTimeMs = candidateMs,
        };
    }

    /// <summary>
    /// Stops and removes a session by ID. Returns the summary if the session existed.
    /// </summary>
    public ShadowTestSummary? StopSession(string sessionId, string reason = "cancelled")
    {
        if (!_sessions.TryRemove(sessionId, out var session))
            return null;

        _functionSessions.TryRemove(session.FunctionId, out _);

        var summary = new ShadowTestSummary
        {
            SessionId = sessionId,
            FunctionId = session.FunctionId,
            TotalProcessed = session.SampleSize - session.Remaining,
            MatchCount = 0, // calculated by the caller from accumulated results
            DiffCount = 0,
            AvgCurrentTimeMs = 0,
            AvgCandidateTimeMs = 0,
            CompletedAt = DateTime.UtcNow,
            Reason = reason,
        };

        _logger.LogInformation(
            "Shadow test session {SessionId} stopped: {Reason}, processed {TotalProcessed}/{SampleSize}",
            sessionId, reason, summary.TotalProcessed, session.SampleSize);

        return summary;
    }

    /// <summary>
    /// Checks if a session has completed (sample reached or timed out).
    /// </summary>
    public bool IsSessionComplete(string sessionId)
    {
        if (!_sessions.TryGetValue(sessionId, out var session))
            return true;
        return session.Remaining <= 0 || DateTime.UtcNow >= session.TimeoutAt;
    }

    private static Dictionary<string, object?> JObjectToDict(JObject? obj)
    {
        if (obj is null) return new Dictionary<string, object?>();

        var dict = new Dictionary<string, object?>();
        foreach (var prop in obj.Properties())
        {
            dict[prop.Name] = prop.Value.Type switch
            {
                JTokenType.Null => null,
                JTokenType.Integer => prop.Value.Value<long>(),
                JTokenType.Float => prop.Value.Value<double>(),
                JTokenType.Boolean => prop.Value.Value<bool>(),
                JTokenType.Date => prop.Value.Value<DateTime>(),
                _ => prop.Value.Value<string>(),
            };
        }
        return dict;
    }

    private static List<FieldDiff> ComputeDiffs(
        Dictionary<string, object?> current,
        Dictionary<string, object?> candidate)
    {
        var diffs = new List<FieldDiff>();
        var allKeys = current.Keys.Union(candidate.Keys).Distinct();

        foreach (var key in allKeys)
        {
            var hasCurrent = current.TryGetValue(key, out var currentVal);
            var hasCandidate = candidate.TryGetValue(key, out var candidateVal);

            if (hasCurrent && hasCandidate)
            {
                var status = Equals(currentVal, candidateVal) ? "matched" : "changed";
                if (status == "changed")
                {
                    diffs.Add(new FieldDiff
                    {
                        Field = key,
                        CurrentValue = currentVal,
                        CandidateValue = candidateVal,
                        Status = status,
                    });
                }
            }
            else if (hasCurrent && !hasCandidate)
            {
                diffs.Add(new FieldDiff
                {
                    Field = key,
                    CurrentValue = currentVal,
                    CandidateValue = null,
                    Status = "removed",
                });
            }
            else if (!hasCurrent && hasCandidate)
            {
                diffs.Add(new FieldDiff
                {
                    Field = key,
                    CurrentValue = null,
                    CandidateValue = candidateVal,
                    Status = "added",
                });
            }
        }

        return diffs;
    }
}
