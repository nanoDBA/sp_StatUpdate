SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/*

sp_StatUpdate_Diag - Diagnostic & Recommendation Engine

Copyright (c) 2026 Community Contribution
https://github.com/nanoDBA/sp_StatUpdate

Purpose:    Analyze CommandLog data from sp_StatUpdate runs to detect problems,
            identify suboptimal configurations, and recommend parameter changes.
            Supports obfuscated output mode for safe external sharing.

Based on:   sp_StatUpdate CommandLog format and Ola Hallengren's CommandLog table.

License:    MIT License

            Permission is hereby granted, free of charge, to any person obtaining a copy
            of this software and associated documentation files (the "Software"), to deal
            in the Software without restriction, including without limitation the rights
            to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
            copies of the Software, and to permit persons to whom the Software is
            furnished to do so, subject to the following conditions:

            The above copyright notice and this permission notice shall be included in all
            copies or substantial portions of the Software.

            THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
            IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
            FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
            AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
            LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
            OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
            SOFTWARE.

Version:    2026.03.23.1 (CalVer: YYYY.MM.DD; same-day patches append .1, .2, etc.)

History:    2026.03.23.1 - I10 RECOMMENDED_CONFIG: synthesized parameter set based on diagnostic
                           findings and parameter history.  Preserves safeguards from prior runs,
                           adjusts flagged parameters, suggests untracked safeguards (@MinTempdbFreeMB,
                           @MaxConsecutiveFailures, @MaxAGRedoQueueMB).
            2026.03.23   - NULL RunLabel fix for legacy CommandLog entries (ISNULL fallback).
                         - INSERT...EXEC safety: auto @SkipHistory=1, DB_ID guard, TRY/CATCH on map write.
                         - @ObfuscationMapTable dedup (NOT EXISTS prevents duplicate rows on repeated runs).
                         - Arithmetic overflow fix: nvarchar(10) widened to nvarchar(20) for decimal/ratio conversions.
                         - Unicode arrow (U+2192) replaced with ASCII '->'.
                         - Enhanced @Debug=1: timing at every stage, effective parameter echo, temp table row
                           counts and samples (#qs_efficacy, #executive_dashboard, #recommendations severity
                           breakdown), CommandLog date range, legacy label count, execution summary.
                         - Enhanced @Help=1: grading scale docs, prerequisites/limitations, grade override
                           examples, version history (9 result sets, was 5).
                         - Em dash removal (~60 occurrences) for PowerShell 5.1 compatibility.
            2026.03.20   - 26-issue bulk resolution across 5 phases:
                         Phase 1: Cache watermark sentinel fix (#312), C4 parallelism-aware (#306).
                         Phase 2: ObjectId/StatsId extraction (#268), RS 2 Summary (#283),
                           RS 7 StatsInParallel (#287), RS 12 WorkloadRankPct/CumulativeCpuPct (#262),
                           RS 10 Top5ConcentrationPct (#269), cursor-to-set-based parsing (#302).
                         Phase 3: #workload_impact staging table, ImpactScore in RS 5/7 (#255),
                           WorkloadImpactPct in RS 5 (#261), IsVolatile flag (#259),
                           W7 HIGH_IMPACT_STATS_DEPRIORITIZED (#263), I9 WORKLOAD_CONCENTRATION (#264),
                           C3 workload impact enhancement (#265).
                         Phase 4: W2 CRITICAL escalation (#266), W5 partial read-only (#274),
                           W5/I6 capture mode guidance (#278), I8 improve-then-regress (#276),
                           W6 memory pressure guidance (#285), dashboard non-QS scoring (#267),
                           ExpertMode=0 workload context (#270).
                         Phase 5: AG secondary replica detection (#296), RS 3 ReplicaRole column,
                           15 new tests (162 -> 177).
            2026.03.13   - Fix RS 12 WorkloadRank always=1 in SingleResultSet mode (#280).
                         - Fix RS 11 DeltaVsPrior missing minutes-to-critical delta in SingleResultSet mode (#282).
            2026.03.11   - Fix empty-data RS 3 schema mismatch (#304): 7 columns -> 17 columns
                         matching production RS 3 (Run Health Summary).
            2026.03.10.2 - RS 13 forced plan awareness (#292): ForcedPlanCount column, PlanTrend
                         distinguishes 'MORE PLANS (forced plan at risk)' from generic proliferation.
                         I8 recommendation warns when degrading stats have forced plans.
                         Cross-database sys.query_store_plan lookup (graceful on QS-disabled DBs).
            2026.03.10.1 - @GradeOverrides and @GradeWeights parameters for Executive Dashboard.
                         Force grades (RELIABILITY=A), exclude categories (SPEED=IGNORE),
                         or change weights (COMPLETION=50, auto-normalized to 100%).
                         I6 QS_EFFICACY: graceful message when QS runs exist but no CPU data
                         (was emitting question marks).
            2026.03.10   - DECLARE-in-loop fix for @map_table_msg variable.
                         RAISERROR decode query output when @ObfuscationMapTable is used.
            2026.03.09.3 - W6 EXCESSIVE_OVERHEAD diagnostic check: flags runs where discovery/environment
                         overhead exceeds 40% of wall-clock time.
                         PAGE compression on StatUpdateDiagHistory PK and IX.
            2026.03.09.2 - I8 silent failure: inserts INFO recommendation when no QS CPU data exists (#239).
                         @SingleResultSet=1 auto-promotes @ExpertMode=1 for stable ResultSetID contract (#236).
            2026.03.09.1 - RunLabel dedup prevents PK violation on duplicate START entries (#216).
                         Watermark gap detection resets when CommandLog archived (#232).
                         I5 VERSION_HISTORY implemented (was documented but missing).
                         I8 QS_PERFORMANCE_TREND: per-stat per-execution CPU trend.
                         StatUpdateDiagCache persistent table avoids XML re-parse.
                         New @Help note for automation / @SingleResultSet.
            2026.03.08.1 - Executive Dashboard (RS 1): letter grades A-F, health score 0-100,
                         5 categories (Overall, Completion, Reliability, Speed, Workload Focus).
                         @ExpertMode parameter: 0 = management view (2 RS), 1 = DBA deep-dive (13 RS).
                         Persistent history table (dbo.StatUpdateDiagHistory) with watermark-based
                         incremental inserts. @SkipHistory parameter to opt out.
                         QS Performance Correlation (I8 + RS 13): per-stat CPU trend detail.
                         QS Efficacy Trending (I6/I7 + RS 10/11/12): weekly aggregates,
                         per-run detail, high-CPU stat positions.
                         RS renumbered: RS 1 = Dashboard, RS 2 = Recommendations (always),
                         RS 3-13 = ExpertMode=1 only.
            2026.03.06   - Security/correctness: SQL injection fix in @CommandLogDatabase,
                         missing AG keyword in @Help, parameter validation warnings.
                         12 bug fixes from SME review (BUG-01 through BUG-12).
                         @ObfuscationSeed and @ObfuscationMapTable parameters for
                         deterministic, reproducible obfuscation across runs.
                         @Help enhanced with valid_inputs, examples, operational notes.
            2026.03.04.1 - Version format adopted CalVer (YYYY.MM.DD).
                         @SingleResultSet parameter: wraps all output into one table with
                         stable ResultSetID column for INSERT...EXEC automation.
                         Fix: empty-data path respects @SingleResultSet + @Obfuscate.
            2026.02.12   - Initial release. 8 diagnostic checks (C1-C4, W1-W5, I1-I4),
                         obfuscation mode, @Help, @Debug.
                         9 result sets. 53 tests.

Requires:   - dbo.CommandLog table (Ola Hallengren's SQL Server Maintenance Solution)
            - sp_StatUpdate entries in CommandLog (SP_STATUPDATE_START/END + UPDATE_STATISTICS)
            - SQL Server 2017+ (STRING_AGG; STRING_SPLIT is 2016+ but STRING_AGG requires 2017+)

Key Features:
    - Executive Dashboard with A-F grades and health scores
    - 16 diagnostic checks across CRITICAL/WARNING/INFO severities
    - Obfuscation mode for safe external sharing (HASHBYTES-based)
    - Persistent history for trend tracking (dbo.StatUpdateDiagHistory)
    - Persistent stat cache for fast re-runs (dbo.StatUpdateDiagCache)
    - Query Store efficacy analysis (proves QS prioritization value)
    - @ExpertMode: management view (2 RS) vs DBA deep-dive (13 RS)
    - @SingleResultSet for stable automation interface

Usage:      -- Quick health check (management dashboard):
            EXECUTE dbo.sp_StatUpdate_Diag;

            -- Full DBA deep-dive:
            EXECUTE dbo.sp_StatUpdate_Diag @ExpertMode = 1;

            -- Obfuscated for sharing externally:
            EXECUTE dbo.sp_StatUpdate_Diag @Obfuscate = 1;

            -- Custom analysis window:
            EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 90, @Debug = 1;

            -- CommandLog in a different database:
            EXECUTE dbo.sp_StatUpdate_Diag @CommandLogDatabase = N'DBATools';
*/

IF OBJECT_ID(N'dbo.sp_StatUpdate_Diag', N'P') IS NULL
BEGIN
    EXECUTE (N'CREATE PROCEDURE dbo.sp_StatUpdate_Diag AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    dbo.sp_StatUpdate_Diag
(
    @DaysBack integer = 30,                     /* history window in days */
    @CommandLogDatabase sysname = NULL,          /* NULL = current DB */
    @Obfuscate bit = 0,                         /* 0 = real names, 1 = hashed names */
    @ObfuscationSeed nvarchar(128) = NULL,       /* salt for HASHBYTES -- makes tokens unpredictable without seed */
    @ObfuscationMapTable sysname = NULL,          /* persist obfuscation map to this table (auto-creates if missing) */
    @LongRunningMinutes integer = 10,            /* threshold for "long-running stat" detection */
    @FailureThreshold integer = 3,               /* same stat failing N+ times = CRITICAL */
    @TimeLimitExhaustionPct integer = 80,         /* warn if >X% of runs hit time limit */
    @ThroughputWindowDays integer = 7,            /* window size for throughput trend comparison */
    @TopN integer = 20,                           /* top N items in detail result sets */
    @EfficacyDaysBack integer = NULL,              /* QS efficacy trending window (NULL = @DaysBack) */
    @EfficacyDetailDays integer = NULL,            /* QS efficacy close-up window (NULL = 14 or @EfficacyDaysBack) */
    @ExpertMode bit = 0,                           /* 0 = Executive Dashboard + Recommendations (management view), 1 = all result sets (DBA view) */
    @SkipHistory bit = 0,                          /* 1 = skip reading/writing dbo.StatUpdateDiagHistory (for testing or transient installs) */
    @GradeOverrides nvarchar(500) = NULL,          /* force grades or IGNORE categories: 'RELIABILITY=A, SPEED=IGNORE' */
    @GradeWeights nvarchar(500) = NULL,            /* custom category weights (auto-normalized): 'COMPLETION=40, WORKLOAD=40' */
    @Help bit = 0,
    @Debug bit = 0,
    @SingleResultSet bit = 0,       /* 0 = default multi-result-set, 1 = single result set with ResultSetID/RowData */
    @Version varchar(20) = NULL OUTPUT,
    @VersionDate datetime = NULL OUTPUT
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    /*
    ============================================================================
    VERSION AND CONSTANTS
    ============================================================================
    */
    DECLARE
        @procedure_version varchar(20) = '2026.03.23.1',
        @procedure_version_date datetime = '20260323';

    SET @Version = @procedure_version;
    SET @VersionDate = @procedure_version_date;

    /* Debug timing baseline */
    DECLARE @debug_timer datetime2(3) = SYSDATETIME();
    DECLARE @debug_elapsed_ms integer;

    /*
    ============================================================================
    HELP
    ============================================================================
    */
    IF @Help = 1
    BEGIN
        /* Result set 1: Parameters */
        SELECT
            help_topic = N'Parameters',
            parameter_name = parameter_name,
            data_type = data_type,
            description = description,
            valid_inputs = valid_inputs,
            defaults = default_value
        FROM
        (
            VALUES
                (N'@DaysBack',                 N'integer',  N'History window in days -- how far back to scan CommandLog',
                    N'1-3650', N'30'),
                (N'@CommandLogDatabase',        N'sysname',  N'Database containing dbo.CommandLog table. NULL = current database context.',
                    N'NULL, database name (e.g., DBATools, master)', N'NULL (current database)'),
                (N'@Obfuscate',                N'bit',      N'Hash database/schema/table/stat names for safe external sharing. Uses HASHBYTES MD5. Obfuscation map returned as result set 8 (multi-result-set mode only).',
                    N'0, 1', N'0'),
                (N'@ObfuscationSeed',          N'nvarchar(128)', N'Salt prepended to names before hashing. Makes tokens unpredictable without the seed but deterministic across runs/servers with the same seed. NULL = unsalted (backward compatible).',
                    N'NULL, any string up to 128 chars', N'NULL'),
                (N'@ObfuscationMapTable',      N'sysname',  N'Persist obfuscation map to this table (auto-creates if missing, merges new entries on subsequent runs). Enables saving the map on prod while exporting only obfuscated results. Requires @Obfuscate=1.',
                    N'NULL, table name (e.g., dbo.DiagMap, tempdb.dbo.diag_map)', N'NULL'),
                (N'@LongRunningMinutes',        N'integer',  N'Stats taking longer than this (in minutes) are flagged in W2 check and Long-Running Statistics result set',
                    N'1-N minutes', N'10'),
                (N'@FailureThreshold',          N'integer',  N'Same statistic failing this many times across runs triggers C2 CRITICAL finding',
                    N'1-N', N'3'),
                (N'@TimeLimitExhaustionPct',    N'integer',  N'If more than this percentage of runs hit TIME_LIMIT stop reason, triggers C3 CRITICAL finding',
                    N'1-100', N'80'),
                (N'@ThroughputWindowDays',      N'integer',  N'Compare recent N days throughput against prior window for C4 degrading throughput detection',
                    N'1-@DaysBack', N'7'),
                (N'@TopN',                     N'integer',  N'Maximum rows returned in detail result sets (Top Tables, Failing Stats, Long-Running Stats)',
                    N'1-1000', N'20'),
                (N'@EfficacyDaysBack',         N'integer',  N'Broad window for QS efficacy trending (weekly aggregates). NULL inherits from @DaysBack.',
                    N'NULL, 1-3650', N'NULL (= @DaysBack)'),
                (N'@EfficacyDetailDays',       N'integer',  N'Close-up window for run-over-run efficacy detail. NULL defaults to 14 or @EfficacyDaysBack if smaller.',
                    N'NULL, 1-@EfficacyDaysBack', N'NULL (= min(14, @EfficacyDaysBack))'),
                (N'@ExpertMode',              N'bit',      N'0 = Executive Dashboard + Recommendations only (management-friendly, show this to leadership). 1 = All result sets including technical detail (DBA deep-dive). Inspired by sp_Blitz @ExpertMode.',
                    N'0, 1', N'0'),
                (N'@SkipHistory',             N'bit',      N'1 = Skip reading/writing dbo.StatUpdateDiagHistory persistent table. Useful for transient installs, testing, or when you do not want permanent tables created.',
                    N'0, 1', N'0'),
                (N'@GradeOverrides',         N'nvarchar(500)', N'Force dashboard grades or ignore categories. Comma-separated CATEGORY=VALUE pairs. Categories: COMPLETION, RELIABILITY, SPEED, WORKLOAD. Values: A/B/C/D/F (force grade) or IGNORE (exclude from OVERALL). Example: ''RELIABILITY=A, SPEED=IGNORE'' -- forces Reliability to A, excludes Speed from the overall score.',
                    N'NULL, comma-separated pairs (e.g., ''RELIABILITY=A'', ''SPEED=IGNORE, WORKLOAD=B'')', N'NULL'),
                (N'@GradeWeights',           N'nvarchar(500)', N'Custom category weights for OVERALL score. Comma-separated CATEGORY=WEIGHT pairs. Integers, auto-normalized to sum to 100%. Omitted categories keep default weight. Weight of 0 = same as IGNORE. Defaults: COMPLETION=30, RELIABILITY=25, SPEED=20, WORKLOAD=25.',
                    N'NULL, comma-separated pairs (e.g., ''COMPLETION=50'', ''COMPLETION=40, WORKLOAD=40, SPEED=20'')', N'NULL'),
                (N'@Help',                     N'bit',      N'Show this help output and return immediately',
                    N'0, 1', N'0'),
                (N'@Debug',                    N'bit',      N'Verbose diagnostic output -- shows intermediate temp table counts and timing',
                    N'0, 1', N'0'),
                (N'@SingleResultSet',          N'bit',      N'Collapse all result sets into one with columns (ResultSetID, ResultSetName, RowNum, RowData). RowData is JSON. Enables INSERT...EXEC capture in automation. Auto-promotes @ExpertMode=1 so ResultSetIDs 1-13 are always present.',
                    N'0, 1', N'0'),
                (N'@Version',                  N'varchar(20)', N'OUTPUT: returns procedure version string (e.g., 2026.03.04)',
                    N'OUTPUT', N'OUTPUT'),
                (N'@VersionDate',              N'datetime', N'OUTPUT: returns procedure version date',
                    N'OUTPUT', N'OUTPUT')
        ) AS v (parameter_name, data_type, description, valid_inputs, default_value);

        /* Result set 2: Diagnostic checks */
        SELECT
            help_topic = N'Diagnostic Checks',
            check_id = check_id,
            severity = severity,
            category = category,
            description = description
        FROM
        (
            VALUES
                (N'C1', N'CRITICAL', N'KILLED_RUNS',           N'START without matching END (orphaned/killed runs)'),
                (N'C2', N'CRITICAL', N'REPEATED_FAILURES',     N'Same statistic failing consistently across runs'),
                (N'C3', N'CRITICAL', N'TIME_LIMIT_EXHAUSTION', N'Majority of runs hitting TIME_LIMIT with stats remaining'),
                (N'C4', N'CRITICAL', N'DEGRADING_THROUGHPUT',  N'Average seconds-per-stat increasing over time'),
                (N'W1', N'WARNING',  N'SUBOPTIMAL_PARAMS',     N'Parameter choices that may reduce effectiveness'),
                (N'W2', N'WARNING',  N'LONG_RUNNING_STATS',    N'Individual stats consistently exceeding threshold'),
                (N'W3', N'WARNING',  N'STALE_BACKLOG',         N'Persistent backlog of unprocessed qualifying stats'),
                (N'W4', N'WARNING',  N'OVERLAPPING_RUNS',      N'Multiple runs active simultaneously'),
                (N'W5', N'WARNING',  N'QS_NOT_EFFECTIVE',      N'Query Store priority enabled but no QS data captured'),
                (N'W6', N'WARNING',  N'EXCESSIVE_OVERHEAD',    N'Discovery/environment checks consuming disproportionate time vs actual UPDATE STATISTICS'),
                (N'I1', N'INFO',     N'RUN_HEALTH',            N'Completion rate, duration trend, StopReason distribution'),
                (N'I2', N'INFO',     N'PARAMETER_HISTORY',     N'How parameters changed across runs'),
                (N'I3', N'INFO',     N'TOP_TABLES',            N'Tables consuming the most maintenance time'),
                (N'I4', N'INFO',     N'UNUSED_FEATURES',       N'Available features not being used'),
                (N'I5', N'INFO/WARNING', N'VERSION_HISTORY',    N'sp_StatUpdate versions used across analysis window. WARNING if multiple versions detected (version skew).'),
                (N'I6', N'INFO',     N'QS_EFFICACY',           N'Query Store prioritization effectiveness -- what % of high-workload stats get serviced early'),
                (N'I7', N'INFO',     N'QS_INFLECTION',         N'Before/after comparison when sort order changed to Query Store-based prioritization'),
                (N'I8', N'INFO',     N'QS_PERFORMANCE_TREND',  N'Per-stat per-execution query CPU trend -- are queries getting faster after stat updates? Detects forced plans at risk (#292).'),
                (N'I10', N'INFO',    N'RECOMMENDED_CONFIG',    N'Synthesized parameter set balancing immediate fixes, long-term safeguards, and historical parameter usage.  Based on diagnostic findings and parameter change history.')
        ) AS v (check_id, severity, category, description);

        /* Result set 3: Result set order */
        SELECT
            help_topic = N'Result Sets',
            result_set_number = rs_num,
            name = rs_name,
            description = rs_desc
        FROM
        (
            VALUES
                (1, N'Executive Dashboard',        N'ALWAYS returned first. Letter-graded categories (A-F) with plain English headlines for management. One row per category: Overall, Completion, Reliability, Speed, Workload Coverage. Inspired by sp_Blitz priority system.'),
                (2, N'Recommendations',            N'Severity-categorized findings with parameter suggestions. Returned with @ExpertMode=0 (management view).'),
                (3, N'Run Health Summary',          N'Aggregate metrics across all runs. Requires @ExpertMode=1.'),
                (4, N'Run Detail',                  N'Per-run metrics. Requires @ExpertMode=1.'),
                (5, N'Top Tables',                  N'Top N tables by total update duration. Requires @ExpertMode=1.'),
                (6, N'Failing Statistics',           N'Stats with errors, grouped. Requires @ExpertMode=1.'),
                (7, N'Long-Running Statistics',      N'Stats exceeding threshold. Requires @ExpertMode=1.'),
                (8, N'Parameter Change History',     N'Parameter values across runs. Requires @ExpertMode=1.'),
                (9, N'Obfuscation Map (conditional)', N'Only when @Obfuscate=1. Requires @ExpertMode=1.'),
                (10, N'Efficacy Trend (Weekly)',       N'Weekly QS efficacy metrics: high-workload coverage, completion %, throughput trend. Requires @ExpertMode=1.'),
                (11, N'Efficacy Detail (Per-Run)',    N'Per-run QS efficacy for close-up window (@EfficacyDetailDays). Requires @ExpertMode=1.'),
                (12, N'High-CPU Stat Positions',      N'Top-workload stats from most recent run with their processing positions. Requires @ExpertMode=1.'),
                (13, N'QS Performance Correlation',   N'Per-stat Query Store CPU trend across runs. Shows whether queries get faster after stat updates. Leadership-friendly: "Are queries actually improving?" Requires @ExpertMode=1.'),
                (0, N'Unified Result Set',            N'When @SingleResultSet=1: one result set with ResultSetID, ResultSetName, RowNum, RowData (JSON). Replaces all result sets.')
        ) AS v (rs_num, rs_name, rs_desc);

        /* Result set 4: Examples */
        SELECT
            help_topic = N'Examples',
            example_name = example_name,
            example_description = example_description,
            example_code = example_code
        FROM
        (
            VALUES
                (
                    N'Quick Health Check',
                    N'Run with defaults against current database CommandLog',
                    N'EXECUTE dbo.sp_StatUpdate_Diag;'
                ),
                (
                    N'Obfuscated for Sharing',
                    N'Hash all object names for safe sharing with consultants or support',
                    N'EXECUTE dbo.sp_StatUpdate_Diag @Obfuscate = 1;'
                ),
                (
                    N'Extended History',
                    N'Analyze 90 days of history with debug output',
                    N'EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 90, @Debug = 1;'
                ),
                (
                    N'Remote CommandLog',
                    N'CommandLog lives in a central DBA database',
                    N'EXECUTE dbo.sp_StatUpdate_Diag @CommandLogDatabase = N''DBATools'';'
                ),
                (
                    N'Strict Failure Detection',
                    N'Flag stats failing 2+ times, long-running at 5 min',
                    N'EXECUTE dbo.sp_StatUpdate_Diag @FailureThreshold = 2, @LongRunningMinutes = 5;'
                ),
                (
                    N'Automation Capture',
                    N'Single result set for INSERT...EXEC or PowerShell capture',
                    N'EXECUTE dbo.sp_StatUpdate_Diag @SingleResultSet = 1;'
                ),
                (
                    N'Secure Obfuscated Export',
                    N'Save map on prod, export only obfuscated results for external analysis',
                    N'EXECUTE dbo.sp_StatUpdate_Diag @Obfuscate = 1, @SingleResultSet = 1, @ObfuscationSeed = N''MySecretSeed'', @ObfuscationMapTable = N''dbo.DiagObfuscationMap'';'
                ),
                (
                    N'QS Efficacy Report',
                    N'Show 100-day QS prioritization effectiveness with 14-day close-up',
                    N'EXECUTE dbo.sp_StatUpdate_Diag @EfficacyDaysBack = 100, @EfficacyDetailDays = 14;'
                ),
                (
                    N'Multi-Server (PowerShell)',
                    N'Use Invoke-StatUpdateDiag.ps1 for cross-server analysis',
                    N'.\Invoke-StatUpdateDiag.ps1 -Servers "Server1","Server2" -OutputFormat Markdown -OutputPath C:\Reports'
                )
        ) AS example_data (example_name, example_description, example_code);

        /* Result set 5: Operational Notes */
        SELECT
            help_topic = N'Operational Notes',
            topic = topic,
            detail = detail
        FROM
        (
            VALUES
                (N'Obfuscation',
                 N'@Obfuscate=1 replaces names with MD5 hashes (e.g., DB_a1b2c3, TBL_d4e5f6). Prefixes preserved for readability. Map is result set 8 in multi-result-set mode (excluded from @SingleResultSet=1 to prevent leaking real names). Use @ObfuscationMapTable to persist the map on prod. Use @ObfuscationSeed to salt hashes -- makes tokens stable across servers with the same seed but unpredictable without it.'),
                (N'Killed Run Detection',
                 N'Two detection methods: (1) SP_STATUPDATE_START without matching SP_STATUPDATE_END = orphaned run, (2) SP_STATUPDATE_END with StopReason=KILLED = cleaned up by @CleanupOrphanedRuns. Both trigger C1 CRITICAL.'),
                (N'Throughput Trend (C4)',
                 N'Compares average seconds-per-stat in the recent @ThroughputWindowDays against the prior window of equal length. If recent average is >50% worse, triggers C4 CRITICAL. Data-dependent -- requires sufficient runs in both windows.'),
                (N'Overlapping Runs (W4)',
                 N'Detects concurrent sp_StatUpdate executions by checking for overlapping StartTime/EndTime ranges. Excludes killed runs to prevent false positives from orphan-cleanup END records.'),
                (N'SingleResultSet Mode',
                 N'@SingleResultSet=1 wraps all 8 result sets into one table with columns: ResultSetID (int), ResultSetName (nvarchar), RowNum (int), RowData (nvarchar(max) as JSON). Use OPENJSON(RowData) to parse. Enables INSERT...EXEC patterns that fail with multiple result sets.'),
                (N'CommandLog Requirements',
                 N'Requires Ola Hallengren''s dbo.CommandLog table with sp_StatUpdate entries (CommandType IN SP_STATUPDATE_START, SP_STATUPDATE_END, UPDATE_STATISTICS). No data = no diagnostics (graceful empty result sets). @CommandLogDatabase lets you point to a central logging database.'),
                (N'PowerShell Wrapper',
                 N'Invoke-StatUpdateDiag.ps1 runs sp_StatUpdate_Diag across multiple servers in parallel, detects version skew and parameter inconsistencies, and generates Markdown/HTML/JSON reports. Use -Obfuscate for safe sharing.'),
                (N'Automation / Stable Result Sets',
                 N'The number of result sets varies by @ExpertMode (2 vs 12-13) and @Obfuscate (shifts RS9). For automation, use @SingleResultSet=1 -- it wraps all output into one table with a stable ResultSetID column (values 1-13). This makes INSERT...EXEC reliable regardless of parameter combinations.')
        ) AS notes (topic, detail);

        /* Result set 6: Grading Scale */
        SELECT
            help_topic = N'Grading Scale',
            grade = grade,
            score_range = score_range,
            meaning = meaning
        FROM
        (
            VALUES
                (N'A', N'90-100', N'Excellent -- statistics maintenance is healthy and effective'),
                (N'B', N'75-89',  N'Good -- minor improvements possible but no urgent issues'),
                (N'C', N'60-74',  N'Fair -- noticeable gaps or inefficiencies that deserve attention'),
                (N'D', N'40-59',  N'Poor -- significant problems impacting maintenance quality'),
                (N'F', N'0-39',   N'Failing -- critical issues requiring immediate action')
        ) AS v (grade, score_range, meaning);

        SELECT
            help_topic = N'Grading Scale',
            category = category,
            default_weight = default_weight,
            what_it_measures = what_it_measures
        FROM
        (
            VALUES
                (N'COMPLETION',  N'30%', N'What percentage of qualifying stats get updated each run'),
                (N'RELIABILITY', N'25%', N'Absence of killed runs, failures, and orphaned entries'),
                (N'SPEED',       N'20%', N'Average seconds per stat update -- are updates fast enough'),
                (N'WORKLOAD',    N'25%', N'Are high-CPU stats prioritized early (requires @QueryStorePriority=Y)')
        ) AS v (category, default_weight, what_it_measures);

        /* Result set 7: Prerequisites and Known Limitations */
        SELECT
            help_topic = N'Prerequisites and Limitations',
            topic = topic,
            detail = detail
        FROM
        (
            VALUES
                (N'SQL Server Version',
                 N'Requires SQL Server 2016+ (STRING_SPLIT dependency).  Best on 2017+ for STRING_AGG.'),
                (N'CommandLog Table',
                 N'Requires dbo.CommandLog from Ola Hallengren''s Maintenance Solution.  sp_StatUpdate writes SP_STATUPDATE_START, SP_STATUPDATE_END, and UPDATE_STATISTICS entries.  No CommandLog = no diagnostics.'),
                (N'sp_StatUpdate Version',
                 N'Full feature support requires sp_StatUpdate v2.16+ (ProcessingPosition in ExtendedInfo for RS 12).  QS efficacy requires @QueryStorePriority=Y runs.  Legacy runs (pre-RunLabel) are supported with synthetic labels.'),
                (N'INSERT...EXEC Limitation',
                 N'@SingleResultSet=1 auto-enables @SkipHistory=1 because SQL Server''s INSERT...EXEC creates an implicit transaction that prevents persistent table writes.  @ObfuscationMapTable writes are attempted but may silently fail in this context.'),
                (N'Obfuscation Map Security',
                 N'@SingleResultSet=1 excludes the obfuscation map from RowData to prevent leaking real names in the unified output.  Use @ObfuscationMapTable to persist the map separately.'),
                (N'WORKLOAD Grade Without QS',
                 N'If no runs use @QueryStorePriority=Y, the WORKLOAD category scores a neutral 50/100 (grade C).  Enable QS prioritization for meaningful workload grades.')
        ) AS v (topic, detail);

        /* Result set 8: Additional Examples */
        SELECT
            help_topic = N'Grade Override Examples',
            example_name = example_name,
            example_description = example_description,
            example_code = example_code
        FROM
        (
            VALUES
                (
                    N'Override Reliability Grade',
                    N'Force Reliability to A (you know the killed runs are from planned maintenance)',
                    N'EXECUTE dbo.sp_StatUpdate_Diag @GradeOverrides = N''RELIABILITY=A'';'
                ),
                (
                    N'Ignore Workload (No QS)',
                    N'Exclude Workload from Overall score when QS is not used',
                    N'EXECUTE dbo.sp_StatUpdate_Diag @GradeOverrides = N''WORKLOAD=IGNORE'';'
                ),
                (
                    N'Custom Weights',
                    N'Weight Completion and Reliability heavily, reduce Speed importance',
                    N'EXECUTE dbo.sp_StatUpdate_Diag @GradeWeights = N''COMPLETION=40, RELIABILITY=35, SPEED=5, WORKLOAD=20'';'
                ),
                (
                    N'Combined Override + Weights',
                    N'Force Reliability=A and reweight remaining categories',
                    N'EXECUTE dbo.sp_StatUpdate_Diag @GradeOverrides = N''RELIABILITY=A'', @GradeWeights = N''COMPLETION=50, SPEED=25, WORKLOAD=25'';'
                )
        ) AS v (example_name, example_description, example_code);

        /* Result set 9: Version History */
        SELECT
            help_topic = N'Version History',
            version_date = version_date,
            changes = changes
        FROM
        (
            VALUES
                (N'2026.03.23', N'NULL RunLabel fix, INSERT...EXEC safety, ObfuscationMapTable dedup, nvarchar overflow fixes, enhanced debug output, improved @Help'),
                (N'2026.03.20', N'26-issue bulk resolution: workload impact, check improvements, AG detection, 15 new tests'),
                (N'2026.03.13', N'RS 12 WorkloadRank fix, RS 11 DeltaVsPrior fix in SingleResultSet mode'),
                (N'2026.03.11', N'Empty-data RS 3 schema mismatch fix'),
                (N'2026.03.09', N'RunLabel dedup, QS efficacy, executive dashboard, persistent history')
        ) AS v (version_date, changes);

        RETURN;
    END;

    /*
    ============================================================================
    RUNTIME VERSION GUARD (BUG-02: STRING_AGG requires SQL Server 2017+)
    ============================================================================
    */
    IF CAST(SERVERPROPERTY('ProductMajorVersion') AS integer) < 14
    BEGIN
        RAISERROR(N'sp_StatUpdate_Diag requires SQL Server 2017 or later (STRING_AGG).', 16, 1);
        RETURN;
    END;

    /* #296: AG secondary replica detection -- warn that CommandLog may reflect primary-side maintenance only */
    DECLARE @ag_replica_role nvarchar(20) = NULL;
    BEGIN TRY
        SELECT TOP (1)
            @ag_replica_role = CASE ars.role
                WHEN 1 THEN N'PRIMARY'
                WHEN 2 THEN N'SECONDARY'
                ELSE N'UNKNOWN'
            END
        FROM sys.dm_hadr_availability_replica_states AS ars
        WHERE ars.is_local = 1
        ORDER BY ars.role;

        IF @ag_replica_role = N'SECONDARY'
            RAISERROR(N'WARNING: Running on AG secondary replica -- CommandLog data may reflect primary-side maintenance only.', 10, 1) WITH NOWAIT;
    END TRY
    BEGIN CATCH
        /* Non-AG instances don''t have the DMV -- silently ignore */
        SET @ag_replica_role = NULL;
    END CATCH;

    /*
    ============================================================================
    PARAMETER VALIDATION
    ============================================================================
    */
    DECLARE @errors nvarchar(max) = N'';

    IF @DaysBack < 1 OR @DaysBack > 3650
        SET @errors = @errors + N'@DaysBack must be between 1 and 3650. ';

    IF @LongRunningMinutes < 1
        SET @errors = @errors + N'@LongRunningMinutes must be >= 1. ';

    IF @FailureThreshold < 1
        SET @errors = @errors + N'@FailureThreshold must be >= 1. ';

    IF @TimeLimitExhaustionPct < 1 OR @TimeLimitExhaustionPct > 100
        SET @errors = @errors + N'@TimeLimitExhaustionPct must be between 1 and 100. ';

    IF @ThroughputWindowDays < 1 OR @ThroughputWindowDays > @DaysBack
        SET @errors = @errors + N'@ThroughputWindowDays must be between 1 and @DaysBack. ';

    IF @TopN < 1 OR @TopN > 1000
        SET @errors = @errors + N'@TopN must be between 1 and 1000. ';

    /* Default @EfficacyDaysBack from @DaysBack; @EfficacyDetailDays from min(14, efficacy window) */
    SET @EfficacyDaysBack = ISNULL(@EfficacyDaysBack, @DaysBack);
    SET @EfficacyDetailDays = ISNULL(@EfficacyDetailDays, CASE WHEN @EfficacyDaysBack < 14 THEN @EfficacyDaysBack ELSE 14 END);

    IF @EfficacyDaysBack < 1 OR @EfficacyDaysBack > 3650
        SET @errors = @errors + N'@EfficacyDaysBack must be between 1 and 3650. ';

    IF @EfficacyDetailDays < 1 OR @EfficacyDetailDays > @EfficacyDaysBack
        SET @errors = @errors + N'@EfficacyDetailDays must be between 1 and @EfficacyDaysBack. ';

    IF @Obfuscate = 0 AND @ObfuscationMapTable IS NOT NULL
        RAISERROR(N'WARNING: @ObfuscationMapTable is ignored when @Obfuscate = 0.', 10, 1) WITH NOWAIT;

    IF @Obfuscate = 0 AND @ObfuscationSeed IS NOT NULL
        RAISERROR(N'WARNING: @ObfuscationSeed is ignored when @Obfuscate = 0.', 10, 1) WITH NOWAIT;

    /* ---- Grade Override / Weight parsing ---- */
    DECLARE
        @override_completion  char(1) = NULL,  /* NULL=computed, A-F=forced, X=IGNORE */
        @override_reliability char(1) = NULL,
        @override_speed       char(1) = NULL,
        @override_workload    char(1) = NULL,
        @weight_completion    decimal(5, 2) = 30.0,
        @weight_reliability   decimal(5, 2) = 25.0,
        @weight_speed         decimal(5, 2) = 20.0,
        @weight_workload      decimal(5, 2) = 25.0,
        @has_overrides        bit = 0;

    IF @GradeOverrides IS NOT NULL
    BEGIN
        SET @has_overrides = 1;

        /* #302: Set-based parsing (replaces cursor) */
        DECLARE @go_parsed TABLE (pair nvarchar(100), cat nvarchar(50), val nvarchar(50));
        INSERT INTO @go_parsed (pair, cat, val)
        SELECT
            pair = LTRIM(RTRIM(s.value)),
            cat  = UPPER(LTRIM(RTRIM(LEFT(LTRIM(RTRIM(s.value)), CHARINDEX(N'=', LTRIM(RTRIM(s.value))) - 1)))),
            val  = UPPER(LTRIM(RTRIM(SUBSTRING(LTRIM(RTRIM(s.value)), CHARINDEX(N'=', LTRIM(RTRIM(s.value))) + 1, 50))))
        FROM STRING_SPLIT(@GradeOverrides, N',') AS s
        WHERE LTRIM(RTRIM(s.value)) <> N''
        AND   CHARINDEX(N'=', LTRIM(RTRIM(s.value))) > 0;

        /* Collect errors for malformed pairs (no '=') */
        SELECT @errors = @errors + N'@GradeOverrides: invalid pair ''' + LTRIM(RTRIM(s.value)) + N''' (expected CATEGORY=VALUE). '
        FROM STRING_SPLIT(@GradeOverrides, N',') AS s
        WHERE LTRIM(RTRIM(s.value)) <> N''
        AND   CHARINDEX(N'=', LTRIM(RTRIM(s.value))) = 0;

        /* Unknown categories */
        SELECT @errors = @errors + N'@GradeOverrides: unknown category ''' + p.cat + N'''. Valid: COMPLETION, RELIABILITY, SPEED, WORKLOAD. '
        FROM @go_parsed AS p
        WHERE p.cat NOT IN (N'COMPLETION', N'RELIABILITY', N'SPEED', N'WORKLOAD');

        /* Invalid values */
        SELECT @errors = @errors + N'@GradeOverrides: invalid value ''' + p.val + N''' for ' + p.cat + N'. Valid: A, B, C, D, F, IGNORE. '
        FROM @go_parsed AS p
        WHERE p.cat IN (N'COMPLETION', N'RELIABILITY', N'SPEED', N'WORKLOAD')
        AND   p.val NOT IN (N'A', N'B', N'C', N'D', N'F', N'IGNORE');

        /* Apply valid overrides (ISNULL preserves defaults for unmentioned categories) */
        SELECT
            @override_completion  = ISNULL(MAX(CASE WHEN p.cat = N'COMPLETION'  THEN CASE WHEN p.val = N'IGNORE' THEN 'X' ELSE LEFT(p.val, 1) END END), @override_completion),
            @override_reliability = ISNULL(MAX(CASE WHEN p.cat = N'RELIABILITY' THEN CASE WHEN p.val = N'IGNORE' THEN 'X' ELSE LEFT(p.val, 1) END END), @override_reliability),
            @override_speed       = ISNULL(MAX(CASE WHEN p.cat = N'SPEED'       THEN CASE WHEN p.val = N'IGNORE' THEN 'X' ELSE LEFT(p.val, 1) END END), @override_speed),
            @override_workload    = ISNULL(MAX(CASE WHEN p.cat = N'WORKLOAD'    THEN CASE WHEN p.val = N'IGNORE' THEN 'X' ELSE LEFT(p.val, 1) END END), @override_workload)
        FROM @go_parsed AS p
        WHERE p.cat IN (N'COMPLETION', N'RELIABILITY', N'SPEED', N'WORKLOAD')
        AND   p.val IN (N'A', N'B', N'C', N'D', N'F', N'IGNORE');
    END;

    IF @GradeWeights IS NOT NULL
    BEGIN
        SET @has_overrides = 1;

        /* #302: Set-based parsing (replaces cursor) */
        DECLARE @gw_parsed TABLE (pair nvarchar(100), cat nvarchar(50), val_str nvarchar(50));
        INSERT INTO @gw_parsed (pair, cat, val_str)
        SELECT
            pair    = LTRIM(RTRIM(s.value)),
            cat     = UPPER(LTRIM(RTRIM(LEFT(LTRIM(RTRIM(s.value)), CHARINDEX(N'=', LTRIM(RTRIM(s.value))) - 1)))),
            val_str = LTRIM(RTRIM(SUBSTRING(LTRIM(RTRIM(s.value)), CHARINDEX(N'=', LTRIM(RTRIM(s.value))) + 1, 50)))
        FROM STRING_SPLIT(@GradeWeights, N',') AS s
        WHERE LTRIM(RTRIM(s.value)) <> N''
        AND   CHARINDEX(N'=', LTRIM(RTRIM(s.value))) > 0;

        /* Malformed pairs (no '=') */
        SELECT @errors = @errors + N'@GradeWeights: invalid pair ''' + LTRIM(RTRIM(s.value)) + N''' (expected CATEGORY=WEIGHT). '
        FROM STRING_SPLIT(@GradeWeights, N',') AS s
        WHERE LTRIM(RTRIM(s.value)) <> N''
        AND   CHARINDEX(N'=', LTRIM(RTRIM(s.value))) = 0;

        /* Unknown categories */
        SELECT @errors = @errors + N'@GradeWeights: unknown category ''' + p.cat + N'''. Valid: COMPLETION, RELIABILITY, SPEED, WORKLOAD. '
        FROM @gw_parsed AS p
        WHERE p.cat NOT IN (N'COMPLETION', N'RELIABILITY', N'SPEED', N'WORKLOAD');

        /* Invalid weights */
        SELECT @errors = @errors + N'@GradeWeights: invalid weight ''' + p.val_str + N''' for ' + p.cat + N'. Must be a non-negative integer. '
        FROM @gw_parsed AS p
        WHERE p.cat IN (N'COMPLETION', N'RELIABILITY', N'SPEED', N'WORKLOAD')
        AND   (TRY_CONVERT(integer, p.val_str) IS NULL OR TRY_CONVERT(integer, p.val_str) < 0);

        /* Apply valid weights (ISNULL preserves defaults for unmentioned categories) */
        SELECT
            @weight_completion  = ISNULL(MAX(CASE WHEN p.cat = N'COMPLETION'  THEN CONVERT(decimal(5, 2), CONVERT(integer, p.val_str)) END), @weight_completion),
            @weight_reliability = ISNULL(MAX(CASE WHEN p.cat = N'RELIABILITY' THEN CONVERT(decimal(5, 2), CONVERT(integer, p.val_str)) END), @weight_reliability),
            @weight_speed       = ISNULL(MAX(CASE WHEN p.cat = N'SPEED'       THEN CONVERT(decimal(5, 2), CONVERT(integer, p.val_str)) END), @weight_speed),
            @weight_workload    = ISNULL(MAX(CASE WHEN p.cat = N'WORKLOAD'    THEN CONVERT(decimal(5, 2), CONVERT(integer, p.val_str)) END), @weight_workload)
        FROM @gw_parsed AS p
        WHERE p.cat IN (N'COMPLETION', N'RELIABILITY', N'SPEED', N'WORKLOAD')
        AND   TRY_CONVERT(integer, p.val_str) IS NOT NULL
        AND   TRY_CONVERT(integer, p.val_str) >= 0;
    END;

    /* Apply IGNORE -> weight=0, weight=0 -> IGNORE (they are equivalent) */
    IF @override_completion  = 'X' SET @weight_completion  = 0;
    IF @override_reliability = 'X' SET @weight_reliability = 0;
    IF @override_speed       = 'X' SET @weight_speed       = 0;
    IF @override_workload    = 'X' SET @weight_workload    = 0;
    IF @weight_completion  = 0 AND @override_completion  IS NULL SET @override_completion  = 'X';
    IF @weight_reliability = 0 AND @override_reliability IS NULL SET @override_reliability = 'X';
    IF @weight_speed       = 0 AND @override_speed       IS NULL SET @override_speed       = 'X';
    IF @weight_workload    = 0 AND @override_workload    IS NULL SET @override_workload    = 'X';

    /* Normalize weights to sum to 1.0 */
    DECLARE @weight_sum decimal(10, 2) = @weight_completion + @weight_reliability + @weight_speed + @weight_workload;

    IF @has_overrides = 1 AND @weight_sum = 0
        SET @errors = @errors + N'All categories are IGNOREd or have weight 0 -- cannot compute OVERALL score. ';

    IF @weight_sum > 0
    BEGIN
        SET @weight_completion  = @weight_completion  / @weight_sum;
        SET @weight_reliability = @weight_reliability / @weight_sum;
        SET @weight_speed       = @weight_speed       / @weight_sum;
        SET @weight_workload    = @weight_workload    / @weight_sum;
    END;

    IF @errors <> N''
    BEGIN
        RAISERROR(N'Parameter validation failed: %s', 16, 1, @errors) WITH NOWAIT;
        RETURN;
    END;

    /*
    ============================================================================
    VALIDATE COMMANDLOG EXISTS
    ============================================================================
    */
    DECLARE
        @commandlog_ref nvarchar(500),
        @commandlog_db sysname = ISNULL(@CommandLogDatabase, DB_NAME()),
        @sql nvarchar(max),
        @commandlog_exists bit = 0;

    SET @commandlog_ref = QUOTENAME(@commandlog_db) + N'.dbo.CommandLog';

    SET @sql = N'
        IF OBJECT_ID(N''' + REPLACE(@commandlog_ref, N'''', N'''''') + N''', N''U'') IS NOT NULL
            SET @exists = 1;
        ELSE
            SET @exists = 0;
    ';

    EXECUTE sys.sp_executesql
        @sql,
        N'@exists bit OUTPUT',
        @exists = @commandlog_exists OUTPUT;

    IF @commandlog_exists = 0
    BEGIN
        RAISERROR(N'CommandLog table not found at %s. Verify @CommandLogDatabase parameter.', 16, 1, @commandlog_ref) WITH NOWAIT;
        RETURN;
    END;

    DECLARE @expert_int integer = CONVERT(integer, @ExpertMode);

    /* #236: @SingleResultSet=1 auto-promotes @ExpertMode=1 so automation always gets RS 1-13.
       Also forces @SkipHistory=1 because INSERT...EXEC callers cannot write to persistent tables
       (SQL Server nested transaction limitation). */
    IF @SingleResultSet = 1
    BEGIN
        IF @ExpertMode = 0
        BEGIN
            SET @ExpertMode = 1;
            SET @expert_int = 1;
            RAISERROR(N'Note: @SingleResultSet=1 auto-enabled @ExpertMode=1 for stable ResultSetID contract (1-13).', 10, 1) WITH NOWAIT;
        END;

        IF @SkipHistory = 0
        BEGIN
            SET @SkipHistory = 1;
            RAISERROR(N'Note: @SingleResultSet=1 auto-enabled @SkipHistory=1 (INSERT...EXEC cannot write persistent tables).', 10, 1) WITH NOWAIT;
        END;
    END;

    RAISERROR(N'sp_StatUpdate_Diag v%s', 10, 1, @procedure_version) WITH NOWAIT;
    RAISERROR(N'CommandLog: %s', 10, 1, @commandlog_ref) WITH NOWAIT;
    RAISERROR(N'Analysis window: %i days', 10, 1, @DaysBack) WITH NOWAIT;
    DECLARE @obfuscate_int integer = CONVERT(integer, @Obfuscate);
    RAISERROR(N'Obfuscate: %i', 10, 1, @obfuscate_int) WITH NOWAIT;
    RAISERROR(N'ExpertMode: %i', 10, 1, @expert_int) WITH NOWAIT;

    IF @has_overrides = 1
    BEGIN
        DECLARE @override_msg nvarchar(500) = N'Grade overrides active:';
        IF @override_completion  IS NOT NULL SET @override_msg = @override_msg + N' COMPLETION=' + CASE @override_completion  WHEN 'X' THEN N'IGNORE' ELSE @override_completion  END;
        IF @override_reliability IS NOT NULL SET @override_msg = @override_msg + N' RELIABILITY=' + CASE @override_reliability WHEN 'X' THEN N'IGNORE' ELSE @override_reliability END;
        IF @override_speed       IS NOT NULL SET @override_msg = @override_msg + N' SPEED=' + CASE @override_speed       WHEN 'X' THEN N'IGNORE' ELSE @override_speed       END;
        IF @override_workload    IS NOT NULL SET @override_msg = @override_msg + N' WORKLOAD=' + CASE @override_workload    WHEN 'X' THEN N'IGNORE' ELSE @override_workload    END;
        RAISERROR(@override_msg, 10, 1) WITH NOWAIT;

        DECLARE @weight_msg nvarchar(500) = N'Effective weights: COMPLETION='
            + CONVERT(nvarchar(10), CONVERT(integer, @weight_completion * 100))
            + N', RELIABILITY=' + CONVERT(nvarchar(10), CONVERT(integer, @weight_reliability * 100))
            + N', SPEED=' + CONVERT(nvarchar(10), CONVERT(integer, @weight_speed * 100))
            + N', WORKLOAD=' + CONVERT(nvarchar(10), CONVERT(integer, @weight_workload * 100))
            + N' (sum=100)';
        RAISERROR(@weight_msg, 10, 1) WITH NOWAIT;
    END;

    IF @Debug = 1
    BEGIN
        SET @debug_elapsed_ms = DATEDIFF(MILLISECOND, @debug_timer, SYSDATETIME());
        RAISERROR(N'', 10, 1) WITH NOWAIT;
        RAISERROR(N'DEBUG: === Effective Parameters ===', 10, 1) WITH NOWAIT;
        RAISERROR(N'DEBUG: @DaysBack=%i  @LongRunningMinutes=%i  @FailureThreshold=%i', 10, 1, @DaysBack, @LongRunningMinutes, @FailureThreshold) WITH NOWAIT;
        RAISERROR(N'DEBUG: @TimeLimitExhaustionPct=%i  @ThroughputWindowDays=%i  @TopN=%i', 10, 1, @TimeLimitExhaustionPct, @ThroughputWindowDays, @TopN) WITH NOWAIT;

        DECLARE @debug_efficacy_days integer = ISNULL(@EfficacyDaysBack, @DaysBack);
        DECLARE @debug_efficacy_detail integer = ISNULL(@EfficacyDetailDays, 14);
        RAISERROR(N'DEBUG: @EfficacyDaysBack=%i (resolved)  @EfficacyDetailDays=%i (resolved)', 10, 1, @debug_efficacy_days, @debug_efficacy_detail) WITH NOWAIT;

        DECLARE @debug_expert integer = CONVERT(integer, @ExpertMode);
        DECLARE @debug_skip integer = CONVERT(integer, @SkipHistory);
        DECLARE @debug_single integer = CONVERT(integer, @SingleResultSet);
        DECLARE @debug_obf integer = CONVERT(integer, @Obfuscate);
        RAISERROR(N'DEBUG: @ExpertMode=%i  @SkipHistory=%i  @SingleResultSet=%i  @Obfuscate=%i', 10, 1, @debug_expert, @debug_skip, @debug_single, @debug_obf) WITH NOWAIT;

        DECLARE @debug_w_comp integer = CONVERT(integer, @weight_completion * 100);
        DECLARE @debug_w_rel  integer = CONVERT(integer, @weight_reliability * 100);
        DECLARE @debug_w_spd  integer = CONVERT(integer, @weight_speed * 100);
        DECLARE @debug_w_wl   integer = CONVERT(integer, @weight_workload * 100);
        RAISERROR(N'DEBUG: Weights: COMPLETION=%i%%  RELIABILITY=%i%%  SPEED=%i%%  WORKLOAD=%i%%',
            10, 1, @debug_w_comp, @debug_w_rel, @debug_w_spd, @debug_w_wl) WITH NOWAIT;
        RAISERROR(N'DEBUG: Initialization complete (%i ms)', 10, 1, @debug_elapsed_ms) WITH NOWAIT;
        RAISERROR(N'', 10, 1) WITH NOWAIT;
    END;

    /* BUG-07: orphan threshold now a named constant (was hardcoded 60 minutes).
       Default 2880 = 48 hours, matching sp_StatUpdate @OrphanedRunThresholdHours default.
       A run still in progress (no END record) is only classified as orphaned once this
       threshold has elapsed; runs started more recently are excluded to avoid false C1 alerts. */
    DECLARE @OrphanedRunThresholdMinutes integer = 2880;

    /*
    ============================================================================
    PERSISTENT HISTORY TABLE (incremental -- skip XML re-parsing on repeat runs)

    Inspired by sp_Blitz / sp_PressureDetector baseline tables. The first run
    parses all CommandLog XML. Subsequent runs only parse new rows (ID > max
    previously cached), then merge into the persistent table. This cuts repeat
    execution time dramatically on servers with large CommandLog tables.

    Table lives in the same database as CommandLog (so it travels with backups).
    Opt out with @SkipHistory = 1 for ephemeral/test scenarios.
    ============================================================================
    */
    DECLARE
        @history_ref nvarchar(500),
        @history_exists bit = 0,
        @history_max_id integer = 0,
        @history_max_run_start datetime2(3) = NULL;

    SET @history_ref = QUOTENAME(@commandlog_db) + N'.dbo.StatUpdateDiagHistory';

    IF @SkipHistory = 0
    BEGIN
        /* Check if history table exists */
        SET @sql = N'
            IF OBJECT_ID(N''' + REPLACE(@history_ref, N'''', N'''''') + N''', N''U'') IS NOT NULL
                SET @exists = 1;
            ELSE
                SET @exists = 0;
        ';

        EXECUTE sys.sp_executesql
            @sql,
            N'@exists bit OUTPUT',
            @exists = @history_exists OUTPUT;

        /* Auto-create if missing */
        IF @history_exists = 0
        BEGIN
            SET @sql = N'
                CREATE TABLE ' + @history_ref + N'
                (
                    SnapshotID      integer         IDENTITY(1,1) NOT NULL,
                    CapturedAt      datetime2(3)    NOT NULL DEFAULT SYSDATETIME(),
                    MaxCommandLogID integer         NOT NULL,
                    RunLabel        nvarchar(100)   NOT NULL,
                    StartTime       datetime2(3)    NOT NULL,
                    HealthScore     integer         NULL,
                    OverallGrade    char(1)         NULL,
                    CompletionPct   decimal(5,1)    NULL,
                    AvgSecPerStat   decimal(10,1)   NULL,
                    WorkloadCoveragePct decimal(5,1) NULL,
                    HighCpuFirstQuartilePct decimal(5,1) NULL,
                    MinutesToHighCpuComplete decimal(10,1) NULL,
                    StatsFound      integer         NULL,
                    StatsProcessed  integer         NULL,
                    StatsFailed     integer         NULL,
                    StopReason      nvarchar(50)    NULL,
                    DurationSeconds integer         NULL,
                    IsQSRun         bit             NOT NULL DEFAULT 0,
                    DiagVersion     varchar(20)     NOT NULL,

                    CONSTRAINT PK_StatUpdateDiagHistory PRIMARY KEY CLUSTERED (SnapshotID) WITH (DATA_COMPRESSION = PAGE),
                    INDEX IX_StatUpdateDiagHistory_RunLabel UNIQUE NONCLUSTERED (RunLabel) WITH (DATA_COMPRESSION = PAGE)
                );
            ';

            EXECUTE sys.sp_executesql @sql;
            RAISERROR(N'  Created persistent history table: %s', 10, 1, @history_ref) WITH NOWAIT;
            SET @history_exists = 1;
        END
        ELSE
        BEGIN
            /* Get watermark: max CommandLog ID already processed */
            SET @sql = N'
                SELECT @max_id = ISNULL(MAX(MaxCommandLogID), 0),
                       @max_start = MAX(StartTime)
                FROM ' + @history_ref + N';
            ';

            EXECUTE sys.sp_executesql
                @sql,
                N'@max_id integer OUTPUT, @max_start datetime2(3) OUTPUT',
                @max_id = @history_max_id OUTPUT,
                @max_start = @history_max_run_start OUTPUT;

            DECLARE @history_row_count integer;
            SET @sql = N'SELECT @cnt = COUNT_BIG(*) FROM ' + @history_ref + N';';
            EXECUTE sys.sp_executesql @sql, N'@cnt integer OUTPUT', @cnt = @history_row_count OUTPUT;

            /* Watermark gap detection: if CommandLog was archived/truncated, watermark
               may exceed current max ID, permanently skipping new data (#232) */
            DECLARE @cl_max_id integer;
            SET @sql = N'SELECT @mx = ISNULL(MAX(ID), 0) FROM ' + @commandlog_ref + N';';
            EXECUTE sys.sp_executesql @sql, N'@mx integer OUTPUT', @mx = @cl_max_id OUTPUT;

            IF @cl_max_id > 0 AND @history_max_id > @cl_max_id
            BEGIN
                RAISERROR(N'  WARNING: History watermark (%i) exceeds CommandLog max ID (%i). Resetting watermark -- CommandLog may have been archived.', 10, 1,
                    @history_max_id, @cl_max_id) WITH NOWAIT;
                SET @history_max_id = 0;    /* #240: reset to 0, not @cl_max_id -- NOT EXISTS dedup guard prevents duplicates */
            END;

            RAISERROR(N'  History table: %s (%i snapshots, watermark ID %i)', 10, 1,
                @history_ref, @history_row_count, @history_max_id) WITH NOWAIT;
        END;
    END
    ELSE
    BEGIN
        RAISERROR(N'  History: skipped (@SkipHistory = 1)', 10, 1) WITH NOWAIT;
    END;

    /*
    ============================================================================
    PERSISTENT STAT CACHE TABLE (avoids re-parsing CommandLog XML on every run)

    Caches the parsed #stat_updates data (extracted XML fields as typed columns).
    Watermark-based: only parse new CommandLog rows (ID > max cached).
    Same database as CommandLog (travels with backups). Opt-out: @SkipHistory=1.
    ============================================================================
    */
    DECLARE
        @cache_ref nvarchar(500),
        @cache_exists bit = 0,
        @cache_watermark integer = 0;

    SET @cache_ref = QUOTENAME(@commandlog_db) + N'.dbo.StatUpdateDiagCache';

    IF @SkipHistory = 0
    BEGIN
        /* Check if cache table exists */
        SET @sql = N'
            IF OBJECT_ID(N''' + REPLACE(@cache_ref, N'''', N'''''') + N''', N''U'') IS NOT NULL
                SET @exists = 1;
            ELSE
                SET @exists = 0;
        ';

        EXECUTE sys.sp_executesql
            @sql,
            N'@exists bit OUTPUT',
            @exists = @cache_exists OUTPUT;

        /* Auto-create if missing */
        IF @cache_exists = 0
        BEGIN
            SET @sql = N'
                CREATE TABLE ' + @cache_ref + N'
                (
                    CommandLogID        integer         NOT NULL,
                    RunLabel            nvarchar(100)   NULL,
                    DatabaseName        sysname         NULL,
                    SchemaName          sysname         NULL,
                    ObjectName          sysname         NULL,
                    StatisticsName      sysname         NULL,
                    StartTime           datetime2(3)    NOT NULL,
                    EndTime             datetime2(3)    NULL,
                    DurationMs          integer         NULL,
                    ErrorNumber         integer         NULL,
                    ErrorMessage        nvarchar(3900)  NULL,
                    ModificationCounter bigint          NULL,
                    RowCount_           bigint          NULL,
                    PageCount           bigint          NULL,
                    SizeMB              integer         NULL,
                    DaysStale           integer         NULL,
                    QualifyReason       nvarchar(100)   NULL,
                    HasNorecompute      bit             NULL,
                    IsHeap              bit             NULL,
                    AutoCreated         bit             NULL,
                    QSPlanCount         integer         NULL,
                    QSTotalExecutions   bigint          NULL,
                    QSTotalCpuMs        bigint          NULL,
                    EffectiveSamplePct  integer         NULL,
                    SampleSource        nvarchar(50)    NULL,
                    HasFilter           bit             NULL,
                    FilteredDriftRatio  float           NULL,
                    IsIncremental       bit             NULL,
                    ProcessingPosition  integer         NULL,
                    ObjectId            integer         NULL,
                    StatsId             integer         NULL,

                    CONSTRAINT PK_StatUpdateDiagCache PRIMARY KEY CLUSTERED (CommandLogID)
                        WITH (DATA_COMPRESSION = PAGE)
                );
            ';

            EXECUTE sys.sp_executesql @sql;
            RAISERROR(N'  Created persistent cache table: %s', 10, 1, @cache_ref) WITH NOWAIT;
            SET @cache_exists = 1;
        END
        ELSE
        BEGIN
            /* Check for new columns that may have been added in later versions */
            DECLARE @col_check_sql nvarchar(max);
            SET @col_check_sql = N'
                IF COL_LENGTH(N''' + REPLACE(@cache_ref, N'''', N'''''') + N''', N''ProcessingPosition'') IS NULL
                    ALTER TABLE ' + @cache_ref + N' ADD ProcessingPosition integer NULL;
                IF COL_LENGTH(N''' + REPLACE(@cache_ref, N'''', N'''''') + N''', N''ObjectId'') IS NULL
                    ALTER TABLE ' + @cache_ref + N' ADD ObjectId integer NULL;
                IF COL_LENGTH(N''' + REPLACE(@cache_ref, N'''', N'''''') + N''', N''StatsId'') IS NULL
                    ALTER TABLE ' + @cache_ref + N' ADD StatsId integer NULL;
            ';
            EXECUTE sys.sp_executesql @col_check_sql;

            /* Get watermark: max CommandLogID already cached */
            SET @sql = N'SELECT @wm = ISNULL(MAX(CommandLogID), 0) FROM ' + @cache_ref + N';';
            EXECUTE sys.sp_executesql @sql, N'@wm integer OUTPUT', @wm = @cache_watermark OUTPUT;

            /* Watermark gap detection: same as history table (#232) */
            DECLARE @cl_max_id_cache integer;
            SET @sql = N'SELECT @mx = ISNULL(MAX(ID), 0) FROM ' + @commandlog_ref + N';';
            EXECUTE sys.sp_executesql @sql, N'@mx integer OUTPUT', @mx = @cl_max_id_cache OUTPUT;

            IF @cl_max_id_cache > 0 AND @cache_watermark > @cl_max_id_cache
            BEGIN
                RAISERROR(N'  WARNING: Cache watermark (%i) exceeds CommandLog max ID (%i). Resetting -- CommandLog may have been archived.', 10, 1,
                    @cache_watermark, @cl_max_id_cache) WITH NOWAIT;
                SET @cache_watermark = 0;    /* #312: reset to 0, not @cl_max_id_cache -- NOT EXISTS dedup guard prevents duplicates */
            END;

            RAISERROR(N'  Cache table: %s (watermark ID %i)', 10, 1, @cache_ref, @cache_watermark) WITH NOWAIT;
        END;
    END;

    RAISERROR(N'', 10, 1) WITH NOWAIT;

    /*
    ============================================================================
    TEMP TABLES
    ============================================================================
    */

    /* Run pairs: START/END matched by RunLabel */
    CREATE TABLE #runs
    (
        RunLabel nvarchar(100) NOT NULL,
        StartTime datetime2(3) NOT NULL,
        EndTime datetime2(3) NULL,
        DurationSeconds integer NULL,
        StopReason nvarchar(50) NULL,
        StatsFound integer NULL,
        StatsProcessed integer NULL,
        StatsSucceeded integer NULL,
        StatsFailed integer NULL,
        StatsRemaining integer NULL,
        /* Parameters from START record */
        [Version] nvarchar(20) NULL,
        [Databases] nvarchar(max) NULL,
        TimeLimit integer NULL,
        ModificationThreshold bigint NULL,
        TieredThresholds nvarchar(10) NULL,
        ThresholdLogic nvarchar(10) NULL,
        SortOrder nvarchar(50) NULL,
        QueryStorePriority nvarchar(1) NULL,
        StatisticsSample integer NULL,
        StatsInParallel nvarchar(1) NULL,
        Preset nvarchar(30) NULL,
        LongRunningThresholdMinutes integer NULL,
        LongRunningSamplePercent integer NULL,
        GroupByJoinPattern nvarchar(1) NULL,
        FilteredStatsMode nvarchar(10) NULL,
        BatchLimit integer NULL,
        FailFast bit NULL,
        /* Computed */
        IsKilled bit NOT NULL DEFAULT 0,

        CONSTRAINT PK_runs PRIMARY KEY NONCLUSTERED (RunLabel)
    );

    /* Individual stat updates */
    CREATE TABLE #stat_updates
    (
        ID integer NOT NULL,
        RunLabel nvarchar(100) NULL,
        DatabaseName sysname NULL,
        SchemaName sysname NULL,
        ObjectName sysname NULL,
        StatisticsName sysname NULL,
        StartTime datetime2(3) NOT NULL,
        EndTime datetime2(3) NULL,
        DurationMs integer NULL,
        ErrorNumber integer NULL,
        ErrorMessage nvarchar(3900) NULL,
        ModificationCounter bigint NULL,
        RowCount_ bigint NULL,
        PageCount bigint NULL,
        SizeMB integer NULL,
        DaysStale integer NULL,
        QualifyReason nvarchar(100) NULL,
        HasNorecompute bit NULL,
        IsHeap bit NULL,
        AutoCreated bit NULL,
        QSPlanCount integer NULL,
        QSTotalExecutions bigint NULL,
        QSTotalCpuMs bigint NULL,
        EffectiveSamplePct integer NULL,
        SampleSource nvarchar(50) NULL,
        HasFilter bit NULL,
        FilteredDriftRatio float NULL,
        IsIncremental bit NULL,
        ProcessingPosition integer NULL,
        ObjectId integer NULL,           /* #268: from ExtendedInfo v2.20+ */
        StatsId integer NULL,            /* #268: from ExtendedInfo v2.20+ */

        CONSTRAINT PK_stat_updates PRIMARY KEY NONCLUSTERED (ID)
    );

    /* Recommendations output */
    CREATE TABLE #recommendations
    (
        FindingID integer IDENTITY(1, 1) NOT NULL,
        Severity nvarchar(10) NOT NULL,
        Category nvarchar(50) NOT NULL,
        Finding nvarchar(500) NOT NULL,
        Evidence nvarchar(2000) NULL,
        Recommendation nvarchar(2000) NULL,
        ExampleCall nvarchar(1000) NULL,
        SortPriority integer NOT NULL DEFAULT 50,

        CONSTRAINT PK_recommendations PRIMARY KEY CLUSTERED (FindingID)
    );

    /* Obfuscation mapping (populated only when @Obfuscate = 1) */
    CREATE TABLE #obfuscation_map
    (
        ObjectType nvarchar(20) NOT NULL,
        OriginalName nvarchar(256) NOT NULL,
        ObfuscatedName nvarchar(50) NOT NULL
    );

    /* Executive Dashboard (always populated, always returned as RS 1) */
    CREATE TABLE #executive_dashboard
    (
        Category        nvarchar(30)    NOT NULL,
        Grade           char(1)         NOT NULL,
        Score           integer         NOT NULL,
        Headline        nvarchar(500)   NOT NULL,
        Detail          nvarchar(2000)  NULL,
        Trend           nvarchar(20)    NULL,
        SortOrder       integer         NOT NULL
    );

    /* Single result set accumulator (populated only when @SingleResultSet = 1) */
    CREATE TABLE #single_rs
    (
        SingleRsID      int             IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ResultSetID     int             NOT NULL,
        ResultSetName   nvarchar(100)   NOT NULL,
        RowNum          int             NOT NULL,
        RowData         nvarchar(max)   NULL
    );

    /*
    ============================================================================
    DATA EXTRACTION: Populate #runs from START/END pairs
    ============================================================================
    */
    RAISERROR(N'Extracting run data...', 10, 1) WITH NOWAIT;

    SET @sql = N'
    INSERT INTO #runs
    (
        RunLabel, StartTime, EndTime, DurationSeconds, StopReason,
        StatsFound, StatsProcessed, StatsSucceeded, StatsFailed, StatsRemaining,
        [Version], [Databases], TimeLimit, ModificationThreshold, TieredThresholds,
        ThresholdLogic, SortOrder, QueryStorePriority, StatisticsSample,
        StatsInParallel, Preset, LongRunningThresholdMinutes, LongRunningSamplePercent,
        GroupByJoinPattern, FilteredStatsMode, BatchLimit, FailFast, IsKilled
    )
    SELECT
        run_label           = ISNULL(s.ExtendedInfo.value(N''(Parameters/RunLabel)[1]'', N''nvarchar(100)''),
                                     N''legacy_'' + CONVERT(nvarchar(20), s.ID)),
        start_time          = s.StartTime,
        end_time            = e.EndTime,
        duration_seconds    = DATEDIFF(SECOND, s.StartTime, e.EndTime),
        stop_reason         = e.ExtendedInfo.value(N''(Summary/StopReason)[1]'', N''nvarchar(50)''),
        stats_found         = e.ExtendedInfo.value(N''(Summary/StatsFound)[1]'', N''int''),
        stats_processed     = e.ExtendedInfo.value(N''(Summary/StatsProcessed)[1]'', N''int''),
        stats_succeeded     = e.ExtendedInfo.value(N''(Summary/StatsSucceeded)[1]'', N''int''),
        stats_failed        = e.ExtendedInfo.value(N''(Summary/StatsFailed)[1]'', N''int''),
        stats_remaining     = e.ExtendedInfo.value(N''(Summary/StatsRemaining)[1]'', N''int''),
        [version]           = s.ExtendedInfo.value(N''(Parameters/Version)[1]'', N''nvarchar(20)''),
        [databases]         = s.ExtendedInfo.value(N''(Parameters/Databases)[1]'', N''nvarchar(max)''),
        time_limit          = s.ExtendedInfo.value(N''(Parameters/TimeLimit)[1]'', N''int''),
        mod_threshold       = s.ExtendedInfo.value(N''(Parameters/ModificationThreshold)[1]'', N''bigint''),
        tiered_thresholds   = s.ExtendedInfo.value(N''(Parameters/TieredThresholds)[1]'', N''nvarchar(10)''),
        threshold_logic     = s.ExtendedInfo.value(N''(Parameters/ThresholdLogic)[1]'', N''nvarchar(10)''),
        sort_order          = s.ExtendedInfo.value(N''(Parameters/SortOrder)[1]'', N''nvarchar(50)''),
        qs_priority         = s.ExtendedInfo.value(N''(Parameters/QueryStorePriority)[1]'', N''nvarchar(1)''),
        stat_sample         = s.ExtendedInfo.value(N''(Parameters/StatisticsSample)[1]'', N''int''),
        parallel            = s.ExtendedInfo.value(N''(Parameters/StatsInParallel)[1]'', N''nvarchar(1)''),
        preset              = s.ExtendedInfo.value(N''(Parameters/Preset)[1]'', N''nvarchar(30)''),
        long_run_min        = s.ExtendedInfo.value(N''(Parameters/LongRunningThresholdMinutes)[1]'', N''int''),
        long_run_pct        = s.ExtendedInfo.value(N''(Parameters/LongRunningSamplePercent)[1]'', N''int''),
        group_join          = s.ExtendedInfo.value(N''(Parameters/GroupByJoinPattern)[1]'', N''nvarchar(1)''),
        filtered_mode       = s.ExtendedInfo.value(N''(Parameters/FilteredStatsMode)[1]'', N''nvarchar(10)''),
        batch_limit         = s.ExtendedInfo.value(N''(Parameters/BatchLimit)[1]'', N''int''),
        fail_fast           = s.ExtendedInfo.value(N''(Parameters/FailFast)[1]'', N''bit''),
        is_killed           = CASE
                                  WHEN e.ID IS NULL THEN 1
                                  WHEN e.ExtendedInfo.value(N''(Summary/StopReason)[1]'', N''nvarchar(50)'') = N''KILLED'' THEN 1
                                  ELSE 0
                              END
    FROM ' + @commandlog_ref + N' AS s
    LEFT JOIN ' + @commandlog_ref + N' AS e
        ON  e.CommandType = N''SP_STATUPDATE_END''
        AND e.ExtendedInfo.value(N''(Summary/RunLabel)[1]'', N''nvarchar(100)'') =
            s.ExtendedInfo.value(N''(Parameters/RunLabel)[1]'', N''nvarchar(100)'')
    WHERE s.CommandType = N''SP_STATUPDATE_START''
    AND   s.StartTime >= DATEADD(DAY, -@days_back, GETDATE())
    AND   NOT (e.ID IS NULL AND DATEDIFF(MINUTE, s.StartTime, GETDATE()) < @orphan_minutes);
    ';

    EXECUTE sys.sp_executesql
        @sql,
        N'@days_back integer, @orphan_minutes integer',
        @days_back = @DaysBack,
        @orphan_minutes = @OrphanedRunThresholdMinutes;

    /* #216: Dedup -- keep only the most recent START per RunLabel */
    WITH dupes AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY RunLabel ORDER BY StartTime DESC) AS rn
        FROM #runs
    )
    DELETE FROM dupes WHERE rn > 1;

    DECLARE @run_count integer = (SELECT COUNT_BIG(*) FROM #runs);
    DECLARE @killed_count integer = (SELECT COUNT_BIG(*) FROM #runs WHERE IsKilled = 1);

    RAISERROR(N'  Runs found: %i (killed: %i)', 10, 1, @run_count, @killed_count) WITH NOWAIT;

    IF @Debug = 1
    BEGIN
        SET @debug_elapsed_ms = DATEDIFF(MILLISECOND, @debug_timer, SYSDATETIME());
        DECLARE @debug_min_start nvarchar(30) = CONVERT(nvarchar(30), (SELECT MIN(StartTime) FROM #runs), 120);
        DECLARE @debug_max_start nvarchar(30) = CONVERT(nvarchar(30), (SELECT MAX(StartTime) FROM #runs), 120);
        DECLARE @debug_legacy_count integer = (SELECT COUNT_BIG(*) FROM #runs WHERE RunLabel LIKE N'legacy_%');
        RAISERROR(N'DEBUG: #runs date range: %s to %s', 10, 1, @debug_min_start, @debug_max_start) WITH NOWAIT;
        IF @debug_legacy_count > 0
            RAISERROR(N'DEBUG: #runs with synthetic legacy labels: %i (pre-RunLabel CommandLog entries)', 10, 1, @debug_legacy_count) WITH NOWAIT;
        RAISERROR(N'DEBUG: Run extraction complete (%i ms)', 10, 1, @debug_elapsed_ms) WITH NOWAIT;
    END;

    /*
    ============================================================================
    DATA EXTRACTION: Populate #stat_updates from UPDATE_STATISTICS entries
    Uses cache table when available (@SkipHistory=0) to avoid re-parsing XML.
    ============================================================================
    */
    RAISERROR(N'Extracting stat update data...', 10, 1) WITH NOWAIT;

    IF @SkipHistory = 0 AND @cache_exists = 1
    BEGIN
        /* Step 1: Populate cache with new rows (ID > watermark) */
        SET @sql = N'
        INSERT INTO ' + @cache_ref + N'
        (
            CommandLogID, RunLabel, DatabaseName, SchemaName, ObjectName, StatisticsName,
            StartTime, EndTime, DurationMs, ErrorNumber, ErrorMessage,
            ModificationCounter, RowCount_, PageCount, SizeMB, DaysStale,
            QualifyReason, HasNorecompute, IsHeap, AutoCreated,
            QSPlanCount, QSTotalExecutions, QSTotalCpuMs,
            EffectiveSamplePct, SampleSource, HasFilter, FilteredDriftRatio, IsIncremental,
            ProcessingPosition, ObjectId, StatsId
        )
        SELECT
            c.ID,
            c.ExtendedInfo.value(N''(ExtendedInfo/RunLabel)[1]'', N''nvarchar(100)''),
            c.DatabaseName, c.SchemaName, c.ObjectName,
            ISNULL(c.StatisticsName, c.IndexName),
            c.StartTime, c.EndTime,
            DATEDIFF(MILLISECOND, c.StartTime, c.EndTime),
            c.ErrorNumber, c.ErrorMessage,
            c.ExtendedInfo.value(N''(ExtendedInfo/ModificationCounter)[1]'', N''bigint''),
            c.ExtendedInfo.value(N''(ExtendedInfo/RowCount)[1]'', N''bigint''),
            c.ExtendedInfo.value(N''(ExtendedInfo/PageCount)[1]'', N''bigint''),
            c.ExtendedInfo.value(N''(ExtendedInfo/SizeMB)[1]'', N''int''),
            c.ExtendedInfo.value(N''(ExtendedInfo/DaysStale)[1]'', N''int''),
            c.ExtendedInfo.value(N''(ExtendedInfo/QualifyReason)[1]'', N''nvarchar(100)''),
            c.ExtendedInfo.value(N''(ExtendedInfo/HasNorecompute)[1]'', N''bit''),
            c.ExtendedInfo.value(N''(ExtendedInfo/IsHeap)[1]'', N''bit''),
            c.ExtendedInfo.value(N''(ExtendedInfo/AutoCreated)[1]'', N''bit''),
            c.ExtendedInfo.value(N''(ExtendedInfo/QSPlanCount)[1]'', N''int''),
            c.ExtendedInfo.value(N''(ExtendedInfo/QSTotalExecutions)[1]'', N''bigint''),
            c.ExtendedInfo.value(N''(ExtendedInfo/QSTotalCpuMs)[1]'', N''bigint''),
            c.ExtendedInfo.value(N''(ExtendedInfo/EffectiveSamplePct)[1]'', N''int''),
            c.ExtendedInfo.value(N''(ExtendedInfo/SampleSource)[1]'', N''nvarchar(50)''),
            c.ExtendedInfo.value(N''(ExtendedInfo/HasFilter)[1]'', N''bit''),
            c.ExtendedInfo.value(N''(ExtendedInfo/FilteredDriftRatio)[1]'', N''float''),
            c.ExtendedInfo.value(N''(ExtendedInfo/IsIncremental)[1]'', N''bit''),
            c.ExtendedInfo.value(N''(ExtendedInfo/ProcessingPosition)[1]'', N''int''),
            c.ExtendedInfo.value(N''(ExtendedInfo/ObjectId)[1]'', N''int''),
            c.ExtendedInfo.value(N''(ExtendedInfo/StatsId)[1]'', N''int'')
        FROM ' + @commandlog_ref + N' AS c
        WHERE c.CommandType = N''UPDATE_STATISTICS''
        AND   c.ID > @watermark;
        ';

        DECLARE @cache_new_rows integer;
        EXECUTE sys.sp_executesql @sql, N'@watermark integer', @watermark = @cache_watermark;
        SET @cache_new_rows = @@ROWCOUNT;

        IF @cache_new_rows > 0
            RAISERROR(N'  Cached %i new CommandLog rows', 10, 1, @cache_new_rows) WITH NOWAIT;

        /* Step 2: Read from cache (fast typed column reads, no XML parsing) */
        SET @sql = N'
        INSERT INTO #stat_updates
        (
            ID, RunLabel, DatabaseName, SchemaName, ObjectName, StatisticsName,
            StartTime, EndTime, DurationMs, ErrorNumber, ErrorMessage,
            ModificationCounter, RowCount_, PageCount, SizeMB, DaysStale,
            QualifyReason, HasNorecompute, IsHeap, AutoCreated,
            QSPlanCount, QSTotalExecutions, QSTotalCpuMs,
            EffectiveSamplePct, SampleSource, HasFilter, FilteredDriftRatio, IsIncremental,
            ProcessingPosition, ObjectId, StatsId
        )
        SELECT
            ca.CommandLogID, ca.RunLabel, ca.DatabaseName, ca.SchemaName, ca.ObjectName, ca.StatisticsName,
            ca.StartTime, ca.EndTime, ca.DurationMs, ca.ErrorNumber, ca.ErrorMessage,
            ca.ModificationCounter, ca.RowCount_, ca.PageCount, ca.SizeMB, ca.DaysStale,
            ca.QualifyReason, ca.HasNorecompute, ca.IsHeap, ca.AutoCreated,
            ca.QSPlanCount, ca.QSTotalExecutions, ca.QSTotalCpuMs,
            ca.EffectiveSamplePct, ca.SampleSource, ca.HasFilter, ca.FilteredDriftRatio, ca.IsIncremental,
            ca.ProcessingPosition, ca.ObjectId, ca.StatsId
        FROM ' + @cache_ref + N' AS ca
        WHERE ca.StartTime >= DATEADD(DAY, -@days_back, GETDATE());
        ';

        EXECUTE sys.sp_executesql @sql, N'@days_back integer', @days_back = @DaysBack;

        RAISERROR(N'  (loaded from cache)', 10, 1) WITH NOWAIT;
    END
    ELSE
    BEGIN
        /* Direct XML extraction (no cache available or @SkipHistory=1) */
        SET @sql = N'
        INSERT INTO #stat_updates
        (
            ID, RunLabel, DatabaseName, SchemaName, ObjectName, StatisticsName,
            StartTime, EndTime, DurationMs, ErrorNumber, ErrorMessage,
            ModificationCounter, RowCount_, PageCount, SizeMB, DaysStale,
            QualifyReason, HasNorecompute, IsHeap, AutoCreated,
            QSPlanCount, QSTotalExecutions, QSTotalCpuMs,
            EffectiveSamplePct, SampleSource, HasFilter, FilteredDriftRatio, IsIncremental,
            ProcessingPosition, ObjectId, StatsId
        )
        SELECT
            id                  = c.ID,
            run_label           = c.ExtendedInfo.value(N''(ExtendedInfo/RunLabel)[1]'', N''nvarchar(100)''),
            database_name       = c.DatabaseName,
            schema_name         = c.SchemaName,
            object_name         = c.ObjectName,
            statistics_name     = ISNULL(c.StatisticsName, c.IndexName),
            start_time          = c.StartTime,
            end_time            = c.EndTime,
            duration_ms         = DATEDIFF(MILLISECOND, c.StartTime, c.EndTime),
            error_number        = c.ErrorNumber,
            error_message       = c.ErrorMessage,
            mod_counter         = c.ExtendedInfo.value(N''(ExtendedInfo/ModificationCounter)[1]'', N''bigint''),
            row_count           = c.ExtendedInfo.value(N''(ExtendedInfo/RowCount)[1]'', N''bigint''),
            page_count          = c.ExtendedInfo.value(N''(ExtendedInfo/PageCount)[1]'', N''bigint''),
            size_mb             = c.ExtendedInfo.value(N''(ExtendedInfo/SizeMB)[1]'', N''int''),
            days_stale          = c.ExtendedInfo.value(N''(ExtendedInfo/DaysStale)[1]'', N''int''),
            qualify_reason      = c.ExtendedInfo.value(N''(ExtendedInfo/QualifyReason)[1]'', N''nvarchar(100)''),
            has_norecompute     = c.ExtendedInfo.value(N''(ExtendedInfo/HasNorecompute)[1]'', N''bit''),
            is_heap             = c.ExtendedInfo.value(N''(ExtendedInfo/IsHeap)[1]'', N''bit''),
            auto_created        = c.ExtendedInfo.value(N''(ExtendedInfo/AutoCreated)[1]'', N''bit''),
            qs_plan_count       = c.ExtendedInfo.value(N''(ExtendedInfo/QSPlanCount)[1]'', N''int''),
            qs_total_executions = c.ExtendedInfo.value(N''(ExtendedInfo/QSTotalExecutions)[1]'', N''bigint''),
            qs_total_cpu_ms     = c.ExtendedInfo.value(N''(ExtendedInfo/QSTotalCpuMs)[1]'', N''bigint''),
            effective_sample    = c.ExtendedInfo.value(N''(ExtendedInfo/EffectiveSamplePct)[1]'', N''int''),
            sample_source       = c.ExtendedInfo.value(N''(ExtendedInfo/SampleSource)[1]'', N''nvarchar(50)''),
            has_filter          = c.ExtendedInfo.value(N''(ExtendedInfo/HasFilter)[1]'', N''bit''),
            filtered_drift      = c.ExtendedInfo.value(N''(ExtendedInfo/FilteredDriftRatio)[1]'', N''float''),
            is_incremental      = c.ExtendedInfo.value(N''(ExtendedInfo/IsIncremental)[1]'', N''bit''),
            processing_position = c.ExtendedInfo.value(N''(ExtendedInfo/ProcessingPosition)[1]'', N''int''),
            object_id           = c.ExtendedInfo.value(N''(ExtendedInfo/ObjectId)[1]'', N''int''),
            stats_id            = c.ExtendedInfo.value(N''(ExtendedInfo/StatsId)[1]'', N''int'')
        FROM ' + @commandlog_ref + N' AS c
        WHERE c.CommandType = N''UPDATE_STATISTICS''
        AND   c.StartTime >= DATEADD(DAY, -@days_back, GETDATE());
        ';

        EXECUTE sys.sp_executesql
            @sql,
            N'@days_back integer',
            @days_back = @DaysBack;
    END;

    DECLARE @stat_update_count integer = (SELECT COUNT_BIG(*) FROM #stat_updates);

    RAISERROR(N'  Stat updates found: %i', 10, 1, @stat_update_count) WITH NOWAIT;

    IF @Debug = 1
    BEGIN
        SET @debug_elapsed_ms = DATEDIFF(MILLISECOND, @debug_timer, SYSDATETIME());
        DECLARE @debug_failed_stats integer = (SELECT COUNT_BIG(*) FROM #stat_updates WHERE ErrorNumber > 0);
        DECLARE @debug_null_runlabel integer = (SELECT COUNT_BIG(*) FROM #stat_updates WHERE RunLabel IS NULL);
        DECLARE @debug_distinct_dbs integer = (SELECT COUNT(DISTINCT DatabaseName) FROM #stat_updates);
        DECLARE @debug_distinct_tables integer = (SELECT COUNT(DISTINCT DatabaseName + N'.' + SchemaName + N'.' + ObjectName) FROM #stat_updates);
        RAISERROR(N'DEBUG: #stat_updates: %i failed, %i NULL RunLabel, %i databases, %i distinct tables', 10, 1,
            @debug_failed_stats, @debug_null_runlabel, @debug_distinct_dbs, @debug_distinct_tables) WITH NOWAIT;
        RAISERROR(N'DEBUG: Stat extraction complete (%i ms)', 10, 1, @debug_elapsed_ms) WITH NOWAIT;
    END;

    RAISERROR(N'', 10, 1) WITH NOWAIT;

    /*
    ============================================================================
    HANDLE EMPTY DATA
    ============================================================================
    */
    IF @run_count = 0 AND @stat_update_count = 0
    BEGIN
        RAISERROR(N'No sp_StatUpdate data found in CommandLog for the last %i days.', 10, 1, @DaysBack) WITH NOWAIT;

        INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
        VALUES
        (
            N'INFO',
            N'NO_DATA',
            N'No sp_StatUpdate entries found in CommandLog',
            N'Analysis window: ' + CONVERT(nvarchar(10), @DaysBack) + N' days. CommandLog: ' + @commandlog_ref,
            N'Run sp_StatUpdate with @LogToTable = N''Y'' (default) to populate CommandLog, then re-run this diagnostic.',
            N'EXECUTE dbo.sp_StatUpdate @Databases = N''USER_DATABASES'', @LogToTable = N''Y'';',
            99
        );

        /* Return result sets in expected format */
        IF @SingleResultSet = 0
        BEGIN
            /* RS 1: Executive Dashboard (empty state) */
            SELECT Category = N'OVERALL', Grade = 'F', Score = 0, Headline = N'No sp_StatUpdate data found in CommandLog.', Detail = N'Run sp_StatUpdate first, then re-run this diagnostic.', Trend = CONVERT(nvarchar(20), N'N/A') WHERE 1 = 1
            UNION ALL
            SELECT N'COMPLETION', 'F', 0, N'No data to evaluate.', NULL, NULL WHERE 1 = 0
            UNION ALL
            SELECT N'RELIABILITY', 'F', 0, N'No data to evaluate.', NULL, NULL WHERE 1 = 0
            UNION ALL
            SELECT N'SPEED', 'F', 0, N'No data to evaluate.', NULL, NULL WHERE 1 = 0
            UNION ALL
            SELECT N'WORKLOAD FOCUS', 'F', 0, N'No data to evaluate.', NULL, NULL WHERE 1 = 0;
            /* RS 2: Recommendations */
            SELECT Severity, Category, Finding, Evidence, Recommendation, ExampleCall,
                Summary = N'[' + Severity + N'] ' + Finding
                    + CASE WHEN Recommendation IS NOT NULL THEN N' -- ' + LEFT(Recommendation, 200) ELSE N'' END
                    + CASE WHEN ExampleCall IS NOT NULL THEN N' Fix: ' + ExampleCall ELSE N'' END
            FROM #recommendations ORDER BY SortPriority, FindingID;
            IF @ExpertMode = 1
            BEGIN
                /* RS 3-12 empty schemas for ExpertMode -- must match production column lists */
                SELECT TotalRuns = CONVERT(bigint, 0), CompletedRuns = CONVERT(bigint, 0), KilledRuns = CONVERT(bigint, 0), CompletionPct = CONVERT(decimal(5,1), 0), TimeLimitedRuns = CONVERT(bigint, 0), NaturalEndRuns = CONVERT(bigint, 0), AvgDurationSec = CONVERT(bigint, NULL), AvgStatsProcessed = CONVERT(bigint, NULL), AvgStatsRemaining = CONVERT(bigint, NULL), TotalStatUpdates = CONVERT(bigint, 0), TotalFailedUpdates = CONVERT(bigint, 0), AnalysisWindowDays = CONVERT(int, @DaysBack), StopReasonDistribution = CONVERT(nvarchar(max), NULL), HealthScore = CONVERT(int, NULL), QSRunCount = CONVERT(bigint, 0), AvgWorkloadCoveragePct = CONVERT(decimal(5,1), NULL), LatestHighCpuFirstQuartilePct = CONVERT(decimal(5,1), NULL), ReplicaRole = CONVERT(nvarchar(20), NULL);
                SELECT * FROM #runs WHERE 1 = 0;
                SELECT * FROM #stat_updates WHERE 1 = 0;
                SELECT * FROM #stat_updates WHERE 1 = 0;
                SELECT * FROM #stat_updates WHERE 1 = 0;
                SELECT * FROM #runs WHERE 1 = 0;
                IF @Obfuscate = 1 SELECT * FROM #obfuscation_map;
                /* RS 10-12 empty schemas */
                SELECT WeekStart = CONVERT(date, NULL), WeekLabel = CONVERT(nvarchar(5), NULL), RunCount = CONVERT(bigint, NULL), SortStrategy = CONVERT(nvarchar(20), NULL), HighCpuFirstQuartilePct = CONVERT(decimal(5,1), NULL), AvgMinutesToHighCpu = CONVERT(decimal(10,1), NULL), WorkloadCoveragePct = CONVERT(decimal(5,1), NULL), CompletionPct = CONVERT(decimal(5,1), NULL), AvgSecPerStat = CONVERT(decimal(10,1), NULL), TrendDirection = CONVERT(nvarchar(20), NULL), Top5ConcentrationPct = CONVERT(decimal(5,1), NULL) WHERE 1 = 0;
                SELECT RunLabel = CONVERT(nvarchar(100), NULL), StartTime = CONVERT(datetime2(3), NULL), SortStrategy = CONVERT(nvarchar(20), NULL), StatsFound = CONVERT(int, NULL), StatsProcessed = CONVERT(int, NULL), CompletionPct = CONVERT(decimal(5,1), NULL), HighCpuInFirstQuartilePct = CONVERT(decimal(5,1), NULL), MinutesToHighCpuComplete = CONVERT(decimal(10,1), NULL), WorkloadCoveragePct = CONVERT(decimal(5,1), NULL), AvgSecPerStat = CONVERT(decimal(10,1), NULL), StopReason = CONVERT(nvarchar(50), NULL), DeltaVsPrior = CONVERT(nvarchar(100), NULL) WHERE 1 = 0;
                SELECT WorkloadRank = CONVERT(bigint, NULL), DatabaseName = CONVERT(sysname, NULL), SchemaName = CONVERT(sysname, NULL), TableName = CONVERT(sysname, NULL), StatisticsName = CONVERT(sysname, NULL), ProcessingPosition = CONVERT(int, NULL), TotalQueryCpuMs = CONVERT(bigint, NULL), TotalExecutions = CONVERT(bigint, NULL), PlanCount = CONVERT(int, NULL), UpdateDurationMs = CONVERT(int, NULL), QualifyReason = CONVERT(nvarchar(100), NULL), WorkloadRankPct = CONVERT(decimal(5,1), NULL), CumulativeCpuPct = CONVERT(decimal(5,1), NULL) WHERE 1 = 0;
                /* RS 13 empty schema */
                SELECT DatabaseName = CONVERT(sysname, NULL), SchemaName = CONVERT(sysname, NULL), TableName = CONVERT(sysname, NULL), StatisticsName = CONVERT(sysname, NULL), Appearances = CONVERT(bigint, NULL), FirstRunDate = CONVERT(datetime2(3), NULL), LastRunDate = CONVERT(datetime2(3), NULL), FirstCpuMs = CONVERT(bigint, NULL), LastCpuMs = CONVERT(bigint, NULL), CpuChangePct = CONVERT(decimal(10,1), NULL), CpuTrend = CONVERT(nvarchar(20), NULL), FirstCpuPerExec = CONVERT(decimal(18,4), NULL), LastCpuPerExec = CONVERT(decimal(18,4), NULL), CpuPerExecChangePct = CONVERT(decimal(10,1), NULL), FirstExecs = CONVERT(bigint, NULL), LastExecs = CONVERT(bigint, NULL), FirstPlans = CONVERT(int, NULL), LastPlans = CONVERT(int, NULL), ForcedPlanCount = CONVERT(int, NULL), PlanTrend = CONVERT(nvarchar(50), NULL) WHERE 1 = 0;
            END;
        END
        ELSE
        BEGIN
            /* Single result set mode: return Executive Dashboard + NO_DATA recommendation row */
            SELECT
                ResultSetID   = 1,
                ResultSetName = N'Executive Dashboard',
                RowNum        = 1,
                RowData       = (SELECT Category = N'OVERALL', Grade = 'F', Score = 0, Headline = N'No sp_StatUpdate data found in CommandLog.', Detail = N'Run sp_StatUpdate first.', Trend = N'N/A' FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
            UNION ALL
            SELECT
                ResultSetID   = 2,
                ResultSetName = N'Recommendations',
                RowNum        = 1,
                RowData       = (
                    SELECT
                        Severity       = r.Severity,
                        Category       = r.Category,
                        Finding        = r.Finding,
                        Evidence       = r.Evidence,
                        Recommendation = r.Recommendation,
                        ExampleCall    = r.ExampleCall
                    FROM #recommendations AS r
                    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                )
            FROM #recommendations;
        END;

        RETURN;
    END;

    /*
    ============================================================================
    OBFUSCATION PASS
    ============================================================================
    */
    IF @Obfuscate = 1
    BEGIN
        RAISERROR(N'Applying obfuscation...', 10, 1) WITH NOWAIT;

        /* Seed prefix: when @ObfuscationSeed is provided, prepend to all names before hashing */
        DECLARE @seed_prefix nvarchar(128) = ISNULL(@ObfuscationSeed, N'');

        /* Snapshot distinct names BEFORE obfuscating */
        INSERT INTO #obfuscation_map (ObjectType, OriginalName, ObfuscatedName)
        SELECT DISTINCT
            object_type = N'Database',
            original_name = su.DatabaseName,
            obfuscated_name = N'DB_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.DatabaseName), 2), 6)
        FROM #stat_updates AS su
        WHERE su.DatabaseName IS NOT NULL
        UNION
        SELECT DISTINCT
            N'Schema',
            su.SchemaName,
            N'SCH_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.SchemaName), 2), 4)
        FROM #stat_updates AS su
        WHERE su.SchemaName IS NOT NULL
        UNION
        /* BUG-08 fix: hash uses fully-qualified name (DB.Schema.Table) to prevent cross-DB collision.
           OriginalName stored as qualified name for disambiguation in RS8. */
        SELECT DISTINCT
            N'Table',
            su.DatabaseName + N'.' + su.SchemaName + N'.' + su.ObjectName,
            N'TBL_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.DatabaseName + N'.' + su.SchemaName + N'.' + su.ObjectName), 2), 6)
        FROM #stat_updates AS su
        WHERE su.ObjectName IS NOT NULL
        UNION
        SELECT DISTINCT
            N'Statistic',
            su.StatisticsName,
            CASE
                WHEN su.StatisticsName LIKE N'_WA_Sys_%' THEN N'_WA_Sys_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.StatisticsName), 2), 6)
                WHEN su.StatisticsName LIKE N'PK_%'       THEN N'PK_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.StatisticsName), 2), 6)
                WHEN su.StatisticsName LIKE N'IX_%'       THEN N'IX_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.StatisticsName), 2), 6)
                WHEN su.StatisticsName LIKE N'UQ_%'       THEN N'UQ_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.StatisticsName), 2), 6)
                ELSE N'STAT_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.StatisticsName), 2), 6)
            END
        FROM #stat_updates AS su
        WHERE su.StatisticsName IS NOT NULL;

        /* Also snapshot databases from #runs.
           BUG-09: Known keywords (USER_DATABASES, SYSTEM_DATABASES, ALL_DATABASES) are left
           unobfuscated because they contain no sensitive names. For comma-separated lists,
           the entire string is hashed as one unit -- individual database names within the list
           cannot be reverse-mapped to their per-stat-update obfuscated counterparts. This is
           a known limitation; the obfuscation map entry for such runs is marked "(multi-DB list)". */
        INSERT INTO #obfuscation_map (ObjectType, OriginalName, ObfuscatedName)
        SELECT DISTINCT
            N'RunDatabase',
            r.[Databases],
            N'DB_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + r.[Databases]), 2), 6)
        FROM #runs AS r
        WHERE r.[Databases] IS NOT NULL
        AND   r.[Databases] NOT IN (N'USER_DATABASES', N'SYSTEM_DATABASES', N'ALL_DATABASES', N'AVAILABILITY_GROUP_DATABASES')  /* BUG-09: leave keywords unobfuscated */
        AND   NOT EXISTS (SELECT 1 FROM #obfuscation_map AS m WHERE m.OriginalName = r.[Databases] AND m.ObjectType = N'Database');

        /* Apply obfuscation to #stat_updates */
        UPDATE su
        SET
            su.DatabaseName = N'DB_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.DatabaseName), 2), 6),
            su.SchemaName = N'SCH_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.SchemaName), 2), 4),
            /* BUG-08 fix: hash uses pre-update DatabaseName+SchemaName+ObjectName composite key;
               SQL Server evaluates all RHS before applying SET, so original values are used here. */
            su.ObjectName = N'TBL_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.DatabaseName + N'.' + su.SchemaName + N'.' + su.ObjectName), 2), 6),
            su.StatisticsName = CASE
                WHEN su.StatisticsName LIKE N'_WA_Sys_%' THEN N'_WA_Sys_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.StatisticsName), 2), 6)
                WHEN su.StatisticsName LIKE N'PK_%'       THEN N'PK_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.StatisticsName), 2), 6)
                WHEN su.StatisticsName LIKE N'IX_%'       THEN N'IX_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.StatisticsName), 2), 6)
                WHEN su.StatisticsName LIKE N'UQ_%'       THEN N'UQ_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.StatisticsName), 2), 6)
                ELSE N'STAT_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.StatisticsName), 2), 6)
            END
        FROM #stat_updates AS su;

        /* Obfuscate #runs.Databases -- skip known keywords (BUG-09: they contain no sensitive names) */
        UPDATE r
        SET r.[Databases] = N'DB_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + r.[Databases]), 2), 6)
        FROM #runs AS r
        WHERE r.[Databases] IS NOT NULL
        AND   r.[Databases] NOT IN (N'USER_DATABASES', N'SYSTEM_DATABASES', N'ALL_DATABASES', N'AVAILABILITY_GROUP_DATABASES');

        /* Obfuscate RunLabels (contain server names) */
        UPDATE r
        SET r.RunLabel = N'RUN_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + r.RunLabel), 2), 8)
        FROM #runs AS r;

        UPDATE su
        SET su.RunLabel = N'RUN_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.RunLabel), 2), 8)
        FROM #stat_updates AS su
        WHERE su.RunLabel IS NOT NULL;

        DECLARE @obfuscation_count integer = (SELECT COUNT_BIG(*) FROM #obfuscation_map);
        RAISERROR(N'  Obfuscated %i mapping entries', 10, 1, @obfuscation_count) WITH NOWAIT;

        /* Persist obfuscation map to table if requested */
        IF @ObfuscationMapTable IS NOT NULL
        BEGIN
            /* Build safe table reference from PARSENAME to prevent SQL injection.
               Supports 1-part (dbo assumed), 2-part (schema.table), or 3-part (db.schema.table). */
            DECLARE
                @map_part1 sysname = PARSENAME(@ObfuscationMapTable, 1),  /* table */
                @map_part2 sysname = PARSENAME(@ObfuscationMapTable, 2),  /* schema */
                @map_part3 sysname = PARSENAME(@ObfuscationMapTable, 3),  /* database */
                @map_safe_name nvarchar(500);

            IF @map_part1 IS NULL
            BEGIN
                RAISERROR(N'ERROR: @ObfuscationMapTable has invalid table name: %s', 16, 1, @ObfuscationMapTable) WITH NOWAIT;
            END
            ELSE
            BEGIN
                SET @map_safe_name = ISNULL(QUOTENAME(@map_part3) + N'.', N'')
                    + ISNULL(QUOTENAME(@map_part2) + N'.', N'')
                    + QUOTENAME(@map_part1);

                DECLARE @map_sql nvarchar(max);

                SET @map_sql = N'
                    IF OBJECT_ID(' + QUOTENAME(@ObfuscationMapTable, '''') + N') IS NULL
                    BEGIN
                        CREATE TABLE ' + @map_safe_name + N' (
                            ObjectType     nvarchar(20)   NOT NULL,
                            OriginalName   nvarchar(256)  NOT NULL,
                            ObfuscatedName nvarchar(50)   NOT NULL,
                            CapturedAt     datetime2      NOT NULL DEFAULT SYSDATETIME()
                        );
                    END;

                    INSERT INTO ' + @map_safe_name + N' (ObjectType, OriginalName, ObfuscatedName)
                    SELECT m.ObjectType, m.OriginalName, m.ObfuscatedName
                    FROM #obfuscation_map AS m
                    WHERE NOT EXISTS (
                        SELECT 1 FROM ' + @map_safe_name + N' AS t
                        WHERE t.OriginalName = m.OriginalName
                        AND   t.ObjectType   = m.ObjectType
                    );';

                BEGIN TRY
                    EXECUTE sp_executesql @map_sql;

                    SET @map_safe_name = N'  Obfuscation map saved to ' + @map_safe_name;
                    RAISERROR(@map_safe_name, 10, 1) WITH NOWAIT;
                END TRY
                BEGIN CATCH
                    /* INSERT...EXEC context prevents writes to persistent tables.
                       This is expected when caller does INSERT INTO #t EXEC sp_StatUpdate_Diag @SingleResultSet=1. */
                    RAISERROR(N'  WARNING: Could not write to @ObfuscationMapTable (INSERT...EXEC context). Run without INSERT...EXEC to persist the map.', 10, 1) WITH NOWAIT;
                END CATCH;

                RAISERROR(N'', 10, 1) WITH NOWAIT;
                RAISERROR(N'=== Decode obfuscated tokens ===', 10, 1) WITH NOWAIT;
                RAISERROR(N'SELECT ObjectType, OriginalName, ObfuscatedName', 10, 1) WITH NOWAIT;
                RAISERROR(N'FROM %s WHERE ObfuscatedName = N''<paste_token_here>'';', 10, 1, @ObfuscationMapTable) WITH NOWAIT;
            END;
        END;

        RAISERROR(N'', 10, 1) WITH NOWAIT;
    END;

    IF @Debug = 1
    BEGIN
        RAISERROR(N'DEBUG: #runs sample (top 5)', 10, 1) WITH NOWAIT;
        SELECT TOP (5) * FROM #runs ORDER BY StartTime DESC;

        RAISERROR(N'DEBUG: #stat_updates sample (top 5)', 10, 1) WITH NOWAIT;
        SELECT TOP (5) * FROM #stat_updates ORDER BY StartTime DESC;
    END;

    /*
    ============================================================================
    FORCED PLAN INVENTORY (#292): Lightweight cross-database lookup for forced plans
    on objects appearing in QS correlation data. Feeds RS 13 ForcedPlanCount column,
    I8 forced-plan-at-risk recommendation, and enhanced PlanTrend labels.
    ============================================================================
    */
    CREATE TABLE #forced_plans
    (
        DatabaseName sysname NOT NULL,
        ObjectName sysname NOT NULL,
        ForcedPlanCount integer NOT NULL DEFAULT 0,
        CONSTRAINT PK_forced_plans PRIMARY KEY CLUSTERED (DatabaseName, ObjectName)
    );

    BEGIN TRY
        DECLARE
            @fp_db sysname,
            @fp_sql nvarchar(max);

        DECLARE fp_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT DISTINCT su.DatabaseName
            FROM #stat_updates AS su
            WHERE su.QSTotalCpuMs IS NOT NULL
            AND   su.QSTotalCpuMs > 0;

        OPEN fp_cursor;
        FETCH NEXT FROM fp_cursor INTO @fp_db;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            /* Query each database's QS catalog for forced plans on objects in our data set.
               Joins query_store_plan -> query_store_query -> sys.objects to map plan->object_id.
               Skip if database doesn't exist (common with test data or obfuscated names).
               TRY/CATCH per-database: if QS is disabled or DB is inaccessible, skip silently. */
            IF DB_ID(@fp_db) IS NOT NULL
            BEGIN
            SET @fp_sql = N'
                BEGIN TRY
                    INSERT INTO #forced_plans (DatabaseName, ObjectName, ForcedPlanCount)
                    SELECT
                        @db_name,
                        o.name,
                        COUNT_BIG(*)
                    FROM ' + QUOTENAME(@fp_db) + N'.sys.query_store_plan AS qsp
                    INNER JOIN ' + QUOTENAME(@fp_db) + N'.sys.query_store_query AS qsq
                        ON qsq.query_id = qsp.query_id
                    INNER JOIN ' + QUOTENAME(@fp_db) + N'.sys.objects AS o
                        ON o.object_id = qsq.object_id
                    WHERE qsp.is_forced_plan = 1
                    AND   o.name IN (SELECT DISTINCT su2.ObjectName FROM #stat_updates AS su2 WHERE su2.DatabaseName = @db_name AND su2.QSTotalCpuMs IS NOT NULL)
                    GROUP BY o.name;
                END TRY
                BEGIN CATCH
                    /* QS disabled, DB offline, permissions -- skip silently */
                END CATCH;';

            EXECUTE sys.sp_executesql @fp_sql, N'@db_name sysname', @db_name = @fp_db;

            IF @Debug = 1
            BEGIN
                DECLARE @fp_count_msg nvarchar(200) = N'  Forced plans in ' + @fp_db + N': '
                    + CONVERT(nvarchar(10), ISNULL((SELECT SUM(ForcedPlanCount) FROM #forced_plans WHERE DatabaseName = @fp_db), 0))
                    + N' across '
                    + CONVERT(nvarchar(10), (SELECT COUNT(*) FROM #forced_plans WHERE DatabaseName = @fp_db))
                    + N' object(s)';
                RAISERROR(@fp_count_msg, 10, 1) WITH NOWAIT;
            END;
            END; /* END IF DB_ID(@fp_db) IS NOT NULL */

            FETCH NEXT FROM fp_cursor INTO @fp_db;
        END;

        CLOSE fp_cursor;
        DEALLOCATE fp_cursor;
    END TRY
    BEGIN CATCH
        /* Graceful fallback: #forced_plans stays empty, RS 13 works without it */
        DECLARE @fp_error nvarchar(4000) = ERROR_MESSAGE();

        IF CURSOR_STATUS('local', 'fp_cursor') >= 0
        BEGIN
            CLOSE fp_cursor;
            DEALLOCATE fp_cursor;
        END;

        IF @Debug = 1
            RAISERROR(N'  Forced plan lookup failed (non-fatal): %s', 10, 1, @fp_error) WITH NOWAIT;
    END CATCH;

    /*
    ============================================================================
    WORKLOAD IMPACT STAGING (#255, #260, #259, #261, #263, #264, #265)
    Per-object workload metrics from QS data in #stat_updates.
    ============================================================================
    */
    CREATE TABLE #workload_impact
    (
        DatabaseName sysname NOT NULL,
        SchemaName sysname NOT NULL,
        ObjectName sysname NOT NULL,
        TotalCpuMs bigint NOT NULL DEFAULT 0,
        ImpactScore decimal(5, 2) NULL,     /* 0.00-1.00 percentile rank */
        CumulativePct decimal(5, 1) NULL,   /* running SUM / total */
        IsVolatile bit NOT NULL DEFAULT 0   /* #259: MAX/MIN QSTotalCpuMs > 10x across runs */
    );

    /* Populate from latest run per stat, grouped by object */
    ;WITH latest_stat_cpu AS
    (
        SELECT
            su.DatabaseName,
            su.SchemaName,
            su.ObjectName,
            su.StatisticsName,
            su.QSTotalCpuMs,
            rn = ROW_NUMBER() OVER (
                PARTITION BY
                    ISNULL(CONVERT(nvarchar(20), su.ObjectId), su.DatabaseName + N'.' + su.SchemaName + N'.' + su.ObjectName),
                    ISNULL(CONVERT(nvarchar(20), su.StatsId), su.StatisticsName)
                ORDER BY su.StartTime DESC
            )
        FROM #stat_updates AS su
        WHERE su.QSTotalCpuMs IS NOT NULL
        AND   (su.ErrorNumber = 0 OR su.ErrorNumber IS NULL)
    ),
    object_cpu AS
    (
        SELECT
            DatabaseName,
            SchemaName,
            ObjectName,
            TotalCpuMs = SUM(ISNULL(QSTotalCpuMs, 0))
        FROM latest_stat_cpu
        WHERE rn = 1
        GROUP BY DatabaseName, SchemaName, ObjectName
        HAVING SUM(ISNULL(QSTotalCpuMs, 0)) > 0
    )
    INSERT INTO #workload_impact (DatabaseName, SchemaName, ObjectName, TotalCpuMs, ImpactScore, CumulativePct)
    SELECT
        oc.DatabaseName,
        oc.SchemaName,
        oc.ObjectName,
        oc.TotalCpuMs,
        ImpactScore  = CONVERT(decimal(5, 2), PERCENT_RANK() OVER (ORDER BY oc.TotalCpuMs)),
        CumulativePct = CONVERT(decimal(5, 1), 100.0 *
            SUM(oc.TotalCpuMs) OVER (ORDER BY oc.TotalCpuMs DESC ROWS UNBOUNDED PRECEDING)
            / NULLIF(SUM(oc.TotalCpuMs) OVER (), 0))
    FROM object_cpu AS oc;

    /* #259: Mark volatile stats -- QSTotalCpuMs varies > 10x across runs */
    ;WITH stat_volatility AS
    (
        SELECT
            su.DatabaseName,
            su.SchemaName,
            su.ObjectName,
            IsVolatile = CASE
                WHEN MAX(su.QSTotalCpuMs) > 10 * NULLIF(MIN(su.QSTotalCpuMs), 0) THEN 1
                ELSE 0
            END
        FROM #stat_updates AS su
        WHERE su.QSTotalCpuMs IS NOT NULL
        AND   (su.ErrorNumber = 0 OR su.ErrorNumber IS NULL)
        GROUP BY su.DatabaseName, su.SchemaName, su.ObjectName
        HAVING COUNT(DISTINCT su.RunLabel) >= 2
    )
    UPDATE wi
    SET wi.IsVolatile = sv.IsVolatile
    FROM #workload_impact AS wi
    INNER JOIN stat_volatility AS sv
        ON sv.DatabaseName = wi.DatabaseName
        AND sv.SchemaName  = wi.SchemaName
        AND sv.ObjectName  = wi.ObjectName
    WHERE sv.IsVolatile = 1;

    DECLARE @workload_impact_count integer = (SELECT COUNT_BIG(*) FROM #workload_impact);
    IF @Debug = 1
        RAISERROR(N'  Workload impact: %i objects with QS data', 10, 1, @workload_impact_count) WITH NOWAIT;

    /*
    ============================================================================
    DIAGNOSTIC CHECKS
    ============================================================================
    */
    RAISERROR(N'Running diagnostic checks...', 10, 1) WITH NOWAIT;

    /* ======================================================================
       C1: KILLED RUNS (START without matching END)
       ====================================================================== */
    IF EXISTS (SELECT 1 FROM #runs WHERE IsKilled = 1)
    BEGIN
        INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
        SELECT
            N'CRITICAL',
            N'KILLED_RUNS',
            N'Found ' + CONVERT(nvarchar(10), COUNT_BIG(*)) + N' killed run(s) in the last '
                + CONVERT(nvarchar(10), @DaysBack) + N' days',
            N'Run dates: ' + STRING_AGG(CONVERT(nvarchar(20), r.StartTime, 120), N', ')
                WITHIN GROUP (ORDER BY r.StartTime DESC),
            N'Killed runs leave orphaned START markers and can indicate: '
                + N'(1) SQL Agent job killed or timed out, '
                + N'(2) Server restart during maintenance, '
                + N'(3) Manual cancellation. '
                + N'Enable @CleanupOrphanedRuns = N''Y'' (default in v1.9+). '
                + N'Review SQL Agent job history for stop events. '
                + N'Consider adjusting @TimeLimit to complete within your maintenance window.',
            N'EXECUTE dbo.sp_StatUpdate @Databases = N''USER_DATABASES'', @CleanupOrphanedRuns = N''Y'', @TimeLimit = 3600;',
            10
        FROM #runs AS r
        WHERE r.IsKilled = 1;

        RAISERROR(N'  [CRITICAL] C1: Killed runs detected', 10, 1) WITH NOWAIT;
    END;

    /* ======================================================================
       C2: REPEATED FAILURES (same stat failing consistently)
       ====================================================================== */
    INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
    SELECT
        N'CRITICAL',
        N'REPEATED_FAILURES',
        N'Statistic fails consistently: ' + su.DatabaseName + N'.'
            + su.SchemaName + N'.' + su.ObjectName + N'.' + su.StatisticsName,
        N'Failed in ' + CONVERT(nvarchar(10), COUNT(DISTINCT su.RunLabel))
            + N' run(s). Errors: ' + (
                SELECT STRING_AGG(err_num, N', ')
                FROM (SELECT DISTINCT CONVERT(nvarchar(20), su2.ErrorNumber) AS err_num
                      FROM #stat_updates AS su2
                      WHERE su2.ErrorNumber > 0
                      AND   su2.DatabaseName = su.DatabaseName
                      AND   su2.SchemaName = su.SchemaName
                      AND   su2.ObjectName = su.ObjectName
                      AND   su2.StatisticsName = su.StatisticsName) AS distinct_errors
            ),
        N'Investigate the specific error. Common causes: '
            + N'lock contention (Error 1222 - use @LockTimeout), '
            + N'schema changes mid-maintenance, '
            + N'or corrupt statistics (DROP + CREATE). '
            + N'Most common error: ' + LEFT(MAX(su.ErrorMessage), 200),
        N'EXECUTE dbo.sp_StatUpdate @LockTimeout = 30, @FailFast = 0;',
        10
    FROM #stat_updates AS su
    WHERE su.ErrorNumber > 0
    GROUP BY su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName
    HAVING COUNT(DISTINCT su.RunLabel) >= @FailureThreshold;

    IF @@ROWCOUNT > 0
        RAISERROR(N'  [CRITICAL] C2: Repeated failures detected', 10, 1) WITH NOWAIT;

    /* ======================================================================
       C3: TIME LIMIT EXHAUSTION
       ====================================================================== */
    DECLARE
        @total_completed_runs integer = (SELECT COUNT_BIG(*) FROM #runs WHERE IsKilled = 0),
        @time_limit_runs integer = (SELECT COUNT_BIG(*) FROM #runs WHERE StopReason = N'TIME_LIMIT'),
        /* BUG-12: use latest run's TimeLimit for the recommendation, not MAX across all TIME_LIMIT runs.
           Using MAX of historical TIME_LIMIT runs could recommend an extreme value from a one-off test run. */
        @latest_timelimit_sec integer = (SELECT TOP 1 TimeLimit FROM #runs WHERE IsKilled = 0 ORDER BY StartTime DESC);

    IF @total_completed_runs > 0
    AND @time_limit_runs * 100.0 / @total_completed_runs >= @TimeLimitExhaustionPct
    BEGIN
        INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
        SELECT
            N'CRITICAL',
            N'TIME_LIMIT_EXHAUSTION',
            CONVERT(nvarchar(10), @time_limit_runs) + N' of '
                + CONVERT(nvarchar(10), @total_completed_runs) + N' completed runs hit TIME_LIMIT ('
                + CONVERT(nvarchar(10), CONVERT(integer, @time_limit_runs * 100.0 / @total_completed_runs)) + N'%)',
            N'Average stats remaining when time-limited: ' + CONVERT(nvarchar(10), AVG(r.StatsRemaining))
                /* BUG-12 fix: show latest run's TimeLimit instead of AVG across TIME_LIMIT runs */
                + N'. Current time limit (latest run): ' + ISNULL(CONVERT(nvarchar(10), @latest_timelimit_sec), N'NULL') + N' seconds ('
                + ISNULL(CONVERT(nvarchar(10), @latest_timelimit_sec / 60), N'NULL') + N' minutes)'
                /* #265: Append workload impact context when QS data available */
                + ISNULL((
                    SELECT N'. ' + CONVERT(nvarchar(10), COUNT_BIG(*))
                        + N' of top-10 workload stats were not reached before time limit'
                    FROM #workload_impact AS wi
                    WHERE wi.ImpactScore >= 0.90
                    AND   NOT EXISTS (
                        SELECT 1 FROM #stat_updates AS su_c3
                        WHERE su_c3.DatabaseName = wi.DatabaseName
                        AND   su_c3.SchemaName   = wi.SchemaName
                        AND   su_c3.ObjectName   = wi.ObjectName
                        AND   su_c3.RunLabel IN (SELECT r2.RunLabel FROM #runs AS r2 WHERE r2.StopReason = N'TIME_LIMIT')
                        AND   (su_c3.ErrorNumber = 0 OR su_c3.ErrorNumber IS NULL)
                    )
                    HAVING COUNT_BIG(*) > 0
                ), N''),
            N'Stats are consistently not finishing within the time limit. Options: '
                + N'(1) Increase @TimeLimit to ' + ISNULL(CONVERT(nvarchar(10), @latest_timelimit_sec * 2), N'<current_value_x2>') + N' seconds, '
                + N'(2) Enable @LongRunningThresholdMinutes to cap slow individual stats, '
                + N'(3) Raise @ModificationThreshold to reduce qualifying stats, '
                + N'(4) Use @Preset = N''NIGHTLY_MAINTENANCE'' for balanced defaults.',
            N'EXECUTE dbo.sp_StatUpdate @Databases = N''USER_DATABASES'', @TimeLimit = '
                + ISNULL(CONVERT(nvarchar(10), @latest_timelimit_sec * 2), N'<current_value_x2>')
                + N', @LongRunningThresholdMinutes = 30;',
            15
        FROM #runs AS r
        WHERE r.StopReason = N'TIME_LIMIT';

        RAISERROR(N'  [CRITICAL] C3: Time limit exhaustion detected', 10, 1) WITH NOWAIT;
    END;

    /* ======================================================================
       C4: DEGRADING THROUGHPUT
       ====================================================================== */
    /* #306: Compare only runs with same StatsInParallel mode to avoid false CRITICAL
       when switching between serial and parallel modes changes throughput characteristics */
    ;WITH throughput_windows AS
    (
        SELECT
            window_label = CASE
                WHEN r.StartTime >= DATEADD(DAY, -@ThroughputWindowDays, GETDATE()) THEN N'RECENT'
                ELSE N'PRIOR'
            END,
            parallel_mode = ISNULL(r.StatsInParallel, N'N'),
            avg_sec_per_stat = CASE
                WHEN r.StatsProcessed > 0
                THEN r.DurationSeconds * 1.0 / r.StatsProcessed
                ELSE NULL
            END
        FROM #runs AS r
        WHERE r.IsKilled = 0
        AND   r.StatsProcessed > 0
    ),
    window_avgs AS
    (
        SELECT
            window_label,
            parallel_mode,
            avg_sec = AVG(avg_sec_per_stat),
            run_count = COUNT_BIG(*)
        FROM throughput_windows
        GROUP BY window_label, parallel_mode
    )
    INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
    SELECT
        /* If modes differ between recent/prior windows, downgrade to INFO */
        CASE WHEN recent_w.parallel_mode = prior_w.parallel_mode THEN N'CRITICAL' ELSE N'INFO' END,
        N'DEGRADING_THROUGHPUT',
        CASE WHEN recent_w.parallel_mode = prior_w.parallel_mode
            THEN N'Throughput degraded: recent avg ' + CONVERT(nvarchar(20), CONVERT(decimal(10, 1), recent_w.avg_sec))
                + N' sec/stat vs. prior ' + CONVERT(nvarchar(20), CONVERT(decimal(10, 1), prior_w.avg_sec))
                + N' sec/stat (' + CONVERT(nvarchar(20), CONVERT(bigint, CASE WHEN recent_w.avg_sec / prior_w.avg_sec > 1000 THEN 99900 ELSE (recent_w.avg_sec / prior_w.avg_sec - 1) * 100 END))
                + N'% slower)'
            ELSE N'Throughput changed but parallelism mode also changed (StatsInParallel: '
                + prior_w.parallel_mode + N' -> ' + recent_w.parallel_mode
                + N'). Recent avg ' + CONVERT(nvarchar(20), CONVERT(decimal(10, 1), recent_w.avg_sec))
                + N' sec/stat vs. prior ' + CONVERT(nvarchar(20), CONVERT(decimal(10, 1), prior_w.avg_sec))
                + N' sec/stat -- not directly comparable.'
        END,
        N'Recent window: ' + CONVERT(nvarchar(10), recent_w.run_count) + N' runs averaging '
            + CONVERT(nvarchar(20), CONVERT(decimal(10, 1), recent_w.avg_sec)) + N' sec/stat'
            + N' (StatsInParallel=' + recent_w.parallel_mode + N'). '
            + N'Prior window: ' + CONVERT(nvarchar(10), prior_w.run_count) + N' runs averaging '
            + CONVERT(nvarchar(20), CONVERT(decimal(10, 1), prior_w.avg_sec)) + N' sec/stat'
            + N' (StatsInParallel=' + prior_w.parallel_mode + N'). '
            + N'Comparison window: ' + CONVERT(nvarchar(10), @ThroughputWindowDays) + N' days.',
        N'Possible causes: (1) Table growth increasing update cost, '
            + N'(2) I/O subsystem degradation, '
            + N'(3) Concurrent workload pressure, '
            + N'(4) Sample rate too high for growing tables. '
            + N'Consider: @StatsInParallel for throughput, '
            + N'@LongRunningThresholdMinutes to cap outliers, '
            + N'or investigate storage performance.',
        N'EXECUTE dbo.sp_StatUpdate @Databases = N''USER_DATABASES'', @StatsInParallel = N''Y'', @LongRunningThresholdMinutes = 15;',
        20
    FROM window_avgs AS recent_w
    CROSS JOIN window_avgs AS prior_w
    WHERE recent_w.window_label = N'RECENT'
    AND   prior_w.window_label = N'PRIOR'
    AND   prior_w.avg_sec > 0
    AND   recent_w.avg_sec > prior_w.avg_sec * 1.5
    AND   prior_w.run_count >= 2
    AND   recent_w.run_count >= 1
    /* When modes match, compare within same mode. When modes differ, still report but as INFO */
    AND   (recent_w.parallel_mode = prior_w.parallel_mode
        OR NOT EXISTS (SELECT 1 FROM window_avgs AS w2
                       WHERE w2.window_label = N'PRIOR'
                       AND   w2.parallel_mode = recent_w.parallel_mode));

    IF @@ROWCOUNT > 0
        RAISERROR(N'  [CRITICAL] C4: Degrading throughput detected', 10, 1) WITH NOWAIT;

    /* ======================================================================
       W1: SUBOPTIMAL PARAMETERS (checks against most recent run)
       ====================================================================== */
    DECLARE @latest_run_label nvarchar(100) = (SELECT TOP (1) RunLabel FROM #runs ORDER BY StartTime DESC);

    IF @latest_run_label IS NOT NULL
    BEGIN
        DECLARE
            @latest_tiered nvarchar(10),
            @latest_time_limit integer,
            @latest_mod_threshold bigint,
            @latest_qs_priority nvarchar(1),
            @latest_parallel nvarchar(1),
            @latest_preset nvarchar(30),
            @latest_long_run_min integer,
            @latest_group_join nvarchar(1),
            @latest_sort_order nvarchar(50);

        SELECT
            @latest_tiered = TieredThresholds,
            @latest_time_limit = TimeLimit,
            @latest_mod_threshold = ModificationThreshold,
            @latest_qs_priority = QueryStorePriority,
            @latest_parallel = StatsInParallel,
            @latest_preset = Preset,
            @latest_long_run_min = LongRunningThresholdMinutes,
            @latest_group_join = GroupByJoinPattern,
            @latest_sort_order = SortOrder
        FROM #runs
        WHERE RunLabel = @latest_run_label;

        /* W1a: TieredThresholds disabled */
        IF @latest_tiered IN (N'0', N'False')
        BEGIN
            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES
            (
                N'WARNING', N'SUBOPTIMAL_PARAMS',
                N'TieredThresholds is disabled',
                N'Current: @TieredThresholds = 0. Fixed threshold of ' + ISNULL(CONVERT(nvarchar(20), @latest_mod_threshold), N'NULL') + N' applied uniformly to all table sizes.',
                N'Tiered thresholds adapt the modification threshold based on table size (small tables need fewer mods to justify an update). This is especially important for mixed workloads with tables ranging from hundreds to millions of rows.',
                N'EXECUTE dbo.sp_StatUpdate @TieredThresholds = 1;',
                30
            );
        END;

        /* W1b: No TimeLimit */
        IF @latest_time_limit IS NULL
        BEGIN
            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES
            (
                N'WARNING', N'SUBOPTIMAL_PARAMS',
                N'No time limit configured (@TimeLimit = NULL)',
                N'Without a time limit, statistics maintenance can run indefinitely and conflict with business hours.',
                N'Set @TimeLimit to a value appropriate for your maintenance window. Common values: 3600 (1 hour), 7200 (2 hours), 18000 (5 hours).',
                N'EXECUTE dbo.sp_StatUpdate @TimeLimit = 3600;',
                30
            );
        END;

        /* W1c: No adaptive sampling when long-running stats exist */
        IF @latest_long_run_min IS NULL
        AND EXISTS (
            SELECT 1 FROM #stat_updates
            WHERE DurationMs > @LongRunningMinutes * 60 * 1000
            AND   ErrorNumber = 0
        )
        BEGIN
            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES
            (
                N'WARNING', N'SUBOPTIMAL_PARAMS',
                N'Long-running stats exist but adaptive sampling is not enabled',
                N'@LongRunningThresholdMinutes is NULL, but stats exceeding ' + CONVERT(nvarchar(10), @LongRunningMinutes) + N' minutes were found. These can dominate maintenance time.',
                N'Enable adaptive sampling to automatically use a lower sample rate for historically slow stats.',
                N'EXECUTE dbo.sp_StatUpdate @LongRunningThresholdMinutes = ' + CONVERT(nvarchar(10), @LongRunningMinutes) + N', @LongRunningSamplePercent = 10;',
                30
            );
        END;
    END;

    /* ======================================================================
       W2: LONG-RUNNING INDIVIDUAL STATS
       ====================================================================== */
    /* #266: Escalate to CRITICAL when long-running stat is also high-impact (top-10 by CPU) */
    INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
    SELECT TOP (@TopN)
        CASE WHEN MAX(wi.ImpactScore) >= 0.90 THEN N'CRITICAL' ELSE N'WARNING' END,
        N'LONG_RUNNING_STATS',
        N'Stat consistently slow: ' + su.DatabaseName + N'.'
            + su.SchemaName + N'.' + su.ObjectName + N'.' + su.StatisticsName,  /* BUG-03 fix: was missing SchemaName */
        N'Avg duration: ' + CONVERT(nvarchar(20), CONVERT(decimal(10,1), AVG(su.DurationMs) / 1000.0))
            + N' sec across ' + CONVERT(nvarchar(20), COUNT_BIG(*))
            + N' updates. Max size: ' + CONVERT(nvarchar(20), MAX(ISNULL(su.SizeMB, 0))) + N' MB'
            + N'. Avg rows: ' + CONVERT(nvarchar(20), AVG(su.RowCount_))
            + CASE WHEN MAX(wi.ImpactScore) >= 0.90
                THEN N'. HIGH IMPACT: workload rank top ' + CONVERT(nvarchar(10), CONVERT(int, (1.0 - ISNULL(MAX(wi.ImpactScore), 0)) * 100)) + N'%'
                ELSE N''
            END,
        N'Enable adaptive sampling: stats that historically exceed a threshold get a reduced sample rate automatically.',
        N'EXECUTE dbo.sp_StatUpdate @LongRunningThresholdMinutes = '
            + CONVERT(nvarchar(10), @LongRunningMinutes)
            + N', @LongRunningSamplePercent = 10;',
        CASE WHEN MAX(wi.ImpactScore) >= 0.90 THEN 12 ELSE 35 END  /* #266: CRITICAL sorts before WARNING */
    FROM #stat_updates AS su
    LEFT JOIN #workload_impact AS wi
        ON wi.DatabaseName = su.DatabaseName
        AND wi.SchemaName  = su.SchemaName
        AND wi.ObjectName  = su.ObjectName
    WHERE su.ErrorNumber = 0
    AND   su.DurationMs > @LongRunningMinutes * 60 * 1000
    GROUP BY su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName
    HAVING COUNT_BIG(*) >= 2
    ORDER BY AVG(su.DurationMs) DESC;

    IF @@ROWCOUNT > 0
        RAISERROR(N'  [WARNING] W2: Long-running stats detected', 10, 1) WITH NOWAIT;

    /* ======================================================================
       W3: STALE-STATS BACKLOG
       ====================================================================== */
    IF EXISTS (
        SELECT 1
        FROM #runs AS r
        WHERE r.IsKilled = 0
        AND   r.StatsRemaining > 0
        AND   r.StatsFound > 0
        GROUP BY r.RunLabel
        HAVING AVG(r.StatsRemaining * 1.0 / NULLIF(r.StatsFound, 0)) > 0.5
    )
    BEGIN
        INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
        SELECT
            N'WARNING',
            N'STALE_BACKLOG',
            N'Persistent backlog: avg ' + CONVERT(nvarchar(10), AVG(r.StatsRemaining))
                + N' stats remaining per run',
            N'Average found/processed/remaining: '
                + CONVERT(nvarchar(10), AVG(r.StatsFound)) + N'/'
                + CONVERT(nvarchar(10), AVG(r.StatsProcessed)) + N'/'
                + CONVERT(nvarchar(10), AVG(r.StatsRemaining))
                + N'. Over ' + CONVERT(nvarchar(10), COUNT_BIG(*)) + N' runs.',
            N'Stats qualify faster than they can be processed. Options: '
                + N'(1) Increase @TimeLimit, '
                + N'(2) Enable @StatsInParallel = N''Y'' for parallel processing, '
                + N'(3) Raise @ModificationThreshold to reduce qualifying stats, '
                + N'(4) Use @Preset = N''NIGHTLY_MAINTENANCE'' for balanced defaults.',
            N'EXECUTE dbo.sp_StatUpdate @Databases = N''USER_DATABASES'', @TimeLimit = 7200, @StatsInParallel = N''Y'';',
            25
        FROM #runs AS r
        WHERE r.IsKilled = 0
        AND   r.StatsRemaining > 0
        AND   r.StatsFound > 0
        AND   r.StatsRemaining * 1.0 / NULLIF(r.StatsFound, 0) > 0.5;  /* BUG-04 fix: match EXISTS guard -- only include runs with >50% remaining */

        RAISERROR(N'  [WARNING] W3: Stale-stats backlog detected', 10, 1) WITH NOWAIT;
    END;

    /* ======================================================================
       W4: OVERLAPPING RUNS
       ====================================================================== */
    INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
    SELECT
        N'WARNING',
        N'OVERLAPPING_RUNS',
        N'Overlapping runs detected: ' + r1.RunLabel + N' and ' + r2.RunLabel,
        N'Run 1 started ' + CONVERT(nvarchar(20), r1.StartTime, 120)
            + N', Run 2 started ' + CONVERT(nvarchar(20), r2.StartTime, 120)
            + N' (' + CONVERT(nvarchar(10), DATEDIFF(MINUTE, r1.StartTime, r2.StartTime))
            + N' minutes apart)',
        N'Multiple concurrent sp_StatUpdate runs can cause lock contention and duplicate work. '
            + N'Check SQL Agent job schedules for overlapping maintenance windows. '
            + N'If intentional, ensure @StatsInParallel = N''Y'' is used for coordinated parallel processing.',
        N'/* Review Agent job schedules for conflicts */',
        40
    FROM #runs AS r1
    INNER JOIN #runs AS r2
        ON  r2.StartTime > r1.StartTime
        AND r2.StartTime < ISNULL(r1.EndTime, DATEADD(SECOND, ISNULL(r1.TimeLimit, 18000), r1.StartTime))
        AND r2.RunLabel <> r1.RunLabel
    WHERE r1.IsKilled = 0
      AND r2.IsKilled = 0;

    IF @@ROWCOUNT > 0
        RAISERROR(N'  [WARNING] W4: Overlapping runs detected', 10, 1) WITH NOWAIT;

    /* ======================================================================
       W5: QUERY STORE NOT EFFECTIVE
       ====================================================================== */
    IF EXISTS (SELECT 1 FROM #runs WHERE QueryStorePriority = N'Y')
    BEGIN
        DECLARE @w5_qs_runs integer = (SELECT COUNT_BIG(*) FROM #runs WHERE QueryStorePriority = N'Y');
        DECLARE @w5_qs_data_runs integer = (
            SELECT COUNT(DISTINCT su.RunLabel)
            FROM #stat_updates AS su
            WHERE su.QSPlanCount > 0
        );

        IF @w5_qs_data_runs = 0
        BEGIN
            /* Original W5: QS enabled but zero data */
            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES
            (
                N'WARNING', N'QS_NOT_EFFECTIVE',
                N'Query Store priority enabled but no QS data captured in stat updates',
                N'@QueryStorePriority = N''Y'' was used but zero stat updates have QSPlanCount > 0. '
                    + N'This typically means Query Store is disabled, read-only, or has been purged.',
                N'Verify Query Store is enabled and in READ_WRITE mode on target databases: '
                    + N'SELECT name, is_query_store_on FROM sys.databases. '
                    + N'If QS is intentionally disabled, remove @QueryStorePriority to avoid unnecessary overhead. '
                    /* #278: capture mode guidance */
                    + N'Also verify CAPTURE_MODE is AUTO or CUSTOM, not ALL (SELECT actual_state_desc, query_capture_mode_desc FROM sys.database_query_store_options).',
                N'/* Check: SELECT name, is_query_store_on FROM sys.databases WHERE state_desc = N''ONLINE''; */',
                35
            );
            RAISERROR(N'  [WARNING] W5: Query Store not effective', 10, 1) WITH NOWAIT;
        END
        ELSE IF @w5_qs_data_runs < @w5_qs_runs
            AND @w5_qs_data_runs * 100.0 / @w5_qs_runs BETWEEN 10 AND 90
        BEGIN
            /* #274: Partial QS data -- may have transitioned to READ_ONLY */
            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES
            (
                N'WARNING', N'QS_NOT_EFFECTIVE',
                N'Query Store data present in only ' + CONVERT(nvarchar(10), @w5_qs_data_runs)
                    + N' of ' + CONVERT(nvarchar(10), @w5_qs_runs) + N' QS-priority runs',
                N'@QueryStorePriority = N''Y'' was used across ' + CONVERT(nvarchar(10), @w5_qs_runs)
                    + N' runs, but QS data appeared in only ' + CONVERT(nvarchar(10), @w5_qs_data_runs)
                    + N'. Query Store may have transitioned to READ_ONLY mid-window.',
                N'Check QS status: SELECT actual_state_desc, query_capture_mode_desc, current_storage_size_mb, max_storage_size_mb FROM sys.database_query_store_options. '
                    + N'If space pressure caused READ_ONLY transition, increase max_storage_size_mb or enable SIZE_BASED_CLEANUP_MODE.',
                N'ALTER DATABASE [YourDB] SET QUERY_STORE (MAX_STORAGE_SIZE_MB = 2048);',
                35
            );
            RAISERROR(N'  [WARNING] W5: Partial QS data -- possible READ_ONLY transition', 10, 1) WITH NOWAIT;
        END;
    END;

    /* ======================================================================
       W6: EXCESSIVE OVERHEAD -- discovery/checks consuming disproportionate time
       Wall clock time vs. actual UPDATE STATISTICS time.
       When overhead > 40% of wall clock, parameter changes may be adding cost.
       ====================================================================== */
    IF @stat_update_count > 0
    BEGIN
        DECLARE
            @w6_overhead_runs int = 0,
            @w6_worst_pct decimal(5,1) = 0,
            @w6_worst_label nvarchar(100) = N'',
            @w6_detail nvarchar(2000) = N'';

        ;WITH run_overhead AS
        (
            SELECT
                r.RunLabel,
                r.DurationSeconds,
                r.StatsProcessed,
                stat_seconds = CONVERT(decimal(10,1), ISNULL(su_agg.TotalStatMs, 0) / 1000.0),
                overhead_pct = CASE
                    WHEN r.DurationSeconds > 0
                    THEN CONVERT(decimal(5,1), (1.0 - ISNULL(su_agg.TotalStatMs, 0) / 1000.0 / r.DurationSeconds) * 100)
                    ELSE 0
                END
            FROM #runs AS r
            LEFT JOIN
            (
                SELECT
                    su.RunLabel,
                    TotalStatMs = SUM(ISNULL(su.DurationMs, 0))
                FROM #stat_updates AS su
                WHERE su.EndTime IS NOT NULL
                GROUP BY su.RunLabel
            ) AS su_agg ON su_agg.RunLabel = r.RunLabel
            WHERE r.IsKilled = 0
            AND   r.DurationSeconds > 10  /* skip trivial runs */
            AND   r.StatsProcessed >= 5   /* need enough stats for meaningful ratio */
        )
        SELECT
            @w6_overhead_runs = COUNT(*),
            @w6_worst_pct = MAX(ro.overhead_pct)
        FROM run_overhead AS ro
        WHERE ro.overhead_pct > 40;

        /* Get the label of the worst-overhead run */
        IF @w6_overhead_runs > 0
        BEGIN
            ;WITH run_overhead2 AS
            (
                SELECT
                    r.RunLabel,
                    overhead_pct = CASE
                        WHEN r.DurationSeconds > 0
                        THEN CONVERT(decimal(5,1), (1.0 - ISNULL(su_agg.TotalStatMs, 0) / 1000.0 / r.DurationSeconds) * 100)
                        ELSE 0
                    END
                FROM #runs AS r
                LEFT JOIN
                (
                    SELECT su.RunLabel, TotalStatMs = SUM(ISNULL(su.DurationMs, 0))
                    FROM #stat_updates AS su WHERE su.EndTime IS NOT NULL GROUP BY su.RunLabel
                ) AS su_agg ON su_agg.RunLabel = r.RunLabel
                WHERE r.IsKilled = 0 AND r.DurationSeconds > 10 AND r.StatsProcessed >= 5
            )
            SELECT TOP (1) @w6_worst_label = ro2.RunLabel
            FROM run_overhead2 AS ro2
            ORDER BY ro2.overhead_pct DESC;
        END;

        IF @w6_overhead_runs > 0
        BEGIN
            SET @w6_detail =
                CONVERT(nvarchar(10), @w6_overhead_runs) + N' run(s) spent >40% of wall-clock time on overhead (discovery, environment checks, per-stat validations) '
                + N'rather than actual UPDATE STATISTICS. Worst: ' + @w6_worst_label
                + N' at ' + CONVERT(nvarchar(10), @w6_worst_pct) + N'% overhead.';

            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES
            (
                N'WARNING', N'EXCESSIVE_OVERHEAD',
                N'Discovery and environment checks consuming disproportionate time vs actual stat updates',
                @w6_detail,
                N'Review recent parameter changes -- newly enabled features (@CollectHeapForwarding, @GroupByJoinPattern, @QueryStorePriority) add discovery overhead. '
                    + N'Consider @StagedDiscovery=N for legacy fast-path, reduce @CommandLogRetentionDays, or check whether CommandLog table needs a StartTime index. '
                    + N'If SQL Server is under memory pressure (check sys.dm_os_memory_brokers, sys.dm_os_process_memory), high overhead may indicate page cache eviction during stat scans.',
                N'EXECUTE dbo.sp_StatUpdate @Databases = N''USER_DATABASES'', @Debug = 1; /* Review per-phase timing in debug output */',
                33
            );

            DECLARE @w6_msg nvarchar(500) =
                N'  [WARNING] W6: Excessive overhead -- ' + CONVERT(nvarchar(10), @w6_overhead_runs)
                + N' run(s) with >40%% overhead (worst: ' + @w6_worst_label
                + N' at ' + CONVERT(nvarchar(10), @w6_worst_pct) + N'%%)';
            RAISERROR(@w6_msg, 10, 1) WITH NOWAIT;
        END;
    END;

    /* ======================================================================
       W7: HIGH_IMPACT_STATS_DEPRIORITIZED (#263)
       Top CPU stats processed late (high ProcessingPosition).
       ====================================================================== */
    IF @workload_impact_count > 0
    BEGIN
        DECLARE @w7_deprioritized_count integer;
        DECLARE @w7_evidence nvarchar(2000);

        /* Pre-compute median processing position */
        DECLARE @w7_median_position decimal(10, 1);
        SELECT TOP (1) @w7_median_position = pos_median
        FROM (
            SELECT pos_median = PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY su2.ProcessingPosition) OVER ()
            FROM #stat_updates AS su2
            WHERE su2.ProcessingPosition IS NOT NULL
            AND   (su2.ErrorNumber = 0 OR su2.ErrorNumber IS NULL)
        ) AS pm;

        ;WITH top_cpu_stats AS
        (
            SELECT TOP (10)
                su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName,
                avg_position = AVG(CONVERT(decimal(10, 1), su.ProcessingPosition))
            FROM #stat_updates AS su
            INNER JOIN #workload_impact AS wi
                ON wi.DatabaseName = su.DatabaseName
                AND wi.SchemaName  = su.SchemaName
                AND wi.ObjectName  = su.ObjectName
            WHERE wi.ImpactScore >= 0.90
            AND   su.ProcessingPosition IS NOT NULL
            AND   (su.ErrorNumber = 0 OR su.ErrorNumber IS NULL)
            GROUP BY su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName
            ORDER BY MAX(wi.TotalCpuMs) DESC
        )
        SELECT @w7_deprioritized_count = SUM(CASE WHEN tcs.avg_position > ISNULL(@w7_median_position, 0) THEN 1 ELSE 0 END)
        FROM top_cpu_stats AS tcs;

        IF ISNULL(@w7_deprioritized_count, 0) > 0
        BEGIN
            SET @w7_evidence = STUFF((
                SELECT TOP (5) N', ' + tcs.StatisticsName + N' (avg pos ' + CONVERT(nvarchar(10), CONVERT(int, tcs.avg_position)) + N')'
                FROM (
                    SELECT TOP (10)
                        su.StatisticsName,
                        avg_position = AVG(CONVERT(decimal(10, 1), su.ProcessingPosition))
                    FROM #stat_updates AS su
                    INNER JOIN #workload_impact AS wi
                        ON wi.DatabaseName = su.DatabaseName
                        AND wi.SchemaName  = su.SchemaName
                        AND wi.ObjectName  = su.ObjectName
                    WHERE wi.ImpactScore >= 0.90
                    AND   su.ProcessingPosition IS NOT NULL
                    AND   (su.ErrorNumber = 0 OR su.ErrorNumber IS NULL)
                    GROUP BY su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName
                    ORDER BY MAX(wi.TotalCpuMs) DESC
                ) AS tcs
                WHERE tcs.avg_position > 5
                ORDER BY tcs.avg_position DESC
                FOR XML PATH(N''), TYPE).value(N'.', N'nvarchar(max)'), 1, 2, N'');

            IF @w7_evidence IS NOT NULL
            BEGIN
                INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
                VALUES (
                    N'WARNING',
                    N'HIGH_IMPACT_STATS_DEPRIORITIZED',
                    N'High-workload statistics processed late in maintenance window',
                    N'Top CPU stats with high processing positions: ' + @w7_evidence,
                    N'Enable Query Store-based prioritization to process high-impact stats first.',
                    N'EXECUTE dbo.sp_StatUpdate @Databases = N''USER_DATABASES'', @QueryStorePriority = N''Y'', @SortOrder = N''QUERY_STORE'';',
                    36
                );

                RAISERROR(N'  [WARNING] W7: High-impact stats deprioritized', 10, 1) WITH NOWAIT;
            END;
        END;
    END;

    /* ======================================================================
       I9: WORKLOAD_CONCENTRATION (#264)
       Leadership-friendly: "Top N stats drive X% of measured query CPU"
       ====================================================================== */
    IF @workload_impact_count >= 5
    BEGIN
        DECLARE @i9_top_n integer;
        DECLARE @i9_pct decimal(5, 1);

        /* Find how many objects it takes to reach 80% of total CPU */
        SELECT TOP (1) @i9_top_n = rn, @i9_pct = CumulativePct
        FROM (
            SELECT
                rn = ROW_NUMBER() OVER (ORDER BY wi.TotalCpuMs DESC),
                wi.CumulativePct
            FROM #workload_impact AS wi
        ) AS ranked
        WHERE ranked.CumulativePct >= 80.0
        ORDER BY ranked.rn;

        IF @i9_top_n IS NOT NULL
        BEGIN
            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES (
                N'INFO',
                N'WORKLOAD_CONCENTRATION',
                N'Top ' + CONVERT(nvarchar(10), @i9_top_n) + N' table(s) drive '
                    + CONVERT(nvarchar(10), @i9_pct) + N'% of measured query CPU',
                N'Out of ' + CONVERT(nvarchar(10), @workload_impact_count) + N' objects with Query Store data, '
                    + CONVERT(nvarchar(10), @i9_top_n) + N' account for '
                    + CONVERT(nvarchar(10), @i9_pct) + N'% of total CPU. '
                    + N'Concentrating statistics maintenance on these tables maximizes performance impact.',
                N'Use @QueryStorePriority = N''Y'' to automatically prioritize these high-impact tables.',
                N'EXECUTE dbo.sp_StatUpdate @Databases = N''USER_DATABASES'', @QueryStorePriority = N''Y'';',
                74
            );

            RAISERROR(N'  [INFO] I9: Workload concentration identified', 10, 1) WITH NOWAIT;
        END;
    END;

    /* ======================================================================
       I4: UNUSED FEATURES
       ====================================================================== */
    IF @latest_run_label IS NOT NULL
    BEGIN
        /* I4a: No preset */
        IF @latest_preset IS NULL
        BEGIN
            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES
            (
                N'INFO', N'UNUSED_FEATURES',
                N'Presets not in use',
                N'@Preset is NULL. Presets provide tested parameter combinations for common scenarios.',
                N'Consider using presets for standardized configurations: NIGHTLY_MAINTENANCE, WEEKLY_FULL, OLTP_LIGHT, WAREHOUSE_AGGRESSIVE.',
                N'EXECUTE dbo.sp_StatUpdate @Preset = N''NIGHTLY_MAINTENANCE'', @Databases = N''USER_DATABASES'';',
                60
            );
        END;

        /* I4b: Long-running stats exist but adaptive not enabled (softer than W1c) */
        IF @latest_long_run_min IS NULL
        AND NOT EXISTS (SELECT 1 FROM #recommendations WHERE Category = N'SUBOPTIMAL_PARAMS' AND Finding LIKE N'%adaptive%')
        AND @stat_update_count > 0
        AND EXISTS (SELECT 1 FROM #stat_updates WHERE DurationMs > 300000 AND ErrorNumber = 0) /* >5 min */
        BEGIN
            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES
            (
                N'INFO', N'UNUSED_FEATURES',
                N'Adaptive sampling available but not enabled',
                N'Some stats take >5 minutes. @LongRunningThresholdMinutes can automatically cap these with a lower sample rate.',
                N'Adaptive sampling uses CommandLog history to identify slow stats and apply reduced sampling.',
                N'EXECUTE dbo.sp_StatUpdate @LongRunningThresholdMinutes = 15, @LongRunningSamplePercent = 10;',
                65
            );
        END;
    END;

    /* ======================================================================
       I5: VERSION_HISTORY -- sp_StatUpdate versions used across analysis window
       ====================================================================== */
    IF @run_count > 0
    BEGIN
        DECLARE
            @i5_version_count integer,
            @i5_version_detail nvarchar(max);

        SET @i5_version_count = (SELECT COUNT(DISTINCT r.[Version]) FROM #runs AS r WHERE r.[Version] IS NOT NULL);

        SELECT @i5_version_detail = STRING_AGG(v.VersionInfo, N'; ') WITHIN GROUP (ORDER BY v.VersionInfo)
        FROM
        (
            SELECT
                VersionInfo = r.[Version] + N' (' + CONVERT(nvarchar(10), COUNT_BIG(*)) + N' runs)'
            FROM #runs AS r
            WHERE r.[Version] IS NOT NULL
            GROUP BY r.[Version]
        ) AS v;

        IF @i5_version_count >= 2
        BEGIN
            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES
            (
                N'WARNING',
                N'VERSION_HISTORY',
                N'Version skew detected: ' + CONVERT(nvarchar(10), @i5_version_count) + N' different sp_StatUpdate versions in the analysis window.',
                @i5_version_detail,
                N'Standardize all servers and jobs on the latest version to ensure consistent behavior and bug fixes.',
                N'/* Deploy latest sp_StatUpdate.sql to all servers */',
                55
            );

            RAISERROR(N'  [WARNING] I5: %i versions detected (version skew)', 10, 1, @i5_version_count) WITH NOWAIT;
        END
        ELSE IF @i5_version_count = 1
        BEGIN
            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES
            (
                N'INFO',
                N'VERSION_HISTORY',
                N'Consistent version: all runs used ' + ISNULL(@i5_version_detail, N'(unknown)') + N'.',
                @i5_version_detail,
                N'No action needed. All runs use the same version.',
                N'',
                70
            );

            RAISERROR(N'  [INFO] I5: single version in use', 10, 1) WITH NOWAIT;
        END;
    END;

    /*
    ============================================================================
    QS EFFICACY ANALYSIS: Build #qs_efficacy temp table
    ============================================================================
    */
    RAISERROR(N'Computing QS efficacy metrics...', 10, 1) WITH NOWAIT;

    CREATE TABLE #qs_efficacy
    (
        RunLabel nvarchar(100) NOT NULL,
        StartTime datetime2(3) NOT NULL,
        SortOrder nvarchar(50) NULL,
        QueryStorePriority nvarchar(1) NULL,
        StatsProcessed integer NULL,
        StatsFound integer NULL,
        DurationSeconds integer NULL,
        StopReason nvarchar(50) NULL,
        IsQSRun bit NOT NULL DEFAULT 0,
        /* Computed efficacy metrics */
        HighCpuInFirstQuartilePct decimal(5, 1) NULL,
        MinutesToHighCpuComplete decimal(10, 1) NULL,
        WorkloadCoveragePct decimal(5, 1) NULL,
        CompletionPct decimal(5, 1) NULL,
        AvgSecPerStat decimal(10, 1) NULL,
        AvgPositionTop10 decimal(10, 1) NULL,

        CONSTRAINT PK_qs_efficacy PRIMARY KEY NONCLUSTERED (RunLabel)
    );

    /* Populate #qs_efficacy for all non-killed runs that have stat updates.
       BUG-D fix: QS-specific metrics (HighCpuInFirstQuartilePct, MinutesToHighCpuComplete,
       WorkloadCoveragePct, AvgPositionTop10) are only computed for runs with actual QS data.
       Non-QS runs get NULL for these columns to prevent random first-quartile noise.
       BUG-A fix: WorkloadCoveragePct uses honest denominator -- for completed runs = 100%,
       for incomplete runs, extrapolates total CPU from processed stats' average. */
    ;WITH stat_positions AS
    (
        /* Determine each stat's position within its run (use ProcessingPosition if available, else order by ID) */
        SELECT
            su.RunLabel,
            su.ID,
            su.DatabaseName,
            su.SchemaName,
            su.ObjectName,
            su.StatisticsName,
            su.QSTotalCpuMs,
            su.QSTotalExecutions,
            su.QSPlanCount,
            su.DurationMs,
            su.QualifyReason,
            processing_pos  = ISNULL(su.ProcessingPosition, ROW_NUMBER() OVER (PARTITION BY su.RunLabel ORDER BY su.ID)),
            cpu_rank        = ROW_NUMBER() OVER (PARTITION BY su.RunLabel ORDER BY ISNULL(su.QSTotalCpuMs, 0) DESC, su.ID),
            su.StartTime AS StatStartTime,
            su.EndTime AS StatEndTime
        FROM #stat_updates AS su
        WHERE su.ErrorNumber = 0
        OR    su.ErrorNumber IS NULL
    ),
    run_metrics AS
    (
        SELECT
            sp.RunLabel,
            /* BUG-D: Only count stats with actual QS data (QSTotalCpuMs > 0) for QS-specific metrics */
            high_cpu_first_q = SUM(
                CASE
                    WHEN sp.cpu_rank <= 10
                    AND  sp.QSTotalCpuMs > 0
                    AND  r.StatsFound > 0
                    AND  sp.processing_pos <= CEILING(r.StatsFound * 0.25)
                    THEN 1
                    ELSE 0
                END
            ),
            high_cpu_total = SUM(CASE WHEN sp.cpu_rank <= 10 AND sp.QSTotalCpuMs > 0 THEN 1 ELSE 0 END),
            /* Minutes from run start until all top-10 CPU stats are processed */
            max_top10_end = MAX(CASE WHEN sp.cpu_rank <= 10 AND sp.QSTotalCpuMs > 0 THEN sp.StatEndTime END),
            /* BUG-A: processed_cpu_ms = CPU of stats that were actually processed */
            processed_cpu_ms = SUM(CASE WHEN sp.QSTotalCpuMs > 0 THEN sp.QSTotalCpuMs ELSE 0 END),
            /* Average position of top-10 CPU stats (only those with QS data) */
            avg_pos_top10 = AVG(CASE WHEN sp.cpu_rank <= 10 AND sp.QSTotalCpuMs > 0 THEN CONVERT(decimal(10, 1), sp.processing_pos) END),
            stat_count = COUNT_BIG(*),
            /* Count of stats with actual QS data -- used to detect non-QS runs */
            qs_stat_count = SUM(CASE WHEN sp.QSTotalCpuMs > 0 THEN 1 ELSE 0 END)
        FROM stat_positions AS sp
        INNER JOIN #runs AS r
            ON r.RunLabel = sp.RunLabel
        GROUP BY sp.RunLabel
    )
    INSERT INTO #qs_efficacy
    (
        RunLabel, StartTime, SortOrder, QueryStorePriority,
        StatsProcessed, StatsFound, DurationSeconds, StopReason, IsQSRun,
        HighCpuInFirstQuartilePct, MinutesToHighCpuComplete,
        WorkloadCoveragePct, CompletionPct, AvgSecPerStat, AvgPositionTop10
    )
    SELECT
        r.RunLabel,
        r.StartTime,
        r.SortOrder,
        r.QueryStorePriority,
        r.StatsProcessed,
        r.StatsFound,
        r.DurationSeconds,
        r.StopReason,
        is_qs_run       = CASE
                              WHEN r.QueryStorePriority = N'Y'
                              OR   r.SortOrder LIKE N'%QUERY_STORE%'
                              THEN 1
                              ELSE 0
                          END,
        /* BUG-D: QS-specific metrics are NULL when no stats have QS data */
        high_cpu_q1_pct = CASE
                              WHEN ISNULL(rm.qs_stat_count, 0) = 0 THEN NULL
                              WHEN rm.high_cpu_total > 0
                              THEN CONVERT(decimal(5, 1), rm.high_cpu_first_q * 100.0 / rm.high_cpu_total)
                              ELSE NULL
                          END,
        min_to_high_cpu = CASE
                              WHEN ISNULL(rm.qs_stat_count, 0) = 0 THEN NULL
                              WHEN rm.max_top10_end IS NOT NULL
                              THEN CONVERT(decimal(10, 1), DATEDIFF(SECOND, r.StartTime, rm.max_top10_end) / 60.0)
                              ELSE NULL
                          END,
        /* BUG-A fix: For completed runs, WorkloadCoveragePct = 100% (all qualifying stats processed).
           For incomplete runs, extrapolate: estimated_total = processed_cpu * (StatsFound / StatsProcessed).
           This accounts for unprocessed stats whose CPU we don't know. */
        workload_pct    = CASE
                              WHEN ISNULL(rm.qs_stat_count, 0) = 0 THEN NULL
                              WHEN rm.processed_cpu_ms <= 0 THEN NULL
                              WHEN r.StopReason = N'COMPLETED' OR r.StatsFound = ISNULL(r.StatsProcessed, 0) THEN 100.0
                              WHEN r.StatsFound > 0 AND ISNULL(r.StatsProcessed, 0) > 0
                              THEN CONVERT(decimal(5, 1),
                                  rm.processed_cpu_ms * 100.0 /
                                  (rm.processed_cpu_ms + (rm.processed_cpu_ms * 1.0 / rm.qs_stat_count)
                                      * (r.StatsFound - ISNULL(r.StatsProcessed, 0)))
                              )
                              ELSE NULL
                          END,
        completion_pct  = CASE
                              WHEN r.StatsFound > 0
                              THEN CONVERT(decimal(5, 1), ISNULL(r.StatsProcessed, 0) * 100.0 / r.StatsFound)
                              ELSE NULL
                          END,
        avg_sec_stat    = CASE
                              WHEN r.StatsProcessed > 0
                              THEN CONVERT(decimal(10, 1), r.DurationSeconds * 1.0 / r.StatsProcessed)
                              ELSE NULL
                          END,
        CASE WHEN ISNULL(rm.qs_stat_count, 0) = 0 THEN NULL ELSE rm.avg_pos_top10 END
    FROM #runs AS r
    LEFT JOIN run_metrics AS rm
        ON rm.RunLabel = r.RunLabel
    WHERE r.IsKilled = 0;

    DECLARE @qs_efficacy_count integer = (SELECT COUNT_BIG(*) FROM #qs_efficacy);
    DECLARE @qs_run_count integer = (SELECT COUNT_BIG(*) FROM #qs_efficacy WHERE IsQSRun = 1);
    RAISERROR(N'  Efficacy rows: %i (QS runs: %i)', 10, 1, @qs_efficacy_count, @qs_run_count) WITH NOWAIT;

    IF @Debug = 1 AND @qs_efficacy_count > 0
    BEGIN
        SET @debug_elapsed_ms = DATEDIFF(MILLISECOND, @debug_timer, SYSDATETIME());
        RAISERROR(N'DEBUG: #qs_efficacy sample (top 5)', 10, 1) WITH NOWAIT;
        SELECT TOP (5) * FROM #qs_efficacy ORDER BY StartTime DESC;
        RAISERROR(N'DEBUG: QS efficacy computation complete (%i ms)', 10, 1, @debug_elapsed_ms) WITH NOWAIT;
    END;

    /* ======================================================================
       I6: QS_EFFICACY - Query Store prioritization effectiveness
       ====================================================================== */
    IF @qs_run_count > 0
    BEGIN
        DECLARE
            @i6_avg_q1_pct decimal(5, 1),
            @i6_avg_minutes decimal(10, 1),
            @i6_avg_workload decimal(5, 1),
            @i6_top10_in_q1 integer,
            @i6_run_count integer;

        SELECT
            @i6_avg_q1_pct    = AVG(e.HighCpuInFirstQuartilePct),
            @i6_avg_minutes   = AVG(e.MinutesToHighCpuComplete),
            @i6_avg_workload  = AVG(e.WorkloadCoveragePct),
            @i6_top10_in_q1   = AVG(CONVERT(integer, e.HighCpuInFirstQuartilePct / 10.0)),
            @i6_run_count     = COUNT_BIG(*)
        FROM #qs_efficacy AS e
        WHERE e.IsQSRun = 1
        AND   e.HighCpuInFirstQuartilePct IS NOT NULL;

        IF @i6_run_count = 0
        BEGIN
            /* QS-flagged runs exist but none had usable CPU metrics (all HighCpuInFirstQuartilePct IS NULL).
               This happens when Query Store was recently enabled, has no runtime stats, or databases have minimal activity. */
            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES
            (
                N'INFO',
                N'QS_EFFICACY',
                N'Query Store prioritization is configured but no CPU workload data was found in '
                    + CONVERT(nvarchar(10), @qs_run_count) + N' recent QS run(s). '
                    + N'This may indicate Query Store was recently enabled, has no runtime stats yet, '
                    + N'or the monitored databases have minimal query activity.',
                N'QS-flagged runs found: ' + CONVERT(nvarchar(10), @qs_run_count)
                    + N'. None contained usable CPU metrics in sys.query_store_runtime_stats.',
                N'Verify Query Store is in READ_WRITE mode and has accumulated runtime statistics. '
                    + N'Check: SELECT actual_state_desc FROM sys.database_query_store_options;',
                N'EXECUTE dbo.sp_StatUpdate_Diag @EfficacyDaysBack = 100, @Debug = 1;',
                70
            );
            RAISERROR(N'  [INFO] I6: QS runs found but no CPU data available', 10, 1) WITH NOWAIT;
        END
        ELSE
        BEGIN
            /* GAP-E: Estimate time without prioritization for comparison.
               Without QS prioritization, critical stats land at random positions.
               Expected position = midpoint (StatsFound/2), so expected time =
               AvgSecPerStat * (StatsFound/2) / 60.0 minutes. */
            DECLARE
                @i6_estimated_no_qs_min decimal(10, 1);

            SELECT
                @i6_estimated_no_qs_min = AVG(e.AvgSecPerStat * e.StatsFound / 2.0 / 60.0)
            FROM #qs_efficacy AS e
            WHERE e.IsQSRun = 1
            AND   e.HighCpuInFirstQuartilePct IS NOT NULL
            AND   e.AvgSecPerStat > 0
            AND   e.StatsFound > 0;

            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES
            (
                N'INFO',
                N'QS_EFFICACY',
                N'Query Store prioritization active: '
                    + CONVERT(nvarchar(10), CONVERT(integer, @i6_avg_q1_pct / 10.0)) + N' of 10 highest-workload statistics updated in the first '
                    + CONVERT(nvarchar(10), CONVERT(integer, @i6_avg_minutes)) + N' minutes across '
                    + CONVERT(nvarchar(10), @i6_run_count) + N' recent run(s). Workload coverage: '
                    + CONVERT(nvarchar(10), @i6_avg_workload) + N'% of measured CPU.'
                    + CASE WHEN @i6_estimated_no_qs_min IS NOT NULL AND @i6_avg_minutes IS NOT NULL
                        THEN N' Critical stats reached in '
                            + CONVERT(nvarchar(10), CONVERT(integer, @i6_avg_minutes)) + N' min (QS) vs ~'
                            + CONVERT(nvarchar(10), CONVERT(integer, @i6_estimated_no_qs_min)) + N' min estimated without prioritization.'
                        ELSE N''
                    END,
                N'Avg high-CPU-in-first-quartile: ' + CONVERT(nvarchar(10), @i6_avg_q1_pct) + N'%. '
                    + N'Avg minutes to complete top-10: ' + CONVERT(nvarchar(20), @i6_avg_minutes) + N'. '
                    + N'Avg workload coverage: ' + CONVERT(nvarchar(10), @i6_avg_workload) + N'%. '
                    + N'Across ' + CONVERT(nvarchar(10), @i6_run_count) + N' QS-priority run(s).'
                    + CASE WHEN @i6_estimated_no_qs_min IS NOT NULL
                        THEN N' Estimated without QS: ~' + CONVERT(nvarchar(10), CONVERT(integer, @i6_estimated_no_qs_min)) + N' min to reach critical stats.'
                        ELSE N''
                    END,
                N'High first-quartile placement means the most impactful statistics are being refreshed early in the maintenance window. '
                    + N'Monitor this over time to confirm QS prioritization continues to deliver value.',
                N'EXECUTE dbo.sp_StatUpdate_Diag @EfficacyDaysBack = 100;',
                70
            );

            RAISERROR(N'  [INFO] I6: QS efficacy computed', 10, 1) WITH NOWAIT;
        END;
    END;

    /* ======================================================================
       I7: QS_INFLECTION - Before/after change detection
       ====================================================================== */
    DECLARE
        @pre_qs_count integer = 0,
        @post_qs_count integer = 0,
        @inflection_date datetime2(3);

    /* Find inflection point: first QS run that follows a non-QS run */
    SELECT TOP (1)
        @inflection_date = e.StartTime
    FROM #qs_efficacy AS e
    WHERE e.IsQSRun = 1
    AND   EXISTS (
        SELECT 1 FROM #qs_efficacy AS e2
        WHERE e2.IsQSRun = 0
        AND   e2.StartTime < e.StartTime
    )
    ORDER BY e.StartTime;

    IF @inflection_date IS NOT NULL
    BEGIN
        SELECT
            @pre_qs_count = COUNT_BIG(*)
        FROM #qs_efficacy
        WHERE StartTime < @inflection_date
        AND   IsQSRun = 0;

        SELECT
            @post_qs_count = COUNT_BIG(*)
        FROM #qs_efficacy
        WHERE StartTime >= @inflection_date
        AND   IsQSRun = 1;
    END;

    IF @inflection_date IS NOT NULL AND @pre_qs_count >= 3 AND @post_qs_count >= 3
    BEGIN
        DECLARE
            @pre_q1_pct decimal(5, 1),
            @post_q1_pct decimal(5, 1),
            @pre_completion decimal(5, 1),
            @post_completion decimal(5, 1),
            @pre_minutes decimal(10, 1),
            @post_minutes decimal(10, 1);

        SELECT
            @pre_q1_pct     = AVG(e.HighCpuInFirstQuartilePct),
            @pre_completion = AVG(e.CompletionPct),
            @pre_minutes    = AVG(e.MinutesToHighCpuComplete)
        FROM #qs_efficacy AS e
        WHERE e.StartTime < @inflection_date
        AND   e.IsQSRun = 0;

        SELECT
            @post_q1_pct     = AVG(e.HighCpuInFirstQuartilePct),
            @post_completion = AVG(e.CompletionPct),
            @post_minutes    = AVG(e.MinutesToHighCpuComplete)
        FROM #qs_efficacy AS e
        WHERE e.StartTime >= @inflection_date
        AND   e.IsQSRun = 1;

        INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
        VALUES
        (
            N'INFO',
            N'QS_INFLECTION',
            N'Configuration change detected ('
                + CONVERT(nvarchar(20), @inflection_date, 120) + N'): High-workload stats moved from '
                + CASE WHEN ISNULL(@pre_q1_pct, 0) < 50 THEN N'bottom-half' ELSE N'mid-range' END
                + N' to first-quartile processing.'
                + CASE WHEN @pre_minutes IS NOT NULL AND @post_minutes IS NOT NULL AND @pre_minutes > @post_minutes
                    THEN N' Before: ' + CONVERT(nvarchar(10), CONVERT(integer, @pre_minutes)) + N' min to critical stats. After: '
                        + CONVERT(nvarchar(10), CONVERT(integer, @post_minutes)) + N' min. Time saved: '
                        + CONVERT(nvarchar(10), CONVERT(integer, @pre_minutes - @post_minutes)) + N' min per run.'
                    ELSE N''
                END,
            N'Before (' + CONVERT(nvarchar(10), @pre_qs_count) + N' runs): '
                + ISNULL(CONVERT(nvarchar(10), CONVERT(integer, @pre_q1_pct / 10.0)), N'?') + N'/10 critical stats in first quartile, '
                + ISNULL(CONVERT(nvarchar(10), CONVERT(integer, @pre_completion)), N'?') + N'% completion, '
                + ISNULL(CONVERT(nvarchar(10), CONVERT(integer, @pre_minutes)), N'?') + N' min to critical. '
                + N'After (' + CONVERT(nvarchar(10), @post_qs_count) + N' runs): '
                + ISNULL(CONVERT(nvarchar(10), CONVERT(integer, @post_q1_pct / 10.0)), N'?') + N'/10 critical stats in first quartile, '
                + ISNULL(CONVERT(nvarchar(10), CONVERT(integer, @post_completion)), N'?') + N'% completion, '
                + ISNULL(CONVERT(nvarchar(10), CONVERT(integer, @post_minutes)), N'?') + N' min to critical.',
            N'The switch to Query Store-based prioritization is delivering measurable improvement in how quickly '
                + N'the highest-workload statistics are refreshed. Continue monitoring via @EfficacyDaysBack.',
            N'EXECUTE dbo.sp_StatUpdate_Diag @EfficacyDaysBack = 100;',
            71
        );

        RAISERROR(N'  [INFO] I7: QS inflection detected', 10, 1) WITH NOWAIT;
    END;

    /* ======================================================================
       I8: QS_PERFORMANCE_TREND -- Are queries actually getting faster after stat updates?
       BUG-B fix: Normalize to per-execution CPU (QSTotalCpuMs / QSTotalExecutions).
       Raw QSTotalCpuMs is cumulative from Query Store and always grows as more
       queries execute. Per-execution CPU shows actual plan efficiency changes.
       ====================================================================== */
    IF EXISTS (
        SELECT 1 FROM #stat_updates
        WHERE QSTotalCpuMs IS NOT NULL AND QSTotalCpuMs > 0
        AND   QSTotalExecutions IS NOT NULL AND QSTotalExecutions > 0
    )
    BEGIN
        DECLARE
            @i8_improving_count integer = 0,
            @i8_degrading_count integer = 0,
            @i8_stable_count integer = 0,
            @i8_regressed_count integer = 0,
            @i8_total_tracked integer = 0,
            @i8_avg_delta_pct decimal(10, 1) = 0;

        /* Compare first vs last per-execution CPU for each stat across runs */
        ;WITH stat_appearances AS
        (
            SELECT
                su.DatabaseName,
                su.SchemaName,
                su.ObjectName,
                su.StatisticsName,
                su.QSTotalCpuMs,
                su.QSTotalExecutions,
                /* BUG-B: normalize to per-execution CPU */
                cpu_per_exec = CONVERT(decimal(18, 4), su.QSTotalCpuMs * 1.0 / NULLIF(su.QSTotalExecutions, 0)),
                su.RunLabel,
                r.StartTime,
                appearance_num = ROW_NUMBER() OVER (
                    PARTITION BY su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName
                    ORDER BY r.StartTime
                ),
                appearance_cnt = COUNT_BIG(*) OVER (
                    PARTITION BY su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName
                )
            FROM #stat_updates AS su
            INNER JOIN #runs AS r
                ON r.RunLabel = su.RunLabel
            WHERE su.QSTotalCpuMs IS NOT NULL
            AND   su.QSTotalCpuMs > 0
            AND   su.QSTotalExecutions IS NOT NULL
            AND   su.QSTotalExecutions > 0
            AND   (su.ErrorNumber = 0 OR su.ErrorNumber IS NULL)
        ),
        first_last AS
        (
            SELECT
                DatabaseName, SchemaName, ObjectName, StatisticsName,
                first_cpu_per_exec = MAX(CASE WHEN appearance_num = 1 THEN cpu_per_exec END),
                last_cpu_per_exec  = MAX(CASE WHEN appearance_num = appearance_cnt THEN cpu_per_exec END),
                min_cpu_per_exec   = MIN(cpu_per_exec),
                appearances = MAX(appearance_cnt)
            FROM stat_appearances
            WHERE appearance_cnt >= 2
            GROUP BY DatabaseName, SchemaName, ObjectName, StatisticsName
        )
        SELECT
            @i8_total_tracked   = COUNT_BIG(*),
            @i8_improving_count = SUM(CASE WHEN last_cpu_per_exec < first_cpu_per_exec * 0.95 THEN 1 ELSE 0 END),
            @i8_degrading_count = SUM(CASE WHEN last_cpu_per_exec > first_cpu_per_exec * 1.05 THEN 1 ELSE 0 END),
            @i8_stable_count    = SUM(CASE WHEN last_cpu_per_exec BETWEEN first_cpu_per_exec * 0.95 AND first_cpu_per_exec * 1.05 THEN 1 ELSE 0 END),
            /* #276: Detect improve-then-regress: min < first AND last > min (improved at some point, then got worse) */
            @i8_regressed_count = SUM(CASE WHEN appearances >= 3 AND min_cpu_per_exec < first_cpu_per_exec * 0.95 AND last_cpu_per_exec > min_cpu_per_exec * 1.05 THEN 1 ELSE 0 END),
            @i8_avg_delta_pct   = AVG(CASE WHEN first_cpu_per_exec > 0 THEN CONVERT(decimal(10, 1), (last_cpu_per_exec - first_cpu_per_exec) * 100.0 / first_cpu_per_exec) ELSE 0 END)
        FROM first_last;

        IF @i8_total_tracked < 2
        BEGIN
            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES
            (
                N'INFO',
                N'QS_PERFORMANCE_TREND',
                N'Insufficient data for per-execution CPU trend analysis. Need 2+ statistics appearing in 2+ runs with Query Store data.'
                    + CASE
                        WHEN (SELECT COUNT(DISTINCT su2.RunLabel) FROM #stat_updates AS su2 WHERE su2.QSTotalCpuMs IS NOT NULL AND su2.QSTotalCpuMs > 0) < 2
                        THEN N' Only ' + CONVERT(nvarchar(10), (SELECT COUNT(DISTINCT su2.RunLabel) FROM #stat_updates AS su2 WHERE su2.QSTotalCpuMs IS NOT NULL AND su2.QSTotalCpuMs > 0)) + N' QS-enabled run(s) found; need 2+ for comparison.'
                        ELSE N' QS data exists across runs, but individual stats appear only once each.'
                    END,
                N'Tracked ' + CONVERT(nvarchar(10), @i8_total_tracked) + N' statistics with 2+ appearances. At least 2 are needed for trend analysis.',
                N'Run sp_StatUpdate with @QueryStorePriority=Y across multiple maintenance windows to build up comparison data.',
                N'EXECUTE dbo.sp_StatUpdate @Databases = N''USER_DATABASES'', @QueryStorePriority = N''Y'';',
                73
            );

            RAISERROR(N'  [INFO] I8: insufficient data for QS performance trend (%i stats tracked)', 10, 1, @i8_total_tracked) WITH NOWAIT;
        END
        ELSE
        BEGIN
            /* #292: Count forced plans on objects with degrading CPU trend */
            DECLARE @i8_forced_at_risk integer = ISNULL((
                SELECT SUM(fp.ForcedPlanCount)
                FROM #forced_plans AS fp
                WHERE EXISTS (
                    SELECT 1
                    FROM #stat_updates AS su
                    WHERE su.DatabaseName = fp.DatabaseName
                    AND   su.ObjectName = fp.ObjectName
                    AND   su.QSTotalCpuMs IS NOT NULL
                    AND   su.QSTotalCpuMs > 0
                )
            ), 0);

            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES
            (
                N'INFO',
                N'QS_PERFORMANCE_TREND',
                CASE
                    WHEN @i8_improving_count > @i8_degrading_count AND @i8_avg_delta_pct < -5
                    THEN N'Per-execution query CPU improving after stat updates: '
                        + CONVERT(nvarchar(10), @i8_improving_count) + N' of ' + CONVERT(nvarchar(10), @i8_total_tracked)
                        + N' tracked statistics show lower per-execution CPU (avg '
                        + CONVERT(nvarchar(20), ABS(@i8_avg_delta_pct)) + N'% reduction).'
                    /* #276: Improve-then-regress pattern -- CPU improved at some point but regressed back */
                    WHEN @i8_regressed_count > 0 AND @i8_regressed_count >= @i8_degrading_count
                    THEN N'Per-execution query CPU improved then regressed: '
                        + CONVERT(nvarchar(10), @i8_regressed_count) + N' of ' + CONVERT(nvarchar(10), @i8_total_tracked)
                        + N' tracked statistics showed CPU improvement at mid-window but have since regressed.'
                        + CASE WHEN @i8_forced_at_risk > 0
                            THEN N' WARNING: ' + CONVERT(nvarchar(10), @i8_forced_at_risk) + N' forced plan(s) exist on affected tables.'
                            ELSE N''
                        END
                    WHEN @i8_degrading_count > @i8_improving_count
                    THEN N'Per-execution query CPU not improving: '
                        + CONVERT(nvarchar(10), @i8_degrading_count) + N' of ' + CONVERT(nvarchar(10), @i8_total_tracked)
                        + N' tracked statistics show higher per-execution CPU (avg '
                        + CONVERT(nvarchar(20), @i8_avg_delta_pct) + N'% change).'
                        + CASE WHEN @i8_forced_at_risk > 0
                            THEN N' WARNING: ' + CONVERT(nvarchar(10), @i8_forced_at_risk) + N' forced plan(s) exist on affected tables -- stat updates may have triggered forced plan abandonment.'
                            ELSE N''
                        END
                    ELSE N'Per-execution query CPU stable across updates: '
                        + CONVERT(nvarchar(10), @i8_stable_count) + N' of ' + CONVERT(nvarchar(10), @i8_total_tracked)
                        + N' tracked statistics show consistent per-execution CPU. '
                        + CONVERT(nvarchar(10), @i8_improving_count) + N' improving, '
                        + CONVERT(nvarchar(10), @i8_degrading_count) + N' degrading.'
                END,
                N'Tracked ' + CONVERT(nvarchar(10), @i8_total_tracked)
                    + N' statistics appearing in 2+ runs (per-execution CPU). Improving: ' + CONVERT(nvarchar(10), @i8_improving_count)
                    + N'. Stable: ' + CONVERT(nvarchar(10), @i8_stable_count)
                    + N'. Degrading: ' + CONVERT(nvarchar(10), @i8_degrading_count)
                    + CASE WHEN @i8_regressed_count > 0
                        THEN N'. Improved-then-regressed: ' + CONVERT(nvarchar(10), @i8_regressed_count)
                        ELSE N''
                    END
                    + N'. Avg per-exec CPU change: ' + CONVERT(nvarchar(20), @i8_avg_delta_pct) + N'%.'
                    + CASE WHEN @i8_forced_at_risk > 0
                        THEN N' Forced plans at risk: ' + CONVERT(nvarchar(10), @i8_forced_at_risk) + N'.'
                        ELSE N''
                    END,
                CASE
                    WHEN @i8_improving_count > @i8_degrading_count
                    THEN N'Statistics maintenance is contributing to query performance improvements. The stat updates are helping the optimizer choose better plans.'
                    WHEN @i8_regressed_count > 0 AND @i8_regressed_count >= @i8_degrading_count
                    THEN N'Some statistics improved then regressed -- possible external workload shift, schema change, or parameter sniffing. '
                        + N'Check RS 13 for specific stats and correlate with deployment or workload changes.'
                    WHEN @i8_degrading_count > @i8_improving_count AND @i8_forced_at_risk > 0
                    THEN N'CRITICAL: Forced plans on degrading tables may have been invalidated by stat updates. '
                        + N'Check sys.query_store_plan for is_forced_plan=1 with force_failure_count > 0. '
                        + N'A forced plan that fails reverts to optimizer choice -- often a regression.'
                    WHEN @i8_degrading_count > @i8_improving_count
                    THEN N'Stat updates are not correlating with improved query performance. Check for plan regressions, forced plans, or parameter sensitivity.'
                    ELSE N'Stat updates are maintaining stable query performance. This is expected for well-maintained databases.'
                END,
                N'EXECUTE dbo.sp_StatUpdate_Diag @ExpertMode = 1; /* RS13 shows per-stat detail */',
                72
            );

            RAISERROR(N'  [INFO] I8: QS performance trend computed', 10, 1) WITH NOWAIT;
        END;
    END
    ELSE
    BEGIN
        /* #239: No QS CPU data at all -- explain why I8/RS13 are empty instead of silent skip. */
        INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
        VALUES (
            N'INFO', N'QS_PERFORMANCE_TREND',
            N'No Query Store CPU data found in any run. RS 13 (QS Performance Correlation) will be empty.',
            N'0 of ' + CONVERT(nvarchar(10), (SELECT COUNT(*) FROM #runs))
                + N' runs contained Query Store CPU metrics. Ensure @QueryStorePriority=Y and Query Store is enabled/READ_WRITE.',
            N'Enable QS prioritization: EXEC sp_StatUpdate @QueryStorePriority = N''Y'', @QueryStoreMetric = N''CPU''.',
            N'EXECUTE dbo.sp_StatUpdate @Databases = N''USER_DATABASES'', @QueryStorePriority = N''Y'';',
            73
        );
        RAISERROR(N'  [INFO] I8: no Query Store CPU data available', 10, 1) WITH NOWAIT;
    END;

    /* ======================================================================
       I10: RECOMMENDED CONFIGURATION -- Synthesize a single parameter set
       based on diagnostic findings, parameter history, and safeguards.
       Baseline: latest completed (non-killed) run.  Adjustments driven by
       which checks fired (C1-C4, W1-W6).  Safeguards from any historical
       run are preserved even if dropped in the latest run.
       ====================================================================== */
    RAISERROR(N'Building recommended configuration (I10)...', 10, 1) WITH NOWAIT;

    BEGIN
        /* ---- Baseline: latest completed (non-killed) run ---- */
        DECLARE @rc_baseline nvarchar(100) = (
            SELECT TOP (1) RunLabel
            FROM #runs
            WHERE IsKilled = 0
            ORDER BY StartTime DESC
        );

        /* Fall back to latest run if every run was killed */
        IF @rc_baseline IS NULL
            SET @rc_baseline = (SELECT TOP (1) RunLabel FROM #runs ORDER BY StartTime DESC);

        IF @rc_baseline IS NOT NULL
        BEGIN
            DECLARE
                @rc_databases       nvarchar(max),
                @rc_timelimit       integer,
                @rc_sort            nvarchar(50),
                @rc_qs_pri          nvarchar(1),
                @rc_mod_thresh      bigint,
                @rc_long_min        integer,
                @rc_long_pct        integer,
                @rc_parallel        nvarchar(1),
                @rc_group_join      nvarchar(1),
                @rc_filtered_mode   nvarchar(10),
                @rc_fail_fast       bit,
                @rc_call            nvarchar(1000),
                @rc_rationale       nvarchar(2000) = N'',
                @rc_safeguards      nvarchar(500);

            SELECT
                @rc_databases     = ISNULL([Databases], N'USER_DATABASES'),
                @rc_timelimit     = TimeLimit,
                @rc_sort          = SortOrder,
                @rc_qs_pri        = QueryStorePriority,
                @rc_mod_thresh    = ModificationThreshold,
                @rc_long_min      = LongRunningThresholdMinutes,
                @rc_long_pct      = LongRunningSamplePercent,
                @rc_parallel      = StatsInParallel,
                @rc_group_join    = GroupByJoinPattern,
                @rc_filtered_mode = FilteredStatsMode,
                @rc_fail_fast     = FailFast
            FROM #runs
            WHERE RunLabel = @rc_baseline;

            /* ---- Adjust based on fired checks ---- */

            /* C3: Time limit exhaustion -- increase if short */
            IF EXISTS (SELECT 1 FROM #recommendations WHERE Category = N'TIME_LIMIT_EXHAUSTION')
            BEGIN
                IF @rc_timelimit IS NULL OR @rc_timelimit < 3600
                    SET @rc_timelimit = 18000;
                ELSE IF @rc_timelimit < 18000
                    SET @rc_timelimit = CASE
                        WHEN @rc_timelimit * 2 > 28800 THEN 28800
                        ELSE @rc_timelimit * 2
                    END;
                SET @rc_rationale += N'TimeLimit adjusted (C3: time-limit exhaustion). ';
            END;

            /* Ensure a sane minimum time limit even without C3 */
            IF @rc_timelimit IS NOT NULL AND @rc_timelimit < 3600
            BEGIN
                SET @rc_timelimit = 18000;
                SET @rc_rationale += N'TimeLimit raised from <1h to 5h (too short for workload). ';
            END;

            /* W5: QS not effective -- revert to MODIFICATION_COUNTER */
            IF EXISTS (SELECT 1 FROM #recommendations WHERE Category = N'QS_NOT_EFFECTIVE')
            BEGIN
                SET @rc_sort   = N'MODIFICATION_COUNTER';
                SET @rc_qs_pri = N'N';
                SET @rc_rationale += N'QS disabled (W5: no QS data captured). ';
            END;

            /* W1a: Always recommend tiered thresholds */
            /* (no variable needed -- hardcoded as 1 in output) */

            /* W2: Long-running stats -- tighten adaptive sampling */
            IF EXISTS (SELECT 1 FROM #recommendations WHERE Category = N'LONG_RUNNING_STATS')
            BEGIN
                SET @rc_long_min = CASE
                    WHEN @rc_long_min IS NULL THEN 10
                    WHEN @rc_long_min > 10    THEN 10
                    ELSE @rc_long_min
                END;

                /* 5% for 100M+ row tables, 10% otherwise */
                IF EXISTS (
                    SELECT 1 FROM #stat_updates
                    WHERE DurationMs > @LongRunningMinutes * 60 * 1000
                    AND   ErrorNumber = 0
                    AND   RowCount_ > 100000000
                )
                    SET @rc_long_pct = 5;
                ELSE
                    SET @rc_long_pct = ISNULL(@rc_long_pct, 10);

                SET @rc_rationale += N'Adaptive sampling tightened to '
                    + CONVERT(nvarchar(10), @rc_long_min) + N'min/'
                    + CONVERT(nvarchar(10), @rc_long_pct) + N'% (W2: slow stats). ';
            END
            ELSE IF @rc_long_min IS NULL
            BEGIN
                /* Preventive: suggest enabling even without current slow stats */
                SET @rc_long_min = 15;
                SET @rc_long_pct = ISNULL(@rc_long_pct, 10);
            END;

            /* W3: Stale backlog -- suggest parallel if not already on */
            IF EXISTS (SELECT 1 FROM #recommendations WHERE Category = N'STALE_BACKLOG')
            AND ISNULL(@rc_parallel, N'N') = N'N'
            BEGIN
                SET @rc_parallel = N'Y';
                SET @rc_rationale += N'Parallel enabled (W3: persistent backlog). ';
            END;

            /* ---- Preserve features from historical runs ---- */
            IF @rc_group_join IS NULL
            AND EXISTS (SELECT 1 FROM #runs WHERE GroupByJoinPattern = N'Y')
            BEGIN
                SET @rc_group_join = N'Y';
                SET @rc_rationale += N'@GroupByJoinPattern restored from prior runs. ';
            END;

            IF @rc_filtered_mode IS NULL
            AND EXISTS (
                SELECT 1 FROM #runs
                WHERE FilteredStatsMode IS NOT NULL
                AND   FilteredStatsMode <> N'INCLUDE'
            )
            BEGIN
                SET @rc_filtered_mode = (
                    SELECT TOP (1) FilteredStatsMode
                    FROM #runs
                    WHERE FilteredStatsMode IS NOT NULL
                    AND   FilteredStatsMode <> N'INCLUDE'
                    ORDER BY StartTime DESC
                );
                SET @rc_rationale += N'@FilteredStatsMode=' + @rc_filtered_mode + N' restored from prior runs. ';
            END;

            /* ---- Build the EXEC call ---- */
            SET @rc_call = N'EXECUTE dbo.sp_StatUpdate'
                + NCHAR(13) + NCHAR(10) + N'    @Databases = N''' + @rc_databases + N''''
                + NCHAR(13) + NCHAR(10) + N'  , @TimeLimit = ' + CONVERT(nvarchar(10), ISNULL(@rc_timelimit, 18000))
                + NCHAR(13) + NCHAR(10) + N'  , @SortOrder = N''' + ISNULL(@rc_sort, N'MODIFICATION_COUNTER') + N''''
                + NCHAR(13) + NCHAR(10) + N'  , @TieredThresholds = 1'
                + NCHAR(13) + NCHAR(10) + N'  , @ModificationThreshold = ' + CONVERT(nvarchar(20), ISNULL(@rc_mod_thresh, 5000))
                + NCHAR(13) + NCHAR(10) + N'  , @CleanupOrphanedRuns = N''Y''';

            IF ISNULL(@rc_qs_pri, N'N') = N'Y'
                SET @rc_call += NCHAR(13) + NCHAR(10) + N'  , @QueryStorePriority = N''Y''';

            IF @rc_long_min IS NOT NULL
                SET @rc_call += NCHAR(13) + NCHAR(10) + N'  , @LongRunningThresholdMinutes = ' + CONVERT(nvarchar(10), @rc_long_min)
                    + NCHAR(13) + NCHAR(10) + N'  , @LongRunningSamplePercent = ' + CONVERT(nvarchar(10), ISNULL(@rc_long_pct, 10));

            IF ISNULL(@rc_parallel, N'N') = N'Y'
                SET @rc_call += NCHAR(13) + NCHAR(10) + N'  , @StatsInParallel = N''Y''';

            IF ISNULL(@rc_group_join, N'N') = N'Y'
                SET @rc_call += NCHAR(13) + NCHAR(10) + N'  , @GroupByJoinPattern = N''Y''';

            IF @rc_filtered_mode IS NOT NULL AND @rc_filtered_mode <> N'INCLUDE'
                SET @rc_call += NCHAR(13) + NCHAR(10) + N'  , @FilteredStatsMode = N''' + @rc_filtered_mode + N'''';

            SET @rc_call += N';';

            /* ---- Safeguard parameters (not tracked in CommandLog) ---- */
            SET @rc_safeguards = N'Also consider safeguards not tracked in CommandLog: @MinTempdbFreeMB = 500, @MaxConsecutiveFailures = 5';

            IF @ag_replica_role = N'PRIMARY'
                SET @rc_safeguards += N', @MaxAGRedoQueueMB = 1024';

            SET @rc_safeguards += N'.';

            /* ---- Insert the recommendation ---- */
            INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
            VALUES (
                N'INFO',
                N'RECOMMENDED_CONFIG',
                N'Recommended parameter set based on ' + CONVERT(nvarchar(10), (SELECT COUNT(*) FROM #runs))
                    + N' run(s) and ' + CONVERT(nvarchar(10), (SELECT COUNT(*) FROM #recommendations))
                    + N' diagnostic finding(s)',
                LEFT(@rc_rationale + @rc_safeguards, 2000),
                N'Balanced configuration: preserves safeguards from prior runs, adjusts parameters flagged by diagnostics, and applies best-practice defaults.  Review @TimeLimit for your maintenance window.',
                @rc_call,
                5
            );

            RAISERROR(N'  [INFO] I10: Recommended configuration built', 10, 1) WITH NOWAIT;
        END;
    END;

    RAISERROR(N'', 10, 1) WITH NOWAIT;
    RAISERROR(N'Diagnostic checks complete.', 10, 1) WITH NOWAIT;

    IF @Debug = 1
    BEGIN
        SET @debug_elapsed_ms = DATEDIFF(MILLISECOND, @debug_timer, SYSDATETIME());
        DECLARE @debug_crit integer = (SELECT COUNT_BIG(*) FROM #recommendations WHERE Severity = N'CRITICAL');
        DECLARE @debug_warn integer = (SELECT COUNT_BIG(*) FROM #recommendations WHERE Severity = N'WARNING');
        DECLARE @debug_info integer = (SELECT COUNT_BIG(*) FROM #recommendations WHERE Severity = N'INFO');
        RAISERROR(N'DEBUG: #recommendations: %i CRITICAL, %i WARNING, %i INFO', 10, 1, @debug_crit, @debug_warn, @debug_info) WITH NOWAIT;
        RAISERROR(N'DEBUG: Diagnostic checks complete (%i ms)', 10, 1, @debug_elapsed_ms) WITH NOWAIT;
    END;

    RAISERROR(N'', 10, 1) WITH NOWAIT;

    /*
    ============================================================================
    EXECUTIVE DASHBOARD -- Letter grades (A-F) with plain English headlines.
    Inspired by sp_Blitz priority system. Always returned as RS 1.

    Grading scale (each category scored 0-100):
      A = 90-100   B = 75-89   C = 60-74   D = 40-59   F = 0-39

    Categories:
      1. Overall        -- composite of all other categories
      2. Completion      -- are all qualifying stats getting updated?
      3. Reliability     -- how often do runs complete without failures or kills?
      4. Speed           -- throughput (seconds per stat) + time to critical stats
      5. Workload Focus  -- are the highest-impact stats prioritized? (QS runs)
    ============================================================================
    */
    RAISERROR(N'Computing executive dashboard...', 10, 1) WITH NOWAIT;

    DECLARE
        @score_completion integer = 0,
        @score_reliability integer = 0,
        @score_speed integer = 0,
        @score_workload integer = 50,   /* default 50 if no QS runs (neutral); #267: updated below based on run count */
        @score_overall integer = 0,
        @health_score integer = 0;

    /* Completion score: based on avg completion % across recent runs */
    IF @run_count > 0
    BEGIN
        DECLARE @avg_completion decimal(5, 1) = (
            SELECT AVG(CASE WHEN r.StatsFound > 0 THEN CONVERT(decimal(5, 1), ISNULL(r.StatsProcessed, 0) * 100.0 / r.StatsFound) ELSE 100.0 END)
            FROM #runs AS r WHERE r.IsKilled = 0
        );
        SET @score_completion = CASE
            WHEN @avg_completion >= 95 THEN 100
            WHEN @avg_completion >= 85 THEN 90
            WHEN @avg_completion >= 70 THEN 75
            WHEN @avg_completion >= 50 THEN 60
            WHEN @avg_completion >= 30 THEN 40
            ELSE 20
        END;

        /* #238: If majority of runs hit TIME_LIMIT, cap completion score --
           a 95% avg looks great but means nothing if the window is too short */
        DECLARE @tl_run_pct decimal(5, 1) = @time_limit_runs * 100.0 / @run_count;

        IF @tl_run_pct >= 80.0 AND @score_completion > 60
            SET @score_completion = 60;     /* Can't exceed C if 80%+ runs hit time limit */
        ELSE IF @tl_run_pct >= 50.0 AND @score_completion > 75
            SET @score_completion = 75;     /* Can't exceed B if 50%+ runs hit time limit */
    END;

    /* Reliability score: penalize killed runs, failures, time limit exhaustion */
    IF @run_count > 0
    BEGIN
        DECLARE
            @kill_pct decimal(5, 1) = @killed_count * 100.0 / @run_count,
            @fail_pct decimal(5, 1) = (SELECT COUNT_BIG(*) FROM #stat_updates WHERE ErrorNumber > 0) * 100.0 / NULLIF(@stat_update_count, 0),
            @tl_pct decimal(5, 1) = @time_limit_runs * 100.0 / @run_count;

        SET @score_reliability = 100
            - CONVERT(integer, ISNULL(@kill_pct, 0) * 3)     /* heavy penalty for killed runs */
            - CONVERT(integer, ISNULL(@fail_pct, 0) * 2)     /* moderate penalty for failures */
            - CONVERT(integer, ISNULL(@tl_pct, 0) * 1.5);    /* C3 is CRITICAL -- stronger penalty for chronic time limit exhaustion */

        IF @score_reliability < 0 SET @score_reliability = 0;
        IF @score_reliability > 100 SET @score_reliability = 100;
    END;

    /* Speed score: based on avg seconds per stat (lower is better) */
    IF EXISTS (SELECT 1 FROM #runs WHERE IsKilled = 0 AND StatsProcessed > 0)
    BEGIN
        DECLARE @avg_sec_per_stat decimal(10, 1) = (
            SELECT AVG(CONVERT(decimal(10, 1), r.DurationSeconds * 1.0 / r.StatsProcessed))
            FROM #runs AS r WHERE r.IsKilled = 0 AND r.StatsProcessed > 0
        );
        SET @score_speed = CASE
            WHEN @avg_sec_per_stat <= 0.5 THEN 100
            WHEN @avg_sec_per_stat <= 1.0 THEN 90
            WHEN @avg_sec_per_stat <= 2.0 THEN 80
            WHEN @avg_sec_per_stat <= 5.0 THEN 70
            WHEN @avg_sec_per_stat <= 10.0 THEN 55
            WHEN @avg_sec_per_stat <= 30.0 THEN 40
            ELSE 20
        END;
    END;

    /* Workload Focus score: how well QS prioritization is working */
    IF @qs_run_count > 0
    BEGIN
        DECLARE @avg_workload_cov decimal(5, 1) = (
            SELECT AVG(e.WorkloadCoveragePct) FROM #qs_efficacy AS e WHERE e.IsQSRun = 1 AND e.WorkloadCoveragePct IS NOT NULL
        );
        DECLARE @avg_q1_pct_score decimal(5, 1) = (
            SELECT AVG(e.HighCpuInFirstQuartilePct) FROM #qs_efficacy AS e WHERE e.IsQSRun = 1 AND e.HighCpuInFirstQuartilePct IS NOT NULL
        );
        SET @score_workload = CONVERT(integer, (ISNULL(@avg_workload_cov, 50) + ISNULL(@avg_q1_pct_score, 50)) / 2.0);
        IF @score_workload > 100 SET @score_workload = 100;
    END
    ELSE IF @run_count >= 10
    BEGIN
        /* #267: No QS runs but enough data to know -- grade D (35) indicating missing feature */
        SET @score_workload = 35;
    END;

    /* Apply grade overrides: forced grades map to fixed scores (A=95, B=82, C=67, D=50, F=20) */
    DECLARE
        @effective_completion  integer = @score_completion,
        @effective_reliability integer = @score_reliability,
        @effective_speed       integer = @score_speed,
        @effective_workload    integer = @score_workload;

    IF @override_completion  IN ('A','B','C','D','F') SET @effective_completion  = CASE @override_completion  WHEN 'A' THEN 95 WHEN 'B' THEN 82 WHEN 'C' THEN 67 WHEN 'D' THEN 50 ELSE 20 END;
    IF @override_reliability IN ('A','B','C','D','F') SET @effective_reliability = CASE @override_reliability WHEN 'A' THEN 95 WHEN 'B' THEN 82 WHEN 'C' THEN 67 WHEN 'D' THEN 50 ELSE 20 END;
    IF @override_speed       IN ('A','B','C','D','F') SET @effective_speed       = CASE @override_speed       WHEN 'A' THEN 95 WHEN 'B' THEN 82 WHEN 'C' THEN 67 WHEN 'D' THEN 50 ELSE 20 END;
    IF @override_workload    IN ('A','B','C','D','F') SET @effective_workload    = CASE @override_workload    WHEN 'A' THEN 95 WHEN 'B' THEN 82 WHEN 'C' THEN 67 WHEN 'D' THEN 50 ELSE 20 END;

    /* Overall = weighted average (defaults: COMPLETION=30%, RELIABILITY=25%, SPEED=20%, WORKLOAD=25%;
       overridden by @GradeWeights, normalized to sum to 1.0; IGNOREd categories get weight 0) */
    SET @score_overall = CONVERT(integer,
        @effective_completion  * @weight_completion
        + @effective_reliability * @weight_reliability
        + @effective_speed       * @weight_speed
        + @effective_workload    * @weight_workload
    );
    SET @health_score = @score_overall;

    /* GAP-F: Compute OVERALL trend from history table (if available) */
    DECLARE @prior_health integer = NULL;
    DECLARE @overall_trend nvarchar(20) = NULL;

    IF @SkipHistory = 0 AND @history_exists = 1
    BEGIN
        SET @sql = N'SELECT TOP (1) @ph = h.HealthScore FROM ' + @history_ref + N' AS h ORDER BY h.CapturedAt DESC;';
        EXECUTE sys.sp_executesql @sql, N'@ph integer OUTPUT', @ph = @prior_health OUTPUT;

        IF @prior_health IS NOT NULL
            SET @overall_trend = CASE
                WHEN @score_overall >= @prior_health + 5 THEN N'IMPROVING'
                WHEN @score_overall <= @prior_health - 5 THEN N'DEGRADING'
                ELSE N'STABLE'
            END;
    END;

    /* Populate dashboard rows.
       Override logic: IGNORE -> Grade='-', Score=0, headline prefixed '[IGNORED]'.
       Forced grade -> use forced grade/score, headline prefixed '[OVERRIDE: X]', detail shows actual score.
       OVERALL appends '[Overrides active]' when any override is in effect. */
    INSERT INTO #executive_dashboard (Category, Grade, Score, Headline, Detail, Trend, SortOrder)
    VALUES
    (
        N'OVERALL',
        CASE WHEN @score_overall >= 90 THEN 'A' WHEN @score_overall >= 75 THEN 'B' WHEN @score_overall >= 60 THEN 'C' WHEN @score_overall >= 40 THEN 'D' ELSE 'F' END,
        @score_overall,
        CASE
            WHEN @score_overall >= 90 THEN N'Statistics maintenance is running excellently. No immediate action needed.'
            WHEN @score_overall >= 75 THEN N'Statistics maintenance is healthy with minor opportunities for improvement.'
            WHEN @score_overall >= 60 THEN N'Statistics maintenance is functional but has notable gaps. Review recommendations below.'
            WHEN @score_overall >= 40 THEN N'Statistics maintenance needs attention. Multiple issues affecting database performance.'
            ELSE N'Statistics maintenance has critical issues requiring immediate action.'
        END
            + CASE WHEN @overall_trend IS NOT NULL THEN N' (' + @overall_trend + N' from prior assessment: ' + CONVERT(nvarchar(10), @prior_health) + N' -> ' + CONVERT(nvarchar(10), @score_overall) + N')' ELSE N'' END
            + CASE WHEN @has_overrides = 1 THEN N' [Overrides active]' ELSE N'' END,
        CONVERT(nvarchar(10), @run_count) + N' runs analyzed over ' + CONVERT(nvarchar(10), @DaysBack) + N' days. '
            + CONVERT(nvarchar(10), @stat_update_count) + N' individual stat updates. '
            + CASE WHEN @qs_run_count > 0 THEN CONVERT(nvarchar(10), @qs_run_count) + N' runs using Query Store prioritization.' ELSE N'Query Store prioritization not in use.' END,
        @overall_trend,
        1
    ),
    (
        N'COMPLETION',
        CASE WHEN @override_completion = 'X' THEN '-'
             WHEN @override_completion IN ('A','B','C','D','F') THEN @override_completion
             WHEN @score_completion >= 90 THEN 'A' WHEN @score_completion >= 75 THEN 'B' WHEN @score_completion >= 60 THEN 'C' WHEN @score_completion >= 40 THEN 'D' ELSE 'F' END,
        CASE WHEN @override_completion = 'X' THEN 0 ELSE @score_completion END,
        CASE WHEN @override_completion = 'X' THEN N'[IGNORED] '
             WHEN @override_completion IN ('A','B','C','D','F') THEN N'[OVERRIDE: ' + @override_completion + N'] '
             ELSE N'' END
            + CASE
                WHEN @score_completion >= 90 THEN N'Nearly all qualifying statistics are being updated each run.'
                WHEN @score_completion >= 75 AND @tl_run_pct >= 50.0
                    THEN N'Most statistics are updated, but ' + CONVERT(nvarchar(10), @time_limit_runs)
                         + N' of ' + CONVERT(nvarchar(10), @run_count) + N' runs hit the time limit.'
                WHEN @score_completion >= 75 THEN N'Most statistics are being updated, but some are consistently left behind.'
                WHEN @score_completion >= 60 AND @tl_run_pct >= 50.0
                    THEN N'Maintenance window is too short -- ' + CONVERT(nvarchar(10), @time_limit_runs)
                         + N' of ' + CONVERT(nvarchar(10), @run_count) + N' runs hit the time limit before finishing.'
                WHEN @score_completion >= 60 THEN N'A significant portion of statistics are not being reached within the maintenance window.'
                WHEN @score_completion >= 40 THEN N'Less than half of qualifying statistics are being updated. Increase time limit or reduce scope.'
                ELSE N'Very few statistics are being updated. Maintenance window is severely undersized for the workload.'
            END,
        N'Average completion rate: ' + ISNULL(CONVERT(nvarchar(10), @avg_completion), N'N/A') + N'% of qualifying statistics updated per run.'
            + CASE WHEN @override_completion IN ('A','B','C','D','F') THEN N' (actual score: ' + CONVERT(nvarchar(10), @score_completion) + N')' ELSE N'' END,
        NULL,
        2
    ),
    (
        N'RELIABILITY',
        CASE WHEN @override_reliability = 'X' THEN '-'
             WHEN @override_reliability IN ('A','B','C','D','F') THEN @override_reliability
             WHEN @score_reliability >= 90 THEN 'A' WHEN @score_reliability >= 75 THEN 'B' WHEN @score_reliability >= 60 THEN 'C' WHEN @score_reliability >= 40 THEN 'D' ELSE 'F' END,
        CASE WHEN @override_reliability = 'X' THEN 0 ELSE @score_reliability END,
        CASE WHEN @override_reliability = 'X' THEN N'[IGNORED] '
             WHEN @override_reliability IN ('A','B','C','D','F') THEN N'[OVERRIDE: ' + @override_reliability + N'] '
             ELSE N'' END
            + CASE
                WHEN @killed_count > 0 AND @score_reliability < 60 THEN CONVERT(nvarchar(10), @killed_count) + N' run(s) were killed before completing. Check SQL Agent job history for failures.'
                WHEN @killed_count > 0 THEN CONVERT(nvarchar(10), @killed_count) + N' run(s) were killed, but overall reliability is acceptable.'
                WHEN @score_reliability >= 90 THEN N'All runs completed normally without errors or kills.'
                ELSE N'Runs are completing but experiencing errors or consistently hitting time limits.'
            END,
        N'Killed runs: ' + CONVERT(nvarchar(10), @killed_count)
            + N'. Failed stat updates: ' + CONVERT(nvarchar(10), (SELECT COUNT_BIG(*) FROM #stat_updates WHERE ErrorNumber > 0))
            + N'. Time-limited runs: ' + CONVERT(nvarchar(10), @time_limit_runs) + N'.'
            + CASE WHEN @override_reliability IN ('A','B','C','D','F') THEN N' (actual score: ' + CONVERT(nvarchar(10), @score_reliability) + N')' ELSE N'' END,
        NULL,
        3
    ),
    (
        N'SPEED',
        CASE WHEN @override_speed = 'X' THEN '-'
             WHEN @override_speed IN ('A','B','C','D','F') THEN @override_speed
             WHEN @score_speed >= 90 THEN 'A' WHEN @score_speed >= 75 THEN 'B' WHEN @score_speed >= 60 THEN 'C' WHEN @score_speed >= 40 THEN 'D' ELSE 'F' END,
        CASE WHEN @override_speed = 'X' THEN 0 ELSE @score_speed END,
        CASE WHEN @override_speed = 'X' THEN N'[IGNORED] '
             WHEN @override_speed IN ('A','B','C','D','F') THEN N'[OVERRIDE: ' + @override_speed + N'] '
             ELSE N'' END
            + CASE
                WHEN @score_speed >= 90 THEN N'Statistics are being updated very quickly (' + ISNULL(CONVERT(nvarchar(20), @avg_sec_per_stat), N'<1') + N' sec/stat average).'
                WHEN @score_speed >= 75 THEN N'Update speed is good at ' + ISNULL(CONVERT(nvarchar(20), @avg_sec_per_stat), N'?') + N' sec/stat average.'
                WHEN @score_speed >= 60 THEN N'Update speed is moderate at ' + ISNULL(CONVERT(nvarchar(20), @avg_sec_per_stat), N'?') + N' sec/stat. Consider parallel mode or adaptive sampling.'
                ELSE N'Updates are slow at ' + ISNULL(CONVERT(nvarchar(20), @avg_sec_per_stat), N'?') + N' sec/stat. Investigate I/O, table sizes, and sample rates.'
            END,
        N'Average seconds per stat update: ' + ISNULL(CONVERT(nvarchar(20), @avg_sec_per_stat), N'N/A')
            + N'. Average run duration: ' + ISNULL(CONVERT(nvarchar(10), (SELECT AVG(DurationSeconds) / 60 FROM #runs WHERE IsKilled = 0)), N'N/A') + N' minutes.'
            + CASE WHEN @override_speed IN ('A','B','C','D','F') THEN N' (actual score: ' + CONVERT(nvarchar(10), @score_speed) + N')' ELSE N'' END,
        NULL,
        4
    ),
    (
        N'WORKLOAD FOCUS',
        CASE WHEN @override_workload = 'X' THEN '-'
             WHEN @override_workload IN ('A','B','C','D','F') THEN @override_workload
             /* #267: insufficient data for workload grade when <10 runs and no QS */
             WHEN @qs_run_count = 0 AND @run_count < 10 THEN '-'
             WHEN @score_workload >= 90 THEN 'A' WHEN @score_workload >= 75 THEN 'B' WHEN @score_workload >= 60 THEN 'C' WHEN @score_workload >= 40 THEN 'D' ELSE 'F' END,
        CASE WHEN @override_workload = 'X' THEN 0 WHEN @qs_run_count = 0 AND @run_count < 10 THEN 0 ELSE @score_workload END,
        CASE WHEN @override_workload = 'X' THEN N'[IGNORED] '
             WHEN @override_workload IN ('A','B','C','D','F') THEN N'[OVERRIDE: ' + @override_workload + N'] '
             ELSE N'' END
            + CASE
                /* #267: Differentiate no-QS headline based on data sufficiency */
                WHEN @qs_run_count = 0 AND @run_count < 10 THEN N'Insufficient data to evaluate workload-aware prioritization.'
                WHEN @qs_run_count = 0 THEN N'No workload-aware prioritization configured. Statistics are updated by modification count, not workload impact.'
                WHEN @score_workload >= 90 THEN N'Query Store prioritization is highly effective. The most performance-critical statistics are updated first.'
                WHEN @score_workload >= 75 THEN N'Query Store prioritization is working well. Most high-impact statistics are prioritized.'
                ELSE N'Query Store prioritization is enabled but not fully effective. Check Query Store health on target databases.'
            END,
        CASE
            WHEN @qs_run_count > 0 THEN N'QS runs: ' + CONVERT(nvarchar(10), @qs_run_count)
                + N'. Avg workload coverage: ' + ISNULL(CONVERT(nvarchar(10), @avg_workload_cov), N'N/A') + N'%'
                + N'. High-CPU stats in first quartile: ' + ISNULL(CONVERT(nvarchar(10), @avg_q1_pct_score), N'N/A') + N'%.'
            ELSE N'Enable with: @QueryStorePriority = N''Y'', @SortOrder = N''QUERY_STORE'''
        END
            + CASE WHEN @override_workload IN ('A','B','C','D','F') THEN N' (actual score: ' + CONVERT(nvarchar(10), @score_workload) + N')' ELSE N'' END,
        NULL,
        5
    );

    RAISERROR(N'  Health score: %i / 100', 10, 1, @health_score) WITH NOWAIT;

    /*
    ============================================================================
    PERSIST TO HISTORY TABLE (incremental insert for new runs only)
    ============================================================================
    */
    IF @SkipHistory = 0 AND @history_exists = 1 AND @Obfuscate = 0
    BEGIN
        DECLARE @new_max_id integer = (SELECT ISNULL(MAX(ID), 0) FROM #stat_updates);

        /* BUG-C fix: Compute per-run HealthScore inline instead of using aggregate @health_score.
           Each run gets its own score based on its completion, speed, and workload metrics.
           Grading scale: A=90-100, B=75-89, C=60-74, D=40-59, F=0-39 */
        SET @sql = N'
            INSERT INTO ' + @history_ref + N'
            (MaxCommandLogID, RunLabel, StartTime, HealthScore, OverallGrade,
             CompletionPct, AvgSecPerStat, WorkloadCoveragePct, HighCpuFirstQuartilePct,
             MinutesToHighCpuComplete, StatsFound, StatsProcessed, StatsFailed,
             StopReason, DurationSeconds, IsQSRun, DiagVersion)
            SELECT
                @max_id,
                e.RunLabel,
                e.StartTime,
                /* Per-run HealthScore: weighted composite of completion + speed + workload focus */
                per_run_health = CONVERT(integer,
                    /* Completion 40% */
                    CASE
                        WHEN ISNULL(e.CompletionPct, 0) >= 95 THEN 100
                        WHEN ISNULL(e.CompletionPct, 0) >= 85 THEN 90
                        WHEN ISNULL(e.CompletionPct, 0) >= 70 THEN 75
                        WHEN ISNULL(e.CompletionPct, 0) >= 50 THEN 60
                        WHEN ISNULL(e.CompletionPct, 0) >= 30 THEN 40
                        ELSE 20
                    END * 0.40
                    /* Speed 30% */
                    + CASE
                        WHEN ISNULL(e.AvgSecPerStat, 999) <= 0.5 THEN 100
                        WHEN ISNULL(e.AvgSecPerStat, 999) <= 1.0 THEN 90
                        WHEN ISNULL(e.AvgSecPerStat, 999) <= 2.0 THEN 80
                        WHEN ISNULL(e.AvgSecPerStat, 999) <= 5.0 THEN 70
                        WHEN ISNULL(e.AvgSecPerStat, 999) <= 10.0 THEN 55
                        WHEN ISNULL(e.AvgSecPerStat, 999) <= 30.0 THEN 40
                        ELSE 20
                    END * 0.30
                    /* Workload Focus 30% (neutral 50 if no QS data) */
                    + CASE
                        WHEN e.IsQSRun = 0 OR e.HighCpuInFirstQuartilePct IS NULL THEN 50
                        ELSE CONVERT(integer, (ISNULL(e.WorkloadCoveragePct, 50) + ISNULL(e.HighCpuInFirstQuartilePct, 50)) / 2.0)
                    END * 0.30
                ),
                per_run_grade = CASE
                    WHEN CONVERT(integer,
                        CASE WHEN ISNULL(e.CompletionPct, 0) >= 95 THEN 100 WHEN ISNULL(e.CompletionPct, 0) >= 85 THEN 90
                             WHEN ISNULL(e.CompletionPct, 0) >= 70 THEN 75 WHEN ISNULL(e.CompletionPct, 0) >= 50 THEN 60
                             WHEN ISNULL(e.CompletionPct, 0) >= 30 THEN 40 ELSE 20 END * 0.40
                        + CASE WHEN ISNULL(e.AvgSecPerStat, 999) <= 0.5 THEN 100 WHEN ISNULL(e.AvgSecPerStat, 999) <= 1.0 THEN 90
                               WHEN ISNULL(e.AvgSecPerStat, 999) <= 2.0 THEN 80 WHEN ISNULL(e.AvgSecPerStat, 999) <= 5.0 THEN 70
                               WHEN ISNULL(e.AvgSecPerStat, 999) <= 10.0 THEN 55 WHEN ISNULL(e.AvgSecPerStat, 999) <= 30.0 THEN 40 ELSE 20 END * 0.30
                        + CASE WHEN e.IsQSRun = 0 OR e.HighCpuInFirstQuartilePct IS NULL THEN 50
                               ELSE CONVERT(integer, (ISNULL(e.WorkloadCoveragePct, 50) + ISNULL(e.HighCpuInFirstQuartilePct, 50)) / 2.0) END * 0.30
                    ) >= 90 THEN ''A'' WHEN CONVERT(integer,
                        CASE WHEN ISNULL(e.CompletionPct, 0) >= 95 THEN 100 WHEN ISNULL(e.CompletionPct, 0) >= 85 THEN 90
                             WHEN ISNULL(e.CompletionPct, 0) >= 70 THEN 75 WHEN ISNULL(e.CompletionPct, 0) >= 50 THEN 60
                             WHEN ISNULL(e.CompletionPct, 0) >= 30 THEN 40 ELSE 20 END * 0.40
                        + CASE WHEN ISNULL(e.AvgSecPerStat, 999) <= 0.5 THEN 100 WHEN ISNULL(e.AvgSecPerStat, 999) <= 1.0 THEN 90
                               WHEN ISNULL(e.AvgSecPerStat, 999) <= 2.0 THEN 80 WHEN ISNULL(e.AvgSecPerStat, 999) <= 5.0 THEN 70
                               WHEN ISNULL(e.AvgSecPerStat, 999) <= 10.0 THEN 55 WHEN ISNULL(e.AvgSecPerStat, 999) <= 30.0 THEN 40 ELSE 20 END * 0.30
                        + CASE WHEN e.IsQSRun = 0 OR e.HighCpuInFirstQuartilePct IS NULL THEN 50
                               ELSE CONVERT(integer, (ISNULL(e.WorkloadCoveragePct, 50) + ISNULL(e.HighCpuInFirstQuartilePct, 50)) / 2.0) END * 0.30
                    ) >= 75 THEN ''B'' WHEN CONVERT(integer,
                        CASE WHEN ISNULL(e.CompletionPct, 0) >= 95 THEN 100 WHEN ISNULL(e.CompletionPct, 0) >= 85 THEN 90
                             WHEN ISNULL(e.CompletionPct, 0) >= 70 THEN 75 WHEN ISNULL(e.CompletionPct, 0) >= 50 THEN 60
                             WHEN ISNULL(e.CompletionPct, 0) >= 30 THEN 40 ELSE 20 END * 0.40
                        + CASE WHEN ISNULL(e.AvgSecPerStat, 999) <= 0.5 THEN 100 WHEN ISNULL(e.AvgSecPerStat, 999) <= 1.0 THEN 90
                               WHEN ISNULL(e.AvgSecPerStat, 999) <= 2.0 THEN 80 WHEN ISNULL(e.AvgSecPerStat, 999) <= 5.0 THEN 70
                               WHEN ISNULL(e.AvgSecPerStat, 999) <= 10.0 THEN 55 WHEN ISNULL(e.AvgSecPerStat, 999) <= 30.0 THEN 40 ELSE 20 END * 0.30
                        + CASE WHEN e.IsQSRun = 0 OR e.HighCpuInFirstQuartilePct IS NULL THEN 50
                               ELSE CONVERT(integer, (ISNULL(e.WorkloadCoveragePct, 50) + ISNULL(e.HighCpuInFirstQuartilePct, 50)) / 2.0) END * 0.30
                    ) >= 60 THEN ''C'' WHEN CONVERT(integer,
                        CASE WHEN ISNULL(e.CompletionPct, 0) >= 95 THEN 100 WHEN ISNULL(e.CompletionPct, 0) >= 85 THEN 90
                             WHEN ISNULL(e.CompletionPct, 0) >= 70 THEN 75 WHEN ISNULL(e.CompletionPct, 0) >= 50 THEN 60
                             WHEN ISNULL(e.CompletionPct, 0) >= 30 THEN 40 ELSE 20 END * 0.40
                        + CASE WHEN ISNULL(e.AvgSecPerStat, 999) <= 0.5 THEN 100 WHEN ISNULL(e.AvgSecPerStat, 999) <= 1.0 THEN 90
                               WHEN ISNULL(e.AvgSecPerStat, 999) <= 2.0 THEN 80 WHEN ISNULL(e.AvgSecPerStat, 999) <= 5.0 THEN 70
                               WHEN ISNULL(e.AvgSecPerStat, 999) <= 10.0 THEN 55 WHEN ISNULL(e.AvgSecPerStat, 999) <= 30.0 THEN 40 ELSE 20 END * 0.30
                        + CASE WHEN e.IsQSRun = 0 OR e.HighCpuInFirstQuartilePct IS NULL THEN 50
                               ELSE CONVERT(integer, (ISNULL(e.WorkloadCoveragePct, 50) + ISNULL(e.HighCpuInFirstQuartilePct, 50)) / 2.0) END * 0.30
                    ) >= 40 THEN ''D'' ELSE ''F'' END,
                e.CompletionPct,
                e.AvgSecPerStat,
                e.WorkloadCoveragePct,
                e.HighCpuInFirstQuartilePct,
                e.MinutesToHighCpuComplete,
                e.StatsFound,
                e.StatsProcessed,
                r.StatsFailed,
                e.StopReason,
                e.DurationSeconds,
                e.IsQSRun,
                @version
            FROM #qs_efficacy AS e
            INNER JOIN #runs AS r ON r.RunLabel = e.RunLabel
            WHERE NOT EXISTS (
                SELECT 1 FROM ' + @history_ref + N' AS h
                WHERE h.RunLabel = e.RunLabel
            );
        ';

        DECLARE @rows_inserted integer;

        EXECUTE sys.sp_executesql
            @sql,
            N'@max_id integer, @version varchar(20)',
            @max_id = @new_max_id,
            @version = @procedure_version;

        SET @rows_inserted = @@ROWCOUNT;

        IF @rows_inserted > 0
            RAISERROR(N'  Persisted %i new run(s) to history table', 10, 1, @rows_inserted) WITH NOWAIT;
    END;

    RAISERROR(N'', 10, 1) WITH NOWAIT;

    /* #270: When ExpertMode=0 and QS data exists, enrich CRITICAL/WARNING evidence with workload context */
    IF @ExpertMode = 0 AND @workload_impact_count > 0
    BEGIN
        UPDATE rec
        SET Evidence = rec.Evidence
            + N' [Workload context: '
            + CONVERT(nvarchar(10), wi_stats.high_impact_count) + N' of '
            + CONVERT(nvarchar(10), @workload_impact_count) + N' tracked stats are high-impact (top 10% by CPU).]'
        FROM #recommendations AS rec
        CROSS APPLY (
            SELECT high_impact_count = COUNT_BIG(*)
            FROM #workload_impact AS wi
            WHERE wi.ImpactScore >= 0.90
        ) AS wi_stats
        WHERE rec.Severity IN (N'CRITICAL', N'WARNING')
        AND   wi_stats.high_impact_count > 0;
    END;

    IF @Debug = 1
    BEGIN
        SET @debug_elapsed_ms = DATEDIFF(MILLISECOND, @debug_timer, SYSDATETIME());
        RAISERROR(N'DEBUG: #executive_dashboard contents', 10, 1) WITH NOWAIT;
        SELECT * FROM #executive_dashboard;
        RAISERROR(N'DEBUG: Dashboard computation complete (%i ms)', 10, 1, @debug_elapsed_ms) WITH NOWAIT;
    END;

    /*
    ============================================================================
    RESULT SET 1: EXECUTIVE DASHBOARD (always returned, even with @ExpertMode=0)
    ============================================================================
    */
    IF @SingleResultSet = 0
    BEGIN
        SELECT
            Category,
            Grade,
            Score,
            Headline,
            Detail,
            Trend
        FROM #executive_dashboard
        ORDER BY SortOrder;
    END
    ELSE
    BEGIN
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT
            ResultSetID   = 1,
            ResultSetName = N'Executive Dashboard',
            RowNum        = ROW_NUMBER() OVER (ORDER BY ed.SortOrder),
            RowData       = (
                SELECT
                    Category = ed2.Category,
                    Grade    = ed2.Grade,
                    Score    = ed2.Score,
                    Headline = ed2.Headline,
                    Detail   = ed2.Detail,
                    Trend    = ed2.Trend
                FROM #executive_dashboard AS ed2
                WHERE ed2.Category = ed.Category
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM #executive_dashboard AS ed;
    END;

    /*
    ============================================================================
    RESULT SET 2: RECOMMENDATIONS (always returned)
    ============================================================================
    */
    IF @SingleResultSet = 0
    BEGIN
        SELECT
            Severity,
            Category,
            Finding,
            Evidence,
            Recommendation,
            ExampleCall,
            /* #283: One-line summary for quick scanning */
            Summary = N'[' + Severity + N'] ' + Finding
                + CASE WHEN Recommendation IS NOT NULL THEN N' -- ' + LEFT(Recommendation, 200) ELSE N'' END
                + CASE WHEN ExampleCall IS NOT NULL THEN N' Fix: ' + ExampleCall ELSE N'' END
        FROM #recommendations
        ORDER BY
            CASE Severity
                WHEN N'CRITICAL' THEN 1
                WHEN N'WARNING'  THEN 2
                WHEN N'INFO'     THEN 3
                ELSE 4
            END,
            SortPriority,
            FindingID;
    END
    ELSE
    BEGIN
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT
            ResultSetID   = 2,
            ResultSetName = N'Recommendations',
            RowNum        = ROW_NUMBER() OVER (
                                ORDER BY
                                    CASE r.Severity
                                        WHEN N'CRITICAL' THEN 1
                                        WHEN N'WARNING'  THEN 2
                                        WHEN N'INFO'     THEN 3
                                        ELSE 4
                                    END,
                                    r.SortPriority,
                                    r.FindingID
                            ),
            RowData       = (
                SELECT
                    Severity       = r2.Severity,
                    Category       = r2.Category,
                    Finding        = r2.Finding,
                    Evidence       = r2.Evidence,
                    Recommendation = r2.Recommendation,
                    ExampleCall    = r2.ExampleCall,
                    Summary        = N'[' + r2.Severity + N'] ' + r2.Finding
                        + CASE WHEN r2.Recommendation IS NOT NULL THEN N' -- ' + LEFT(r2.Recommendation, 200) ELSE N'' END
                        + CASE WHEN r2.ExampleCall IS NOT NULL THEN N' Fix: ' + r2.ExampleCall ELSE N'' END
                FROM #recommendations AS r2
                WHERE r2.FindingID = r.FindingID
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM #recommendations AS r;
    END;

    /*
    ============================================================================
    RESULT SET 3: RUN HEALTH SUMMARY (@ExpertMode = 1 only)
    ============================================================================
    */
    IF @ExpertMode = 1 AND @SingleResultSet = 0
    BEGIN
        SELECT
            TotalRuns = @run_count,
            CompletedRuns = @total_completed_runs,
            KilledRuns = @killed_count,
            CompletionPct = CASE
                WHEN @run_count > 0
                THEN CONVERT(decimal(5, 1), @total_completed_runs * 100.0 / @run_count)
                ELSE 0
            END,
            TimeLimitedRuns = @time_limit_runs,
            NaturalEndRuns = (SELECT COUNT_BIG(*) FROM #runs WHERE StopReason = N'COMPLETED'),
            AvgDurationSec = (SELECT AVG(DurationSeconds) FROM #runs WHERE IsKilled = 0),
            AvgStatsProcessed = (SELECT AVG(StatsProcessed) FROM #runs WHERE IsKilled = 0),
            AvgStatsRemaining = (SELECT AVG(StatsRemaining) FROM #runs WHERE IsKilled = 0),
            TotalStatUpdates = @stat_update_count,
            TotalFailedUpdates = (SELECT COUNT_BIG(*) FROM #stat_updates WHERE ErrorNumber > 0),
            AnalysisWindowDays = @DaysBack,
            StopReasonDistribution = (
                SELECT STRING_AGG(stop_summary, N', ')
                FROM (
                    SELECT r.StopReason + N': ' + CONVERT(nvarchar(10), COUNT_BIG(*)) AS stop_summary
                    FROM #runs AS r
                    WHERE r.IsKilled = 0
                    AND   r.StopReason IS NOT NULL
                    GROUP BY r.StopReason
                ) AS sr
            ),
            HealthScore = @health_score,
            QSRunCount = (SELECT COUNT_BIG(*) FROM #qs_efficacy WHERE IsQSRun = 1),
            AvgWorkloadCoveragePct = (SELECT AVG(WorkloadCoveragePct) FROM #qs_efficacy WHERE IsQSRun = 1),
            LatestHighCpuFirstQuartilePct = (SELECT TOP (1) HighCpuInFirstQuartilePct FROM #qs_efficacy WHERE IsQSRun = 1 ORDER BY StartTime DESC),
            ReplicaRole = @ag_replica_role;
    END
    ELSE IF @ExpertMode = 1
    BEGIN
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT
            ResultSetID   = 3,
            ResultSetName = N'Run Health Summary',
            RowNum        = 1,
            RowData       = (
                SELECT
                    TotalRuns              = @run_count,
                    CompletedRuns          = @total_completed_runs,
                    KilledRuns             = @killed_count,
                    CompletionPct          = CASE
                                                WHEN @run_count > 0
                                                THEN CONVERT(decimal(5, 1), @total_completed_runs * 100.0 / @run_count)
                                                ELSE 0
                                            END,
                    TimeLimitedRuns        = @time_limit_runs,
                    NaturalEndRuns         = (SELECT COUNT_BIG(*) FROM #runs WHERE StopReason = N'COMPLETED'),
                    AvgDurationSec         = (SELECT AVG(DurationSeconds) FROM #runs WHERE IsKilled = 0),
                    AvgStatsProcessed      = (SELECT AVG(StatsProcessed) FROM #runs WHERE IsKilled = 0),
                    AvgStatsRemaining      = (SELECT AVG(StatsRemaining) FROM #runs WHERE IsKilled = 0),
                    TotalStatUpdates       = @stat_update_count,
                    TotalFailedUpdates     = (SELECT COUNT_BIG(*) FROM #stat_updates WHERE ErrorNumber > 0),
                    AnalysisWindowDays     = @DaysBack,
                    HealthScore            = @health_score,
                    StopReasonDistribution = (
                        SELECT STRING_AGG(stop_summary, N', ')
                        FROM (
                            SELECT r.StopReason + N': ' + CONVERT(nvarchar(10), COUNT_BIG(*)) AS stop_summary
                            FROM #runs AS r
                            WHERE r.IsKilled = 0
                            AND   r.StopReason IS NOT NULL
                            GROUP BY r.StopReason
                        ) AS sr
                    ),
                    QSRunCount = (SELECT COUNT_BIG(*) FROM #qs_efficacy WHERE IsQSRun = 1),
                    AvgWorkloadCoveragePct = (SELECT AVG(WorkloadCoveragePct) FROM #qs_efficacy WHERE IsQSRun = 1),
                    LatestHighCpuFirstQuartilePct = (SELECT TOP (1) HighCpuInFirstQuartilePct FROM #qs_efficacy WHERE IsQSRun = 1 ORDER BY StartTime DESC),
                    ReplicaRole = @ag_replica_role
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            );
    END;

    /*
    ============================================================================
    RESULT SET 4: RUN DETAIL (@ExpertMode = 1 only)
    ============================================================================
    */
    IF @ExpertMode = 1 AND @SingleResultSet = 0
    BEGIN
        SELECT
            r.RunLabel,
            r.StartTime,
            r.EndTime,
            r.DurationSeconds,
            DurationMinutes = r.DurationSeconds / 60,
            r.StopReason,
            r.StatsFound,
            r.StatsProcessed,
            r.StatsSucceeded,
            r.StatsFailed,
            r.StatsRemaining,
            AvgSecPerStat = CASE
                WHEN r.StatsProcessed > 0
                THEN CONVERT(decimal(10, 1), r.DurationSeconds * 1.0 / r.StatsProcessed)
                ELSE NULL
            END,
            r.IsKilled,
            r.[Version],
            r.[Databases],
            r.TimeLimit,
            r.ModificationThreshold,
            r.TieredThresholds,
            r.SortOrder,
            r.QueryStorePriority,
            r.StatsInParallel,
            r.Preset,
            r.LongRunningThresholdMinutes,
            WorkloadCoveragePct = e.WorkloadCoveragePct,
            HighCpuInFirstQuartilePct = e.HighCpuInFirstQuartilePct,
            MinutesToHighCpuComplete = e.MinutesToHighCpuComplete
        FROM #runs AS r
        LEFT JOIN #qs_efficacy AS e
            ON e.RunLabel = r.RunLabel
        ORDER BY r.StartTime DESC;
    END
    ELSE IF @ExpertMode = 1
    BEGIN
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT
            ResultSetID   = 4,
            ResultSetName = N'Run Detail',
            RowNum        = ROW_NUMBER() OVER (ORDER BY r.StartTime DESC),
            RowData       = (
                SELECT
                    RunLabel                = r2.RunLabel,
                    StartTime               = r2.StartTime,
                    EndTime                 = r2.EndTime,
                    DurationSeconds         = r2.DurationSeconds,
                    DurationMinutes         = r2.DurationSeconds / 60,
                    StopReason              = r2.StopReason,
                    StatsFound              = r2.StatsFound,
                    StatsProcessed          = r2.StatsProcessed,
                    StatsSucceeded          = r2.StatsSucceeded,
                    StatsFailed             = r2.StatsFailed,
                    StatsRemaining          = r2.StatsRemaining,
                    AvgSecPerStat           = CASE
                                                WHEN r2.StatsProcessed > 0
                                                THEN CONVERT(decimal(10, 1), r2.DurationSeconds * 1.0 / r2.StatsProcessed)
                                                ELSE NULL
                                              END,
                    IsKilled                = r2.IsKilled,
                    [Version]               = r2.[Version],
                    [Databases]             = r2.[Databases],
                    TimeLimit               = r2.TimeLimit,
                    ModificationThreshold   = r2.ModificationThreshold,
                    TieredThresholds        = r2.TieredThresholds,
                    SortOrder               = r2.SortOrder,
                    QueryStorePriority      = r2.QueryStorePriority,
                    StatsInParallel         = r2.StatsInParallel,
                    Preset                  = r2.Preset,
                    LongRunningThresholdMinutes = r2.LongRunningThresholdMinutes,
                    WorkloadCoveragePct     = e2.WorkloadCoveragePct,
                    HighCpuInFirstQuartilePct = e2.HighCpuInFirstQuartilePct,
                    MinutesToHighCpuComplete = e2.MinutesToHighCpuComplete
                FROM #runs AS r2
                LEFT JOIN #qs_efficacy AS e2
                    ON e2.RunLabel = r2.RunLabel
                WHERE r2.RunLabel = r.RunLabel
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM #runs AS r;
    END;

    /*
    ============================================================================
    RESULT SET 5: TOP TABLES BY MAINTENANCE COST (@ExpertMode = 1 only)
    ============================================================================
    */
    /* #261: Pre-compute total CPU for WorkloadImpactPct (can't use subquery inside aggregate) */
    DECLARE @total_qs_cpu_ms bigint = (SELECT SUM(ISNULL(su_all.QSTotalCpuMs, 0)) FROM #stat_updates AS su_all WHERE su_all.QSTotalCpuMs IS NOT NULL);

    IF @ExpertMode = 1 AND @SingleResultSet = 0
    BEGIN
        SELECT TOP (@TopN)
            DatabaseName = su.DatabaseName,
            SchemaName = su.SchemaName,
            TableName = su.ObjectName,
            TotalUpdates = COUNT_BIG(*),
            TotalDurationSec = SUM(su.DurationMs) / 1000.0,  /* BUG-05 fix: was /1000 (integer division) */
            AvgDurationMs = AVG(su.DurationMs),
            MaxDurationMs = MAX(su.DurationMs),
            AvgModCounter = AVG(su.ModificationCounter),
            MaxSizeMB = MAX(su.SizeMB),
            MaxRowCount = MAX(su.RowCount_),
            FailCount = SUM(CASE WHEN su.ErrorNumber > 0 THEN 1 ELSE 0 END),
            IsHeap = MAX(CONVERT(integer, ISNULL(su.IsHeap, 0))),
            HasNorecompute = MAX(CONVERT(integer, ISNULL(su.HasNorecompute, 0))),
            DistinctStats = COUNT(DISTINCT su.StatisticsName),
            /* #261: Workload impact from QS data */
            WorkloadImpactPct = CONVERT(decimal(5, 1), 100.0 * SUM(ISNULL(su.QSTotalCpuMs, 0)) / NULLIF(@total_qs_cpu_ms, 0)),
            /* #255: Percentile rank (0.00-1.00) */
            ImpactScore = MAX(wi.ImpactScore)
        FROM #stat_updates AS su
        LEFT JOIN #workload_impact AS wi
            ON wi.DatabaseName = su.DatabaseName
            AND wi.SchemaName  = su.SchemaName
            AND wi.ObjectName  = su.ObjectName
        GROUP BY su.DatabaseName, su.SchemaName, su.ObjectName
        ORDER BY SUM(su.DurationMs) DESC;
    END
    ELSE IF @ExpertMode = 1
    BEGIN
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT TOP (@TopN)
            ResultSetID   = 5,
            ResultSetName = N'Top Tables',
            RowNum        = ROW_NUMBER() OVER (ORDER BY SUM(su.DurationMs) DESC),
            RowData       = (
                SELECT
                    DatabaseName     = su2.DatabaseName,
                    SchemaName       = su2.SchemaName,
                    TableName        = su2.ObjectName,
                    TotalUpdates     = COUNT_BIG(*),
                    TotalDurationSec = SUM(su2.DurationMs) / 1000.0,  /* BUG-05 fix */
                    AvgDurationMs    = AVG(su2.DurationMs),
                    MaxDurationMs    = MAX(su2.DurationMs),
                    AvgModCounter    = AVG(su2.ModificationCounter),
                    MaxSizeMB        = MAX(su2.SizeMB),
                    MaxRowCount      = MAX(su2.RowCount_),
                    FailCount        = SUM(CASE WHEN su2.ErrorNumber > 0 THEN 1 ELSE 0 END),
                    IsHeap           = MAX(CONVERT(integer, ISNULL(su2.IsHeap, 0))),
                    HasNorecompute   = MAX(CONVERT(integer, ISNULL(su2.HasNorecompute, 0))),
                    DistinctStats    = COUNT(DISTINCT su2.StatisticsName),
                    WorkloadImpactPct = CONVERT(decimal(5, 1), 100.0 * SUM(ISNULL(su2.QSTotalCpuMs, 0)) / NULLIF(@total_qs_cpu_ms, 0)),
                    ImpactScore      = MAX(wi2.ImpactScore)
                FROM #stat_updates AS su2
                LEFT JOIN #workload_impact AS wi2
                    ON wi2.DatabaseName = su2.DatabaseName
                    AND wi2.SchemaName  = su2.SchemaName
                    AND wi2.ObjectName  = su2.ObjectName
                WHERE su2.DatabaseName = su.DatabaseName
                AND   su2.SchemaName   = su.SchemaName
                AND   su2.ObjectName   = su.ObjectName
                GROUP BY su2.DatabaseName, su2.SchemaName, su2.ObjectName
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM #stat_updates AS su
        GROUP BY su.DatabaseName, su.SchemaName, su.ObjectName
        ORDER BY SUM(su.DurationMs) DESC;
    END;

    /*
    ============================================================================
    RESULT SET 6: FAILING STATISTICS (@ExpertMode = 1 only)
    ============================================================================
    */
    IF @ExpertMode = 1 AND @SingleResultSet = 0
    BEGIN
        SELECT TOP (@TopN)
            DatabaseName = su.DatabaseName,
            SchemaName = su.SchemaName,
            TableName = su.ObjectName,
            StatisticsName = su.StatisticsName,
            FailCount = COUNT_BIG(*),
            DistinctRuns = COUNT(DISTINCT su.RunLabel),
            ErrorNumbers = STUFF((
                SELECT DISTINCT N', ' + CONVERT(nvarchar(20), su2.ErrorNumber)
                FROM #stat_updates AS su2
                WHERE su2.ErrorNumber > 0
                AND   su2.DatabaseName = su.DatabaseName
                AND   su2.SchemaName = su.SchemaName
                AND   su2.ObjectName = su.ObjectName
                AND   su2.StatisticsName = su.StatisticsName
                FOR XML PATH(N''), TYPE).value(N'.', N'nvarchar(max)'), 1, 2, N''),
            LastError = MAX(LEFT(su.ErrorMessage, 500)),
            LastFailDate = MAX(su.StartTime),
            AvgSizeMB = AVG(su.SizeMB)
        FROM #stat_updates AS su
        WHERE su.ErrorNumber > 0
        GROUP BY su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName
        ORDER BY COUNT_BIG(*) DESC;
    END
    ELSE IF @ExpertMode = 1
    BEGIN
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT TOP (@TopN)
            ResultSetID   = 6,
            ResultSetName = N'Failing Statistics',
            RowNum        = ROW_NUMBER() OVER (ORDER BY COUNT_BIG(*) DESC),
            RowData       = (
                SELECT
                    DatabaseName   = su2.DatabaseName,
                    SchemaName     = su2.SchemaName,
                    TableName      = su2.ObjectName,
                    StatisticsName = su2.StatisticsName,
                    FailCount      = COUNT_BIG(*),
                    DistinctRuns   = COUNT(DISTINCT su2.RunLabel),
                    ErrorNumbers   = STUFF((
                        SELECT DISTINCT N', ' + CONVERT(nvarchar(20), su3.ErrorNumber)
                        FROM #stat_updates AS su3
                        WHERE su3.ErrorNumber > 0
                        AND   su3.DatabaseName   = su2.DatabaseName
                        AND   su3.SchemaName     = su2.SchemaName
                        AND   su3.ObjectName     = su2.ObjectName
                        AND   su3.StatisticsName = su2.StatisticsName
                        FOR XML PATH(N''), TYPE).value(N'.', N'nvarchar(max)'), 1, 2, N''),
                    LastError      = MAX(LEFT(su2.ErrorMessage, 500)),
                    LastFailDate   = MAX(su2.StartTime),
                    AvgSizeMB      = AVG(su2.SizeMB)
                FROM #stat_updates AS su2
                WHERE su2.ErrorNumber > 0
                AND   su2.DatabaseName   = su.DatabaseName
                AND   su2.SchemaName     = su.SchemaName
                AND   su2.ObjectName     = su.ObjectName
                AND   su2.StatisticsName = su.StatisticsName
                GROUP BY su2.DatabaseName, su2.SchemaName, su2.ObjectName, su2.StatisticsName
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM #stat_updates AS su
        WHERE su.ErrorNumber > 0
        GROUP BY su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName
        ORDER BY COUNT_BIG(*) DESC;
    END;

    /*
    ============================================================================
    RESULT SET 7: LONG-RUNNING STATISTICS (@ExpertMode = 1 only)
    ============================================================================
    */
    IF @ExpertMode = 1 AND @SingleResultSet = 0
    BEGIN
        SELECT TOP (@TopN)
            DatabaseName = su.DatabaseName,
            SchemaName = su.SchemaName,
            TableName = su.ObjectName,
            StatisticsName = su.StatisticsName,
            UpdateCount = COUNT_BIG(*),
            AvgDurationSec = AVG(su.DurationMs) / 1000.0,  /* BUG-05 fix: was /1000 (integer division) */
            MaxDurationSec = MAX(su.DurationMs) / 1000.0,
            MinDurationSec = MIN(su.DurationMs) / 1000.0,
            AvgSizeMB = AVG(su.SizeMB),
            MaxRowCount = MAX(su.RowCount_),
            AvgSamplePct = AVG(su.EffectiveSamplePct),
            SampleSources = STUFF((
                SELECT DISTINCT N', ' + su2.SampleSource
                FROM #stat_updates AS su2
                WHERE su2.ErrorNumber = 0
                AND   su2.DurationMs > @LongRunningMinutes * 60 * 1000
                AND   su2.DatabaseName = su.DatabaseName
                AND   su2.SchemaName = su.SchemaName
                AND   su2.ObjectName = su.ObjectName
                AND   su2.StatisticsName = su.StatisticsName
                AND   su2.SampleSource IS NOT NULL
                FOR XML PATH(N''), TYPE).value(N'.', N'nvarchar(max)'), 1, 2, N''),
            IsHeap = MAX(CONVERT(integer, ISNULL(su.IsHeap, 0))),
            /* #287: Run context for interpreting why a stat was slow */
            StatsInParallel = STUFF((
                SELECT DISTINCT N', ' + ISNULL(r2.StatsInParallel, N'N')
                FROM #stat_updates AS su3
                INNER JOIN #runs AS r2 ON r2.RunLabel = su3.RunLabel
                WHERE su3.ErrorNumber = 0
                AND   su3.DurationMs > @LongRunningMinutes * 60 * 1000
                AND   su3.DatabaseName   = su.DatabaseName
                AND   su3.SchemaName     = su.SchemaName
                AND   su3.ObjectName     = su.ObjectName
                AND   su3.StatisticsName = su.StatisticsName
                FOR XML PATH(N''), TYPE).value(N'.', N'nvarchar(max)'), 1, 2, N''),
            /* #255: Workload impact percentile */
            ImpactScore = MAX(wi.ImpactScore)
        FROM #stat_updates AS su
        LEFT JOIN #workload_impact AS wi
            ON wi.DatabaseName = su.DatabaseName
            AND wi.SchemaName  = su.SchemaName
            AND wi.ObjectName  = su.ObjectName
        WHERE su.ErrorNumber = 0
        AND   su.DurationMs > @LongRunningMinutes * 60 * 1000
        GROUP BY su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName
        ORDER BY AVG(su.DurationMs) DESC;
    END
    ELSE IF @ExpertMode = 1
    BEGIN
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT TOP (@TopN)
            ResultSetID   = 7,
            ResultSetName = N'Long-Running Statistics',
            RowNum        = ROW_NUMBER() OVER (ORDER BY AVG(su.DurationMs) DESC),
            RowData       = (
                SELECT
                    DatabaseName   = su2.DatabaseName,
                    SchemaName     = su2.SchemaName,
                    TableName      = su2.ObjectName,
                    StatisticsName = su2.StatisticsName,
                    UpdateCount    = COUNT_BIG(*),
                    AvgDurationSec = AVG(su2.DurationMs) / 1000.0,  /* BUG-05 fix */
                    MaxDurationSec = MAX(su2.DurationMs) / 1000.0,
                    MinDurationSec = MIN(su2.DurationMs) / 1000.0,
                    AvgSizeMB      = AVG(su2.SizeMB),
                    MaxRowCount    = MAX(su2.RowCount_),
                    AvgSamplePct   = AVG(su2.EffectiveSamplePct),
                    SampleSources  = STUFF((
                        SELECT DISTINCT N', ' + su3.SampleSource
                        FROM #stat_updates AS su3
                        WHERE su3.ErrorNumber = 0
                        AND   su3.DurationMs > @LongRunningMinutes * 60 * 1000
                        AND   su3.DatabaseName   = su2.DatabaseName
                        AND   su3.SchemaName     = su2.SchemaName
                        AND   su3.ObjectName     = su2.ObjectName
                        AND   su3.StatisticsName = su2.StatisticsName
                        AND   su3.SampleSource IS NOT NULL
                        FOR XML PATH(N''), TYPE).value(N'.', N'nvarchar(max)'), 1, 2, N''),
                    IsHeap         = MAX(CONVERT(integer, ISNULL(su2.IsHeap, 0))),
                    StatsInParallel = STUFF((
                        SELECT DISTINCT N', ' + ISNULL(r3.StatsInParallel, N'N')
                        FROM #stat_updates AS su4
                        INNER JOIN #runs AS r3 ON r3.RunLabel = su4.RunLabel
                        WHERE su4.ErrorNumber = 0
                        AND   su4.DurationMs > @LongRunningMinutes * 60 * 1000
                        AND   su4.DatabaseName   = su2.DatabaseName
                        AND   su4.SchemaName     = su2.SchemaName
                        AND   su4.ObjectName     = su2.ObjectName
                        AND   su4.StatisticsName = su2.StatisticsName
                        FOR XML PATH(N''), TYPE).value(N'.', N'nvarchar(max)'), 1, 2, N''),
                    ImpactScore    = MAX(wi3.ImpactScore)
                FROM #stat_updates AS su2
                LEFT JOIN #workload_impact AS wi3
                    ON wi3.DatabaseName = su2.DatabaseName
                    AND wi3.SchemaName  = su2.SchemaName
                    AND wi3.ObjectName  = su2.ObjectName
                WHERE su2.ErrorNumber = 0
                AND   su2.DurationMs > @LongRunningMinutes * 60 * 1000
                AND   su2.DatabaseName   = su.DatabaseName
                AND   su2.SchemaName     = su.SchemaName
                AND   su2.ObjectName     = su.ObjectName
                AND   su2.StatisticsName = su.StatisticsName
                GROUP BY su2.DatabaseName, su2.SchemaName, su2.ObjectName, su2.StatisticsName
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM #stat_updates AS su
        WHERE su.ErrorNumber = 0
        AND   su.DurationMs > @LongRunningMinutes * 60 * 1000
        GROUP BY su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName
        ORDER BY AVG(su.DurationMs) DESC;
    END;

    /*
    ============================================================================
    RESULT SET 8: PARAMETER CHANGE HISTORY (@ExpertMode = 1 only)
    ============================================================================
    */
    IF @ExpertMode = 1 AND @SingleResultSet = 0
    BEGIN
        SELECT
            RunLabel,
            StartTime,
            [Version],
            TimeLimit,
            ModificationThreshold,
            TieredThresholds,
            ThresholdLogic,
            SortOrder,
            QueryStorePriority,
            StatisticsSample,
            StatsInParallel,
            Preset,
            LongRunningThresholdMinutes,
            LongRunningSamplePercent,
            GroupByJoinPattern,
            FilteredStatsMode,
            BatchLimit,
            FailFast
        FROM #runs
        ORDER BY StartTime DESC;
    END
    ELSE IF @ExpertMode = 1
    BEGIN
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT
            ResultSetID   = 8,
            ResultSetName = N'Parameter Change History',
            RowNum        = ROW_NUMBER() OVER (ORDER BY r.StartTime DESC),
            RowData       = (
                SELECT
                    RunLabel                    = r2.RunLabel,
                    StartTime                   = r2.StartTime,
                    [Version]                   = r2.[Version],
                    TimeLimit                   = r2.TimeLimit,
                    ModificationThreshold       = r2.ModificationThreshold,
                    TieredThresholds            = r2.TieredThresholds,
                    ThresholdLogic              = r2.ThresholdLogic,
                    SortOrder                   = r2.SortOrder,
                    QueryStorePriority          = r2.QueryStorePriority,
                    StatisticsSample            = r2.StatisticsSample,
                    StatsInParallel             = r2.StatsInParallel,
                    Preset                      = r2.Preset,
                    LongRunningThresholdMinutes = r2.LongRunningThresholdMinutes,
                    LongRunningSamplePercent    = r2.LongRunningSamplePercent,
                    GroupByJoinPattern          = r2.GroupByJoinPattern,
                    FilteredStatsMode           = r2.FilteredStatsMode,
                    BatchLimit                  = r2.BatchLimit,
                    FailFast                    = r2.FailFast
                FROM #runs AS r2
                WHERE r2.RunLabel = r.RunLabel
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM #runs AS r;
    END;

    /*
    ============================================================================
    RESULT SET 9: OBFUSCATION MAP (conditional, @ExpertMode = 1 only)
    ============================================================================
    */
    IF @Obfuscate = 1 AND @ExpertMode = 1
    BEGIN
        IF @SingleResultSet = 0
        BEGIN
            SELECT
                ObjectType,
                OriginalName,
                ObfuscatedName
            FROM #obfuscation_map
            ORDER BY ObjectType, OriginalName;
        END
        ELSE
        BEGIN
            /* Map excluded from single result set to prevent real names leaking into exportable output.
               Use @ObfuscationMapTable to persist the map, or use multi-result-set mode (default). */
            RAISERROR(N'Obfuscation map excluded from single result set. Use @ObfuscationMapTable to persist the map.', 10, 1) WITH NOWAIT;
        END;
    END;

    /*
    ============================================================================
    RESULT SET 10: EFFICACY TREND (Weekly, broad window -- @ExpertMode = 1 only)
    ============================================================================
    */
    IF @ExpertMode = 1 AND @SingleResultSet = 0
    BEGIN
        /* #269: Per-run top-5 concentration for Top5ConcentrationPct column */
        ;WITH run_concentration AS
        (
            SELECT
                su.RunLabel,
                Top5CpuPct = CASE WHEN SUM(ISNULL(su.QSTotalCpuMs, 0)) = 0 THEN NULL
                    ELSE CONVERT(decimal(5, 1), 100.0 *
                        SUM(CASE WHEN rk.StatRank <= 5 THEN ISNULL(su.QSTotalCpuMs, 0) ELSE 0 END)
                        / NULLIF(SUM(ISNULL(su.QSTotalCpuMs, 0)), 0))
                    END
            FROM #stat_updates AS su
            CROSS APPLY (
                SELECT StatRank = ROW_NUMBER() OVER (
                    PARTITION BY su.RunLabel
                    ORDER BY ISNULL(su.QSTotalCpuMs, 0) DESC, su.ID)
            ) AS rk
            WHERE (su.ErrorNumber = 0 OR su.ErrorNumber IS NULL)
            AND   su.QSTotalCpuMs IS NOT NULL
            GROUP BY su.RunLabel
        ),
        weekly_data AS
        (
            SELECT
                week_start   = DATEADD(DAY, -(DATEPART(WEEKDAY, e.StartTime) + @@DATEFIRST - 2) % 7, CONVERT(date, e.StartTime)),
                e.RunLabel,
                e.IsQSRun,
                e.HighCpuInFirstQuartilePct,
                e.MinutesToHighCpuComplete,
                e.WorkloadCoveragePct,
                e.CompletionPct,
                e.AvgSecPerStat,
                e.SortOrder,
                rc.Top5CpuPct
            FROM #qs_efficacy AS e
            LEFT JOIN run_concentration AS rc ON rc.RunLabel = e.RunLabel
            WHERE e.StartTime >= DATEADD(DAY, -@EfficacyDaysBack, GETDATE())
        ),
        weekly_agg AS
        (
            SELECT
                week_start,
                RunCount                = COUNT_BIG(*),
                SortStrategy            = CASE
                                              WHEN MIN(CONVERT(int, w.IsQSRun)) = 1 THEN N'Query Store CPU'
                                              WHEN MAX(CONVERT(int, w.IsQSRun)) = 0 THEN N'Modification Counter'
                                              ELSE N'Mixed'
                                          END,
                HighCpuFirstQuartilePct  = AVG(w.HighCpuInFirstQuartilePct),
                AvgMinutesToHighCpu     = AVG(w.MinutesToHighCpuComplete),
                WorkloadCoveragePct     = AVG(w.WorkloadCoveragePct),
                CompletionPct           = AVG(w.CompletionPct),
                AvgSecPerStat           = AVG(w.AvgSecPerStat),
                Top5ConcentrationPct    = AVG(w.Top5CpuPct)  /* #269 */
            FROM weekly_data AS w
            GROUP BY week_start
        ),
        weekly_ranked AS
        (
            SELECT
                wa.*,
                WeekLabel = N'W' + RIGHT(N'00' + CONVERT(nvarchar(3), ROW_NUMBER() OVER (ORDER BY wa.week_start)), 2),
                prior_composite = LAG(
                    ISNULL(wa.HighCpuFirstQuartilePct, 0) + ISNULL(wa.WorkloadCoveragePct, 0) + ISNULL(wa.CompletionPct, 0)
                ) OVER (ORDER BY wa.week_start),
                current_composite = ISNULL(wa.HighCpuFirstQuartilePct, 0) + ISNULL(wa.WorkloadCoveragePct, 0) + ISNULL(wa.CompletionPct, 0),
                prior_run_count = LAG(wa.RunCount) OVER (ORDER BY wa.week_start)
            FROM weekly_agg AS wa
        )
        SELECT
            WeekStart               = wr.week_start,
            WeekLabel               = wr.WeekLabel,
            RunCount                = wr.RunCount,
            SortStrategy            = wr.SortStrategy,
            HighCpuFirstQuartilePct = wr.HighCpuFirstQuartilePct,
            AvgMinutesToHighCpu     = wr.AvgMinutesToHighCpu,
            WorkloadCoveragePct     = wr.WorkloadCoveragePct,
            CompletionPct           = wr.CompletionPct,
            AvgSecPerStat           = wr.AvgSecPerStat,
            TrendDirection          = CASE
                                          WHEN wr.prior_composite IS NULL THEN N'N/A'
                                          WHEN wr.RunCount < 2 OR ISNULL(wr.prior_run_count, 0) < 2 THEN N'INSUFFICIENT_DATA'
                                          WHEN wr.current_composite > wr.prior_composite * 1.02 THEN N'IMPROVING'
                                          WHEN wr.current_composite < wr.prior_composite * 0.98 THEN N'DEGRADING'
                                          ELSE N'STABLE'
                                      END,
            Top5ConcentrationPct    = wr.Top5ConcentrationPct  /* #269 */
        FROM weekly_ranked AS wr
        ORDER BY wr.week_start;
    END
    ELSE IF @ExpertMode = 1
    BEGIN
        /* Use temp table approach for RS10 SingleResultSet to avoid complex nested subquery FOR JSON */
        DECLARE @rs9_json nvarchar(max);

        /* #269: Same run_concentration CTE for SingleResultSet path */
        ;WITH run_concentration_s AS
        (
            SELECT
                su.RunLabel,
                Top5CpuPct = CASE WHEN SUM(ISNULL(su.QSTotalCpuMs, 0)) = 0 THEN NULL
                    ELSE CONVERT(decimal(5, 1), 100.0 *
                        SUM(CASE WHEN rk.StatRank <= 5 THEN ISNULL(su.QSTotalCpuMs, 0) ELSE 0 END)
                        / NULLIF(SUM(ISNULL(su.QSTotalCpuMs, 0)), 0))
                    END
            FROM #stat_updates AS su
            CROSS APPLY (
                SELECT StatRank = ROW_NUMBER() OVER (
                    PARTITION BY su.RunLabel
                    ORDER BY ISNULL(su.QSTotalCpuMs, 0) DESC, su.ID)
            ) AS rk
            WHERE (su.ErrorNumber = 0 OR su.ErrorNumber IS NULL)
            AND   su.QSTotalCpuMs IS NOT NULL
            GROUP BY su.RunLabel
        ),
        weekly_data_s AS
        (
            SELECT
                week_start   = DATEADD(DAY, -(DATEPART(WEEKDAY, e.StartTime) + @@DATEFIRST - 2) % 7, CONVERT(date, e.StartTime)),
                e.IsQSRun,
                e.HighCpuInFirstQuartilePct,
                e.MinutesToHighCpuComplete,
                e.WorkloadCoveragePct,
                e.CompletionPct,
                e.AvgSecPerStat,
                rc.Top5CpuPct
            FROM #qs_efficacy AS e
            LEFT JOIN run_concentration_s AS rc ON rc.RunLabel = e.RunLabel
            WHERE e.StartTime >= DATEADD(DAY, -@EfficacyDaysBack, GETDATE())
        ),
        weekly_agg_s AS
        (
            SELECT
                week_start,
                RunCount                = COUNT_BIG(*),
                SortStrategy            = CASE
                                              WHEN MIN(CONVERT(int, w.IsQSRun)) = 1 THEN N'Query Store CPU'
                                              WHEN MAX(CONVERT(int, w.IsQSRun)) = 0 THEN N'Modification Counter'
                                              ELSE N'Mixed'
                                          END,
                HighCpuFirstQuartilePct  = AVG(w.HighCpuInFirstQuartilePct),
                AvgMinutesToHighCpu     = AVG(w.MinutesToHighCpuComplete),
                WorkloadCoveragePct     = AVG(w.WorkloadCoveragePct),
                CompletionPct           = AVG(w.CompletionPct),
                AvgSecPerStat           = AVG(w.AvgSecPerStat),
                Top5ConcentrationPct    = AVG(w.Top5CpuPct)  /* #269 */
            FROM weekly_data_s AS w
            GROUP BY week_start
        ),
        weekly_ranked_s AS
        (
            SELECT
                wa.*,
                WeekLabel = N'W' + RIGHT(N'00' + CONVERT(nvarchar(3), ROW_NUMBER() OVER (ORDER BY wa.week_start)), 2),
                prior_composite = LAG(
                    ISNULL(wa.HighCpuFirstQuartilePct, 0) + ISNULL(wa.WorkloadCoveragePct, 0) + ISNULL(wa.CompletionPct, 0)
                ) OVER (ORDER BY wa.week_start),
                current_composite = ISNULL(wa.HighCpuFirstQuartilePct, 0) + ISNULL(wa.WorkloadCoveragePct, 0) + ISNULL(wa.CompletionPct, 0),
                prior_run_count = LAG(wa.RunCount) OVER (ORDER BY wa.week_start)
            FROM weekly_agg_s AS wa
        )
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT
            ResultSetID   = 10,
            ResultSetName = N'Efficacy Trend',
            RowNum        = ROW_NUMBER() OVER (ORDER BY wr.week_start),
            RowData       = (
                SELECT
                    WeekStart               = wr.week_start,
                    WeekLabel               = wr.WeekLabel,
                    RunCount                = wr.RunCount,
                    SortStrategy            = wr.SortStrategy,
                    HighCpuFirstQuartilePct = wr.HighCpuFirstQuartilePct,
                    AvgMinutesToHighCpu     = wr.AvgMinutesToHighCpu,
                    WorkloadCoveragePct     = wr.WorkloadCoveragePct,
                    CompletionPct           = wr.CompletionPct,
                    AvgSecPerStat           = wr.AvgSecPerStat,
                    TrendDirection          = CASE
                                                  WHEN wr.prior_composite IS NULL THEN N'N/A'
                                                  WHEN wr.RunCount < 2 OR ISNULL(wr.prior_run_count, 0) < 2 THEN N'INSUFFICIENT_DATA'
                                                  WHEN wr.current_composite > wr.prior_composite * 1.02 THEN N'IMPROVING'
                                                  WHEN wr.current_composite < wr.prior_composite * 0.98 THEN N'DEGRADING'
                                                  ELSE N'STABLE'
                                              END,
                    Top5ConcentrationPct    = wr.Top5ConcentrationPct  /* #269 */
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM weekly_ranked_s AS wr;
    END;

    /*
    ============================================================================
    RESULT SET 11: EFFICACY DETAIL (Per-run, close-up window -- @ExpertMode = 1 only)
    ============================================================================
    */
    IF @ExpertMode = 1 AND @SingleResultSet = 0
    BEGIN
        ;WITH detail_runs AS
        (
            SELECT
                e.*,
                prior_completion = LAG(e.CompletionPct) OVER (ORDER BY e.StartTime),
                prior_minutes    = LAG(e.MinutesToHighCpuComplete) OVER (ORDER BY e.StartTime)
            FROM #qs_efficacy AS e
            WHERE e.StartTime >= DATEADD(DAY, -@EfficacyDetailDays, GETDATE())
        )
        SELECT
            RunLabel                = dr.RunLabel,
            StartTime               = dr.StartTime,
            SortStrategy            = CASE
                                          WHEN dr.IsQSRun = 1 THEN N'Query Store CPU'
                                          ELSE ISNULL(dr.SortOrder, N'Modification Counter')
                                      END,
            StatsFound              = dr.StatsFound,
            StatsProcessed          = dr.StatsProcessed,
            CompletionPct           = dr.CompletionPct,
            HighCpuInFirstQuartilePct = dr.HighCpuInFirstQuartilePct,
            MinutesToHighCpuComplete = dr.MinutesToHighCpuComplete,
            WorkloadCoveragePct     = dr.WorkloadCoveragePct,
            AvgSecPerStat           = dr.AvgSecPerStat,
            StopReason              = dr.StopReason,
            DeltaVsPrior            = CASE
                                          WHEN dr.prior_completion IS NULL THEN N'(first run in window)'
                                          ELSE
                                              CASE
                                                  WHEN dr.CompletionPct > dr.prior_completion
                                                  THEN N'+' + CONVERT(nvarchar(10), CONVERT(integer, dr.CompletionPct - dr.prior_completion)) + N'% completion'
                                                  WHEN dr.CompletionPct < dr.prior_completion
                                                  THEN CONVERT(nvarchar(10), CONVERT(integer, dr.CompletionPct - dr.prior_completion)) + N'% completion'
                                                  ELSE N'0% completion'
                                              END
                                              + CASE
                                                  WHEN dr.prior_minutes IS NOT NULL AND dr.MinutesToHighCpuComplete IS NOT NULL
                                                  THEN N', ' + CASE
                                                      WHEN dr.MinutesToHighCpuComplete < dr.prior_minutes
                                                      THEN N'-' + CONVERT(nvarchar(20), CONVERT(integer, dr.prior_minutes - dr.MinutesToHighCpuComplete)) + N'min to critical'
                                                      WHEN dr.MinutesToHighCpuComplete > dr.prior_minutes
                                                      THEN N'+' + CONVERT(nvarchar(20), CONVERT(integer, dr.MinutesToHighCpuComplete - dr.prior_minutes)) + N'min to critical'
                                                      ELSE N'0min to critical'
                                                  END
                                                  ELSE N''
                                              END
                                      END
        FROM detail_runs AS dr
        ORDER BY dr.StartTime DESC;
    END
    ELSE IF @ExpertMode = 1
    BEGIN
        /* GAP-G fix: Include DeltaVsPrior in SingleResultSet mode (matching direct output).
           Uses correlated subquery with LAG window function via derived table. */
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT
            ResultSetID   = 11,
            ResultSetName = N'Efficacy Detail',
            RowNum        = ROW_NUMBER() OVER (ORDER BY dr.StartTime DESC),
            RowData       = N'{'
                + N'"RunLabel":' + CASE WHEN dr.RunLabel IS NULL THEN N'null' ELSE N'"' + dr.RunLabel + N'"' END
                + N',"StartTime":"' + CONVERT(nvarchar(30), dr.StartTime, 126) + N'"'
                + N',"SortStrategy":"' + CASE WHEN dr.IsQSRun = 1 THEN N'Query Store CPU' ELSE ISNULL(dr.SortOrder, N'Modification Counter') END + N'"'
                + N',"StatsFound":' + ISNULL(CONVERT(nvarchar(10), dr.StatsFound), N'null')
                + N',"StatsProcessed":' + ISNULL(CONVERT(nvarchar(10), dr.StatsProcessed), N'null')
                + N',"CompletionPct":' + ISNULL(CONVERT(nvarchar(10), dr.CompletionPct), N'null')
                + N',"HighCpuInFirstQuartilePct":' + ISNULL(CONVERT(nvarchar(10), dr.HighCpuInFirstQuartilePct), N'null')
                + N',"MinutesToHighCpuComplete":' + ISNULL(CONVERT(nvarchar(20), dr.MinutesToHighCpuComplete), N'null')
                + N',"WorkloadCoveragePct":' + ISNULL(CONVERT(nvarchar(10), dr.WorkloadCoveragePct), N'null')
                + N',"AvgSecPerStat":' + ISNULL(CONVERT(nvarchar(20), dr.AvgSecPerStat), N'null')
                + N',"StopReason":' + CASE WHEN dr.StopReason IS NULL THEN N'null' ELSE N'"' + dr.StopReason + N'"' END
                + N',"DeltaVsPrior":"' + CASE
                    WHEN dr.prior_completion IS NULL THEN N'(first run in window)'
                    ELSE CASE
                        WHEN dr.CompletionPct > dr.prior_completion
                        THEN N'+' + CONVERT(nvarchar(10), CONVERT(integer, dr.CompletionPct - dr.prior_completion)) + N'% completion'
                        WHEN dr.CompletionPct < dr.prior_completion
                        THEN CONVERT(nvarchar(10), CONVERT(integer, dr.CompletionPct - dr.prior_completion)) + N'% completion'
                        ELSE N'0% completion'
                    END
                    + CASE
                        WHEN dr.prior_minutes IS NOT NULL AND dr.MinutesToHighCpuComplete IS NOT NULL
                        THEN N', ' + CASE
                            WHEN dr.MinutesToHighCpuComplete < dr.prior_minutes
                            THEN N'-' + CONVERT(nvarchar(20), CONVERT(integer, dr.prior_minutes - dr.MinutesToHighCpuComplete)) + N'min to critical'
                            WHEN dr.MinutesToHighCpuComplete > dr.prior_minutes
                            THEN N'+' + CONVERT(nvarchar(20), CONVERT(integer, dr.MinutesToHighCpuComplete - dr.prior_minutes)) + N'min to critical'
                            ELSE N'0min to critical'
                        END
                        ELSE N''
                    END
                END + N'"'
                + N'}'
        FROM (
            SELECT
                e.*,
                prior_completion = LAG(e.CompletionPct) OVER (ORDER BY e.StartTime),
                prior_minutes    = LAG(e.MinutesToHighCpuComplete) OVER (ORDER BY e.StartTime)
            FROM #qs_efficacy AS e
            WHERE e.StartTime >= DATEADD(DAY, -@EfficacyDetailDays, GETDATE())
        ) AS dr;
    END;

    /*
    ============================================================================
    RESULT SET 12: HIGH-CPU STAT POSITIONS (Most Recent Run -- @ExpertMode = 1 only)
    ============================================================================
    */
    IF @ExpertMode = 1 AND @SingleResultSet = 0
    BEGIN
        DECLARE @latest_qs_run nvarchar(100) = (
            SELECT TOP (1) e.RunLabel
            FROM #qs_efficacy AS e
            WHERE e.IsQSRun = 1
            ORDER BY e.StartTime DESC
        );

        ;WITH rs12_ranked AS
        (
            SELECT
                WorkloadRank        = ROW_NUMBER() OVER (ORDER BY ISNULL(su.QSTotalCpuMs, 0) DESC, su.ID),
                DatabaseName        = su.DatabaseName,
                SchemaName          = su.SchemaName,
                TableName           = su.ObjectName,
                StatisticsName      = su.StatisticsName,
                ProcessingPosition  = ISNULL(su.ProcessingPosition, ROW_NUMBER() OVER (ORDER BY su.ID)),
                TotalQueryCpuMs     = su.QSTotalCpuMs,
                TotalExecutions     = su.QSTotalExecutions,
                PlanCount           = su.QSPlanCount,
                UpdateDurationMs    = su.DurationMs,
                QualifyReason       = su.QualifyReason,
                total_stats         = COUNT_BIG(*) OVER (),
                total_cpu           = SUM(ISNULL(su.QSTotalCpuMs, 0)) OVER ()
            FROM #stat_updates AS su
            WHERE su.RunLabel = @latest_qs_run
            AND   (su.ErrorNumber = 0 OR su.ErrorNumber IS NULL)
        )
        SELECT TOP (@TopN)
            WorkloadRank,
            DatabaseName,
            SchemaName,
            TableName,
            StatisticsName,
            ProcessingPosition,
            TotalQueryCpuMs,
            TotalExecutions,
            PlanCount,
            UpdateDurationMs,
            QualifyReason,
            /* #262: Percentile rank and cumulative CPU */
            WorkloadRankPct   = CONVERT(decimal(5, 1), 100.0 * WorkloadRank / NULLIF(total_stats, 0)),
            CumulativeCpuPct  = CONVERT(decimal(5, 1), 100.0 * SUM(ISNULL(TotalQueryCpuMs, 0)) OVER (ORDER BY WorkloadRank) / NULLIF(total_cpu, 0))
        FROM rs12_ranked
        ORDER BY WorkloadRank;
    END
    ELSE IF @ExpertMode = 1
    BEGIN
        /* #262: Use temp table approach for RS12 SingleResultSet to compute cumulative correctly */
        ;WITH rs12_ranked_s AS
        (
            SELECT
                su.ID,
                su.RunLabel,
                WorkloadRank        = ROW_NUMBER() OVER (ORDER BY ISNULL(su.QSTotalCpuMs, 0) DESC, su.ID),
                DatabaseName        = su.DatabaseName,
                SchemaName          = su.SchemaName,
                TableName           = su.ObjectName,
                StatisticsName      = su.StatisticsName,
                ProcessingPosition  = ISNULL(su.ProcessingPosition, ROW_NUMBER() OVER (ORDER BY su.ID)),
                TotalQueryCpuMs     = su.QSTotalCpuMs,
                TotalExecutions     = su.QSTotalExecutions,
                PlanCount           = su.QSPlanCount,
                UpdateDurationMs    = su.DurationMs,
                QualifyReason       = su.QualifyReason,
                total_stats         = COUNT_BIG(*) OVER (),
                total_cpu           = SUM(ISNULL(su.QSTotalCpuMs, 0)) OVER ()
            FROM #stat_updates AS su
            WHERE su.RunLabel = @latest_qs_run
            AND   (su.ErrorNumber = 0 OR su.ErrorNumber IS NULL)
        )
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT TOP (@TopN)
            ResultSetID   = 12,
            ResultSetName = N'High-CPU Stat Positions',
            RowNum        = r.WorkloadRank,
            RowData       = (
                SELECT
                    WorkloadRank        = r2.WorkloadRank,
                    DatabaseName        = r2.DatabaseName,
                    SchemaName          = r2.SchemaName,
                    TableName           = r2.TableName,
                    StatisticsName      = r2.StatisticsName,
                    ProcessingPosition  = r2.ProcessingPosition,
                    TotalQueryCpuMs     = r2.TotalQueryCpuMs,
                    TotalExecutions     = r2.TotalExecutions,
                    PlanCount           = r2.PlanCount,
                    UpdateDurationMs    = r2.UpdateDurationMs,
                    QualifyReason       = r2.QualifyReason,
                    WorkloadRankPct     = CONVERT(decimal(5, 1), 100.0 * r2.WorkloadRank / NULLIF(r2.total_stats, 0)),
                    CumulativeCpuPct    = CONVERT(decimal(5, 1), 100.0 * SUM(ISNULL(r2.TotalQueryCpuMs, 0)) OVER (ORDER BY r2.WorkloadRank) / NULLIF(r2.total_cpu, 0))
                FROM rs12_ranked_s AS r2
                WHERE r2.ID = r.ID
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM rs12_ranked_s AS r
        ORDER BY r.WorkloadRank;
    END;

    /*
    ============================================================================
    RESULT SET 13: QS PERFORMANCE CORRELATION (Per-stat CPU trend -- @ExpertMode = 1 only)
    Shows how Query Store CPU metrics change for each statistic across runs.
    Leadership takeaway: "Are the queries actually getting faster after we update these statistics?"
    ============================================================================
    */
    /* BUG-B fix: RS13 now includes per-execution CPU normalization columns alongside raw totals */
    IF @ExpertMode = 1 AND @SingleResultSet = 0
    BEGIN
        ;WITH stat_trend AS
        (
            SELECT
                su.DatabaseName,
                su.SchemaName,
                su.ObjectName,
                su.StatisticsName,
                su.QSTotalCpuMs,
                su.QSTotalExecutions,
                su.QSPlanCount,
                cpu_per_exec = CONVERT(decimal(18, 4), su.QSTotalCpuMs * 1.0 / NULLIF(su.QSTotalExecutions, 0)),
                su.RunLabel,
                r.StartTime,
                appearance_num = ROW_NUMBER() OVER (
                    PARTITION BY su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName
                    ORDER BY r.StartTime
                ),
                total_appearances = COUNT_BIG(*) OVER (
                    PARTITION BY su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName
                )
            FROM #stat_updates AS su
            INNER JOIN #runs AS r
                ON r.RunLabel = su.RunLabel
            WHERE su.QSTotalCpuMs IS NOT NULL
            AND   su.QSTotalCpuMs > 0
            AND   (su.ErrorNumber = 0 OR su.ErrorNumber IS NULL)
        ),
        first_last AS
        (
            SELECT
                DatabaseName, SchemaName, ObjectName, StatisticsName,
                Appearances      = MAX(total_appearances),
                FirstRunDate     = MAX(CASE WHEN appearance_num = 1 THEN StartTime END),
                LastRunDate      = MAX(CASE WHEN appearance_num = total_appearances THEN StartTime END),
                FirstCpuMs       = MAX(CASE WHEN appearance_num = 1 THEN QSTotalCpuMs END),
                LastCpuMs        = MAX(CASE WHEN appearance_num = total_appearances THEN QSTotalCpuMs END),
                FirstCpuPerExec  = MAX(CASE WHEN appearance_num = 1 THEN cpu_per_exec END),
                LastCpuPerExec   = MAX(CASE WHEN appearance_num = total_appearances THEN cpu_per_exec END),
                FirstExecs       = MAX(CASE WHEN appearance_num = 1 THEN QSTotalExecutions END),
                LastExecs        = MAX(CASE WHEN appearance_num = total_appearances THEN QSTotalExecutions END),
                FirstPlans       = MAX(CASE WHEN appearance_num = 1 THEN QSPlanCount END),
                LastPlans        = MAX(CASE WHEN appearance_num = total_appearances THEN QSPlanCount END)
            FROM stat_trend
            WHERE total_appearances >= 2
            GROUP BY DatabaseName, SchemaName, ObjectName, StatisticsName
        )
        SELECT TOP (@TopN)
            fl.DatabaseName,
            fl.SchemaName,
            TableName               = fl.ObjectName,
            fl.StatisticsName,
            fl.Appearances,
            fl.FirstRunDate,
            fl.LastRunDate,
            fl.FirstCpuMs,
            fl.LastCpuMs,
            CpuChangePct            = CASE WHEN fl.FirstCpuMs > 0
                                           THEN CONVERT(decimal(10, 1), (fl.LastCpuMs - fl.FirstCpuMs) * 100.0 / fl.FirstCpuMs)
                                           ELSE NULL
                                      END,
            FirstCpuPerExec         = CONVERT(decimal(10, 2), fl.FirstCpuPerExec),
            LastCpuPerExec          = CONVERT(decimal(10, 2), fl.LastCpuPerExec),
            CpuPerExecChangePct     = CASE WHEN fl.FirstCpuPerExec > 0
                                           THEN CONVERT(decimal(10, 1), (fl.LastCpuPerExec - fl.FirstCpuPerExec) * 100.0 / fl.FirstCpuPerExec)
                                           ELSE NULL
                                      END,
            CpuTrend                = CASE
                                          WHEN fl.FirstCpuPerExec > 0 AND fl.LastCpuPerExec < fl.FirstCpuPerExec * 0.95 THEN N'IMPROVING'
                                          WHEN fl.FirstCpuPerExec > 0 AND fl.LastCpuPerExec > fl.FirstCpuPerExec * 1.05 THEN N'DEGRADING'
                                          ELSE N'STABLE'
                                      END,
            fl.FirstExecs,
            fl.LastExecs,
            fl.FirstPlans,
            fl.LastPlans,
            ForcedPlanCount         = ISNULL(fp.ForcedPlanCount, 0),
            PlanTrend               = CASE
                                          WHEN fl.LastPlans > fl.FirstPlans AND ISNULL(fp.ForcedPlanCount, 0) > 0
                                              THEN N'MORE PLANS (forced plan at risk)'
                                          WHEN fl.LastPlans > fl.FirstPlans THEN N'MORE PLANS (possible regression)'
                                          WHEN fl.LastPlans < fl.FirstPlans THEN N'FEWER PLANS (consolidating)'
                                          ELSE N'STABLE'
                                      END
        FROM first_last AS fl
        LEFT JOIN #forced_plans AS fp
            ON fp.DatabaseName = fl.DatabaseName
            AND fp.ObjectName = fl.ObjectName
        ORDER BY
            CASE
                WHEN fl.FirstCpuPerExec > 0 AND fl.LastCpuPerExec < fl.FirstCpuPerExec * 0.95 THEN 1
                WHEN fl.FirstCpuPerExec > 0 AND fl.LastCpuPerExec > fl.FirstCpuPerExec * 1.05 THEN 2
                ELSE 3
            END,
            ISNULL(fl.FirstCpuMs, 0) DESC;
    END
    ELSE IF @ExpertMode = 1
    BEGIN
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT
            ResultSetID   = 13,
            ResultSetName = N'QS Performance Correlation',
            RowNum        = ROW_NUMBER() OVER (
                                ORDER BY
                                    CASE
                                        WHEN fl.FirstCpuPerExec > 0 AND fl.LastCpuPerExec < fl.FirstCpuPerExec * 0.95 THEN 1
                                        WHEN fl.FirstCpuPerExec > 0 AND fl.LastCpuPerExec > fl.FirstCpuPerExec * 1.05 THEN 2
                                        ELSE 3
                                    END,
                                    ISNULL(fl.FirstCpuMs, 0) DESC
                            ),
            RowData       = (
                SELECT
                    DatabaseName        = fl2.DatabaseName,
                    SchemaName          = fl2.SchemaName,
                    TableName           = fl2.ObjectName,
                    StatisticsName      = fl2.StatisticsName,
                    Appearances         = fl2.Appearances,
                    FirstCpuMs          = fl2.FirstCpuMs,
                    LastCpuMs           = fl2.LastCpuMs,
                    CpuChangePct        = CASE WHEN fl2.FirstCpuMs > 0
                                               THEN CONVERT(decimal(10, 1), (fl2.LastCpuMs - fl2.FirstCpuMs) * 100.0 / fl2.FirstCpuMs)
                                               ELSE NULL
                                          END,
                    FirstCpuPerExec     = CONVERT(decimal(10, 2), fl2.FirstCpuPerExec),
                    LastCpuPerExec      = CONVERT(decimal(10, 2), fl2.LastCpuPerExec),
                    CpuPerExecChangePct = CASE WHEN fl2.FirstCpuPerExec > 0
                                               THEN CONVERT(decimal(10, 1), (fl2.LastCpuPerExec - fl2.FirstCpuPerExec) * 100.0 / fl2.FirstCpuPerExec)
                                               ELSE NULL
                                          END,
                    CpuTrend            = CASE
                                             WHEN fl2.FirstCpuPerExec > 0 AND fl2.LastCpuPerExec < fl2.FirstCpuPerExec * 0.95 THEN N'IMPROVING'
                                             WHEN fl2.FirstCpuPerExec > 0 AND fl2.LastCpuPerExec > fl2.FirstCpuPerExec * 1.05 THEN N'DEGRADING'
                                             ELSE N'STABLE'
                                         END,
                    ForcedPlanCount     = fl2.ForcedPlanCount,
                    PlanTrend           = CASE
                                             WHEN fl2.LastPlans > fl2.FirstPlans AND fl2.ForcedPlanCount > 0
                                                 THEN N'MORE PLANS (forced plan at risk)'
                                             WHEN fl2.LastPlans > fl2.FirstPlans THEN N'MORE PLANS (possible regression)'
                                             WHEN fl2.LastPlans < fl2.FirstPlans THEN N'FEWER PLANS (consolidating)'
                                             ELSE N'STABLE'
                                         END
                FROM (SELECT fl.DatabaseName, fl.SchemaName, fl.ObjectName, fl.StatisticsName,
                             fl.Appearances, fl.FirstCpuMs, fl.LastCpuMs, fl.FirstCpuPerExec, fl.LastCpuPerExec,
                             fl.FirstPlans, fl.LastPlans, fl.ForcedPlanCount) AS fl2
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM
        (
            SELECT TOP (@TopN)
                su_fl.DatabaseName, su_fl.SchemaName, su_fl.ObjectName, su_fl.StatisticsName,
                Appearances     = COUNT_BIG(*),
                FirstCpuMs      = MAX(CASE WHEN su_fl.rn = 1 THEN su_fl.QSTotalCpuMs END),
                LastCpuMs       = MAX(CASE WHEN su_fl.rn = su_fl.cnt THEN su_fl.QSTotalCpuMs END),
                FirstCpuPerExec = MAX(CASE WHEN su_fl.rn = 1 THEN su_fl.cpu_per_exec END),
                LastCpuPerExec  = MAX(CASE WHEN su_fl.rn = su_fl.cnt THEN su_fl.cpu_per_exec END),
                FirstPlans      = MAX(CASE WHEN su_fl.rn = 1 THEN su_fl.QSPlanCount END),
                LastPlans       = MAX(CASE WHEN su_fl.rn = su_fl.cnt THEN su_fl.QSPlanCount END),
                ForcedPlanCount = MAX(ISNULL(fp.ForcedPlanCount, 0))
            FROM
            (
                SELECT
                    su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName,
                    su.QSTotalCpuMs,
                    su.QSPlanCount,
                    cpu_per_exec = CONVERT(decimal(18, 4), su.QSTotalCpuMs * 1.0 / NULLIF(su.QSTotalExecutions, 0)),
                    rn  = ROW_NUMBER() OVER (
                              PARTITION BY su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName
                              ORDER BY r.StartTime
                          ),
                    cnt = COUNT_BIG(*) OVER (
                              PARTITION BY su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName
                          )
                FROM #stat_updates AS su
                INNER JOIN #runs AS r ON r.RunLabel = su.RunLabel
                WHERE su.QSTotalCpuMs IS NOT NULL AND su.QSTotalCpuMs > 0
                AND   (su.ErrorNumber = 0 OR su.ErrorNumber IS NULL)
            ) AS su_fl
            LEFT JOIN #forced_plans AS fp
                ON fp.DatabaseName = su_fl.DatabaseName
                AND fp.ObjectName = su_fl.ObjectName
            WHERE su_fl.cnt >= 2
            GROUP BY su_fl.DatabaseName, su_fl.SchemaName, su_fl.ObjectName, su_fl.StatisticsName
            ORDER BY MAX(CASE WHEN su_fl.rn = 1 THEN su_fl.QSTotalCpuMs END) DESC
        ) AS fl;
    END;

    /*
    ============================================================================
    FINAL OUTPUT (single result set mode only)
    ============================================================================
    */
    IF @SingleResultSet = 1
    BEGIN
        SELECT
            ResultSetID,
            ResultSetName,
            RowNum,
            RowData
        FROM #single_rs
        ORDER BY ResultSetID, RowNum;
    END;

    DROP TABLE IF EXISTS #single_rs;

    IF @Debug = 1
    BEGIN
        SET @debug_elapsed_ms = DATEDIFF(MILLISECOND, @debug_timer, SYSDATETIME());
        DECLARE @debug_rec_count integer;
        SELECT @debug_rec_count = COUNT_BIG(*) FROM #recommendations;
        RAISERROR(N'', 10, 1) WITH NOWAIT;
        RAISERROR(N'DEBUG: === Execution Summary ===', 10, 1) WITH NOWAIT;
        RAISERROR(N'DEBUG: Total elapsed: %i ms', 10, 1, @debug_elapsed_ms) WITH NOWAIT;
        RAISERROR(N'DEBUG: Runs: %i  Stat updates: %i  Recommendations: %i',
            10, 1, @run_count, @stat_update_count, @debug_rec_count) WITH NOWAIT;
        RAISERROR(N'DEBUG: QS efficacy rows: %i  Workload impact objects: %i',
            10, 1, @qs_efficacy_count, @workload_impact_count) WITH NOWAIT;
    END;

    /*
    ============================================================================
    CLEANUP
    ============================================================================
    */
    DROP TABLE IF EXISTS #runs;
    DROP TABLE IF EXISTS #stat_updates;
    DROP TABLE IF EXISTS #recommendations;
    DROP TABLE IF EXISTS #obfuscation_map;
    DROP TABLE IF EXISTS #executive_dashboard;
    DROP TABLE IF EXISTS #qs_efficacy;

    RETURN;
END;
GO
