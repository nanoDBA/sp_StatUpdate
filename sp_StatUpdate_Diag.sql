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

License:    MIT License (same as sp_StatUpdate)

Version:    2026.03.04 (CalVer: YYYY.MM.DD; same-day patches append .1, .2, etc.)

Requires:   - dbo.CommandLog table (Ola Hallengren's SQL Server Maintenance Solution)
            - sp_StatUpdate entries in CommandLog (SP_STATUPDATE_START/END + UPDATE_STATISTICS)
            - SQL Server 2016+ (STRING_SPLIT, STRING_AGG)

Usage:      -- Quick health check (SSMS, real names):
            EXECUTE dbo.sp_StatUpdate_Diag;

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
    @ObfuscationSeed nvarchar(128) = NULL,       /* salt for HASHBYTES — makes tokens unpredictable without seed */
    @ObfuscationMapTable sysname = NULL,          /* persist obfuscation map to this table (auto-creates if missing) */
    @LongRunningMinutes integer = 10,            /* threshold for "long-running stat" detection */
    @FailureThreshold integer = 3,               /* same stat failing N+ times = CRITICAL */
    @TimeLimitExhaustionPct integer = 80,         /* warn if >X% of runs hit time limit */
    @ThroughputWindowDays integer = 7,            /* window size for throughput trend comparison */
    @TopN integer = 20,                           /* top N items in detail result sets */
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
        @procedure_version varchar(20) = '2026.03.04',
        @procedure_version_date datetime = '20260212';

    SET @Version = @procedure_version;
    SET @VersionDate = @procedure_version_date;

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
                (N'@DaysBack',                 N'integer',  N'History window in days — how far back to scan CommandLog',
                    N'1-3650', N'30'),
                (N'@CommandLogDatabase',        N'sysname',  N'Database containing dbo.CommandLog table. NULL = current database context.',
                    N'NULL, database name (e.g., DBATools, master)', N'NULL (current database)'),
                (N'@Obfuscate',                N'bit',      N'Hash database/schema/table/stat names for safe external sharing. Uses HASHBYTES MD5. Obfuscation map returned as result set 8 (multi-result-set mode only).',
                    N'0, 1', N'0'),
                (N'@ObfuscationSeed',          N'nvarchar(128)', N'Salt prepended to names before hashing. Makes tokens unpredictable without the seed but deterministic across runs/servers with the same seed. NULL = unsalted (backward compatible).',
                    N'NULL, any string up to 128 chars', N'NULL'),
                (N'@ObfuscationMapTable',      N'sysname',  N'Persist obfuscation map to this table (auto-creates if missing, appends if exists). Enables saving the map on prod while exporting only obfuscated results. Requires @Obfuscate=1.',
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
                (N'@Help',                     N'bit',      N'Show this help output and return immediately',
                    N'0, 1', N'0'),
                (N'@Debug',                    N'bit',      N'Verbose diagnostic output — shows intermediate temp table counts and timing',
                    N'0, 1', N'0'),
                (N'@SingleResultSet',          N'bit',      N'Collapse all result sets into one with columns (ResultSetID, ResultSetName, RowNum, RowData). RowData is JSON. Enables INSERT...EXEC capture in automation.',
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
                (N'I1', N'INFO',     N'RUN_HEALTH',            N'Completion rate, duration trend, StopReason distribution'),
                (N'I2', N'INFO',     N'PARAMETER_HISTORY',     N'How parameters changed across runs'),
                (N'I3', N'INFO',     N'TOP_TABLES',            N'Tables consuming the most maintenance time'),
                (N'I4', N'INFO',     N'UNUSED_FEATURES',       N'Available features not being used'),
                (N'I5', N'INFO',     N'VERSION_HISTORY',       N'sp_StatUpdate versions used across analysis window')
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
                (1, N'Recommendations',            N'Severity-categorized findings with parameter suggestions'),
                (2, N'Run Health Summary',          N'Aggregate metrics across all runs'),
                (3, N'Run Detail',                  N'Per-run metrics'),
                (4, N'Top Tables',                  N'Top N tables by total update duration'),
                (5, N'Failing Statistics',           N'Stats with errors, grouped'),
                (6, N'Long-Running Statistics',      N'Stats exceeding threshold'),
                (7, N'Parameter Change History',     N'Parameter values across runs'),
                (8, N'Obfuscation Map (conditional)', N'Only when @Obfuscate=1'),
                (0, N'Unified Result Set',            N'When @SingleResultSet=1: one result set with ResultSetID, ResultSetName, RowNum, RowData (JSON). Replaces result sets 1-8.')
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
                 N'@Obfuscate=1 replaces names with MD5 hashes (e.g., DB_a1b2c3, TBL_d4e5f6). Prefixes preserved for readability. Map is result set 8 in multi-result-set mode (excluded from @SingleResultSet=1 to prevent leaking real names). Use @ObfuscationMapTable to persist the map on prod. Use @ObfuscationSeed to salt hashes — makes tokens stable across servers with the same seed but unpredictable without it.'),
                (N'Killed Run Detection',
                 N'Two detection methods: (1) SP_STATUPDATE_START without matching SP_STATUPDATE_END = orphaned run, (2) SP_STATUPDATE_END with StopReason=KILLED = cleaned up by @CleanupOrphanedRuns. Both trigger C1 CRITICAL.'),
                (N'Throughput Trend (C4)',
                 N'Compares average seconds-per-stat in the recent @ThroughputWindowDays against the prior window of equal length. If recent average is >50% worse, triggers C4 CRITICAL. Data-dependent — requires sufficient runs in both windows.'),
                (N'Overlapping Runs (W4)',
                 N'Detects concurrent sp_StatUpdate executions by checking for overlapping StartTime/EndTime ranges. Excludes killed runs to prevent false positives from orphan-cleanup END records.'),
                (N'SingleResultSet Mode',
                 N'@SingleResultSet=1 wraps all 8 result sets into one table with columns: ResultSetID (int), ResultSetName (nvarchar), RowNum (int), RowData (nvarchar(max) as JSON). Use OPENJSON(RowData) to parse. Enables INSERT...EXEC patterns that fail with multiple result sets.'),
                (N'CommandLog Requirements',
                 N'Requires Ola Hallengren''s dbo.CommandLog table with sp_StatUpdate entries (CommandType IN SP_STATUPDATE_START, SP_STATUPDATE_END, UPDATE_STATISTICS). No data = no diagnostics (graceful empty result sets). @CommandLogDatabase lets you point to a central logging database.'),
                (N'PowerShell Wrapper',
                 N'Invoke-StatUpdateDiag.ps1 runs sp_StatUpdate_Diag across multiple servers in parallel, detects version skew and parameter inconsistencies, and generates Markdown/HTML/JSON reports. Use -Obfuscate for safe sharing.')
        ) AS notes (topic, detail);

        RETURN;
    END;

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

    RAISERROR(N'sp_StatUpdate_Diag v%s', 10, 1, @procedure_version) WITH NOWAIT;
    RAISERROR(N'CommandLog: %s', 10, 1, @commandlog_ref) WITH NOWAIT;
    RAISERROR(N'Analysis window: %i days', 10, 1, @DaysBack) WITH NOWAIT;
    DECLARE @obfuscate_int integer = CONVERT(integer, @Obfuscate);
    RAISERROR(N'Obfuscate: %i', 10, 1, @obfuscate_int) WITH NOWAIT;
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
        run_label           = s.ExtendedInfo.value(N''(Parameters/RunLabel)[1]'', N''nvarchar(100)''),
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
    /* Exclude currently running (started < 1 hour ago with no END) */
    AND   NOT (e.ID IS NULL AND DATEDIFF(MINUTE, s.StartTime, GETDATE()) < 60);
    ';

    EXECUTE sys.sp_executesql
        @sql,
        N'@days_back integer',
        @days_back = @DaysBack;

    DECLARE @run_count integer = (SELECT COUNT_BIG(*) FROM #runs);
    DECLARE @killed_count integer = (SELECT COUNT_BIG(*) FROM #runs WHERE IsKilled = 1);

    RAISERROR(N'  Runs found: %i (killed: %i)', 10, 1, @run_count, @killed_count) WITH NOWAIT;

    /*
    ============================================================================
    DATA EXTRACTION: Populate #stat_updates from UPDATE_STATISTICS entries
    ============================================================================
    */
    RAISERROR(N'Extracting stat update data...', 10, 1) WITH NOWAIT;

    SET @sql = N'
    INSERT INTO #stat_updates
    (
        ID, RunLabel, DatabaseName, SchemaName, ObjectName, StatisticsName,
        StartTime, EndTime, DurationMs, ErrorNumber, ErrorMessage,
        ModificationCounter, RowCount_, PageCount, SizeMB, DaysStale,
        QualifyReason, HasNorecompute, IsHeap, AutoCreated,
        QSPlanCount, QSTotalExecutions, QSTotalCpuMs,
        EffectiveSamplePct, SampleSource, HasFilter, FilteredDriftRatio, IsIncremental
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
        is_incremental      = c.ExtendedInfo.value(N''(ExtendedInfo/IsIncremental)[1]'', N''bit'')
    FROM ' + @commandlog_ref + N' AS c
    WHERE c.CommandType = N''UPDATE_STATISTICS''
    AND   c.StartTime >= DATEADD(DAY, -@days_back, GETDATE());
    ';

    EXECUTE sys.sp_executesql
        @sql,
        N'@days_back integer',
        @days_back = @DaysBack;

    DECLARE @stat_update_count integer = (SELECT COUNT_BIG(*) FROM #stat_updates);

    RAISERROR(N'  Stat updates found: %i', 10, 1, @stat_update_count) WITH NOWAIT;
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
            SELECT Severity, Category, Finding, Evidence, Recommendation, ExampleCall FROM #recommendations ORDER BY SortPriority, FindingID;
            SELECT [Status] = N'NO_DATA', TotalRuns = 0, CompletedRuns = 0, CompletionPct = 0, AvgDurationSec = 0, Trend = N'N/A';
            SELECT * FROM #runs WHERE 1 = 0;
            SELECT * FROM #stat_updates WHERE 1 = 0;
            SELECT * FROM #stat_updates WHERE 1 = 0;
            SELECT * FROM #stat_updates WHERE 1 = 0;
            SELECT * FROM #runs WHERE 1 = 0;
            IF @Obfuscate = 1 SELECT * FROM #obfuscation_map;
        END
        ELSE
        BEGIN
            /* Single result set mode: return NO_DATA recommendation row */
            SELECT
                ResultSetID   = 1,
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
        SELECT DISTINCT
            N'Table',
            su.ObjectName,
            N'TBL_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.ObjectName), 2), 6)
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

        /* Also snapshot databases from #runs */
        INSERT INTO #obfuscation_map (ObjectType, OriginalName, ObfuscatedName)
        SELECT DISTINCT
            N'RunDatabase',
            r.[Databases],
            N'DB_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + r.[Databases]), 2), 6)
        FROM #runs AS r
        WHERE r.[Databases] IS NOT NULL
        AND   NOT EXISTS (SELECT 1 FROM #obfuscation_map AS m WHERE m.OriginalName = r.[Databases] AND m.ObjectType = N'Database');

        /* Apply obfuscation to #stat_updates */
        UPDATE su
        SET
            su.DatabaseName = N'DB_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.DatabaseName), 2), 6),
            su.SchemaName = N'SCH_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.SchemaName), 2), 4),
            su.ObjectName = N'TBL_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.ObjectName), 2), 6),
            su.StatisticsName = CASE
                WHEN su.StatisticsName LIKE N'_WA_Sys_%' THEN N'_WA_Sys_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.StatisticsName), 2), 6)
                WHEN su.StatisticsName LIKE N'PK_%'       THEN N'PK_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.StatisticsName), 2), 6)
                WHEN su.StatisticsName LIKE N'IX_%'       THEN N'IX_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.StatisticsName), 2), 6)
                WHEN su.StatisticsName LIKE N'UQ_%'       THEN N'UQ_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.StatisticsName), 2), 6)
                ELSE N'STAT_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + su.StatisticsName), 2), 6)
            END
        FROM #stat_updates AS su;

        /* Obfuscate #runs.Databases */
        UPDATE r
        SET r.[Databases] = N'DB_' + RIGHT(CONVERT(varchar(8), HASHBYTES('MD5', @seed_prefix + r.[Databases]), 2), 6)
        FROM #runs AS r
        WHERE r.[Databases] IS NOT NULL;

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
            DECLARE @map_sql nvarchar(max);

            /* Auto-create table if it doesn't exist */
            SET @map_sql = N'
                IF OBJECT_ID(' + QUOTENAME(@ObfuscationMapTable, '''') + N') IS NULL
                BEGIN
                    CREATE TABLE ' + @ObfuscationMapTable + N' (
                        ObjectType     nvarchar(20)   NOT NULL,
                        OriginalName   nvarchar(256)  NOT NULL,
                        ObfuscatedName nvarchar(50)   NOT NULL,
                        CapturedAt     datetime2      NOT NULL DEFAULT SYSDATETIME()
                    );
                END;

                INSERT INTO ' + @ObfuscationMapTable + N' (ObjectType, OriginalName, ObfuscatedName)
                SELECT ObjectType, OriginalName, ObfuscatedName FROM #obfuscation_map;';

            EXECUTE sp_executesql @map_sql;

            DECLARE @map_table_msg nvarchar(200) = N'  Obfuscation map saved to ' + @ObfuscationMapTable;
            RAISERROR(@map_table_msg, 10, 1) WITH NOWAIT;
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
        @time_limit_runs integer = (SELECT COUNT_BIG(*) FROM #runs WHERE StopReason = N'TIME_LIMIT');

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
                + N'. Average time limit used: ' + CONVERT(nvarchar(10), AVG(r.TimeLimit)) + N' seconds ('
                + CONVERT(nvarchar(10), AVG(r.TimeLimit) / 60) + N' minutes)',
            N'Stats are consistently not finishing within the time limit. Options: '
                + N'(1) Increase @TimeLimit to ' + CONVERT(nvarchar(10), MAX(r.TimeLimit) * 2) + N' seconds, '
                + N'(2) Enable @LongRunningThresholdMinutes to cap slow individual stats, '
                + N'(3) Raise @ModificationThreshold to reduce qualifying stats, '
                + N'(4) Use @Preset = N''NIGHTLY_MAINTENANCE'' for balanced defaults.',
            N'EXECUTE dbo.sp_StatUpdate @Databases = N''USER_DATABASES'', @TimeLimit = '
                + CONVERT(nvarchar(10), MAX(r.TimeLimit) * 2)
                + N', @LongRunningThresholdMinutes = 30;',
            15
        FROM #runs AS r
        WHERE r.StopReason = N'TIME_LIMIT';

        RAISERROR(N'  [CRITICAL] C3: Time limit exhaustion detected', 10, 1) WITH NOWAIT;
    END;

    /* ======================================================================
       C4: DEGRADING THROUGHPUT
       ====================================================================== */
    ;WITH throughput_windows AS
    (
        SELECT
            window_label = CASE
                WHEN r.StartTime >= DATEADD(DAY, -@ThroughputWindowDays, GETDATE()) THEN N'RECENT'
                ELSE N'PRIOR'
            END,
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
            avg_sec = AVG(avg_sec_per_stat),
            run_count = COUNT_BIG(*)
        FROM throughput_windows
        GROUP BY window_label
    )
    INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
    SELECT
        N'CRITICAL',
        N'DEGRADING_THROUGHPUT',
        N'Throughput degraded: recent avg ' + CONVERT(nvarchar(10), CONVERT(decimal(10, 1), recent_w.avg_sec))
            + N' sec/stat vs. prior ' + CONVERT(nvarchar(10), CONVERT(decimal(10, 1), prior_w.avg_sec))
            + N' sec/stat (' + CONVERT(nvarchar(10), CONVERT(integer, (recent_w.avg_sec / prior_w.avg_sec - 1) * 100))
            + N'% slower)',
        N'Recent window: ' + CONVERT(nvarchar(10), recent_w.run_count) + N' runs averaging '
            + CONVERT(nvarchar(10), CONVERT(decimal(10, 1), recent_w.avg_sec)) + N' sec/stat. '
            + N'Prior window: ' + CONVERT(nvarchar(10), prior_w.run_count) + N' runs averaging '
            + CONVERT(nvarchar(10), CONVERT(decimal(10, 1), prior_w.avg_sec)) + N' sec/stat. '
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
    INNER JOIN window_avgs AS prior_w
        ON prior_w.window_label = N'PRIOR'
    WHERE recent_w.window_label = N'RECENT'
    AND   prior_w.avg_sec > 0
    AND   recent_w.avg_sec > prior_w.avg_sec * 1.5
    AND   prior_w.run_count >= 2
    AND   recent_w.run_count >= 1;

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
    INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
    SELECT TOP (@TopN)
        N'WARNING',
        N'LONG_RUNNING_STATS',
        N'Stat consistently slow: ' + su.DatabaseName + N'.'
            + su.ObjectName + N'.' + su.StatisticsName,
        N'Avg duration: ' + CONVERT(nvarchar(10), AVG(su.DurationMs) / 1000)
            + N' sec across ' + CONVERT(nvarchar(10), COUNT_BIG(*))
            + N' updates. Max size: ' + CONVERT(nvarchar(10), MAX(ISNULL(su.SizeMB, 0))) + N' MB'
            + N'. Avg rows: ' + CONVERT(nvarchar(20), AVG(su.RowCount_)),
        N'Enable adaptive sampling: stats that historically exceed a threshold get a reduced sample rate automatically.',
        N'EXECUTE dbo.sp_StatUpdate @LongRunningThresholdMinutes = '
            + CONVERT(nvarchar(10), @LongRunningMinutes)
            + N', @LongRunningSamplePercent = 10;',
        35
    FROM #stat_updates AS su
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
        AND   r.StatsFound > 0;

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
    IF EXISTS (
        SELECT 1 FROM #runs
        WHERE QueryStorePriority = N'Y'
    )
    AND NOT EXISTS (
        SELECT 1 FROM #stat_updates
        WHERE QSPlanCount > 0
    )
    BEGIN
        INSERT INTO #recommendations (Severity, Category, Finding, Evidence, Recommendation, ExampleCall, SortPriority)
        VALUES
        (
            N'WARNING', N'QS_NOT_EFFECTIVE',
            N'Query Store priority enabled but no QS data captured in stat updates',
            N'@QueryStorePriority = N''Y'' was used but zero stat updates have QSPlanCount > 0. '
                + N'This typically means Query Store is disabled, read-only, or has been purged.',
            N'Verify Query Store is enabled and in READ_WRITE mode on target databases: '
                + N'SELECT name, is_query_store_on FROM sys.databases. '
                + N'If QS is intentionally disabled, remove @QueryStorePriority to avoid unnecessary overhead.',
            N'/* Check: SELECT name, is_query_store_on FROM sys.databases WHERE state_desc = N''ONLINE''; */',
            35
        );

        RAISERROR(N'  [WARNING] W5: Query Store not effective', 10, 1) WITH NOWAIT;
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

    RAISERROR(N'', 10, 1) WITH NOWAIT;
    RAISERROR(N'Diagnostic checks complete.', 10, 1) WITH NOWAIT;
    RAISERROR(N'', 10, 1) WITH NOWAIT;

    /*
    ============================================================================
    RESULT SET 1: RECOMMENDATIONS
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
            ExampleCall
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
            ResultSetID   = 1,
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
                    ExampleCall    = r2.ExampleCall
                FROM #recommendations AS r2
                WHERE r2.FindingID = r.FindingID
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM #recommendations AS r;
    END;

    /*
    ============================================================================
    RESULT SET 2: RUN HEALTH SUMMARY
    ============================================================================
    */
    IF @SingleResultSet = 0
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
            );
    END
    ELSE
    BEGIN
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT
            ResultSetID   = 2,
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
                    StopReasonDistribution = (
                        SELECT STRING_AGG(stop_summary, N', ')
                        FROM (
                            SELECT r.StopReason + N': ' + CONVERT(nvarchar(10), COUNT_BIG(*)) AS stop_summary
                            FROM #runs AS r
                            WHERE r.IsKilled = 0
                            AND   r.StopReason IS NOT NULL
                            GROUP BY r.StopReason
                        ) AS sr
                    )
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            );
    END;

    /*
    ============================================================================
    RESULT SET 3: RUN DETAIL
    ============================================================================
    */
    IF @SingleResultSet = 0
    BEGIN
        SELECT
            RunLabel,
            StartTime,
            EndTime,
            DurationSeconds,
            DurationMinutes = DurationSeconds / 60,
            StopReason,
            StatsFound,
            StatsProcessed,
            StatsSucceeded,
            StatsFailed,
            StatsRemaining,
            AvgSecPerStat = CASE
                WHEN StatsProcessed > 0
                THEN CONVERT(decimal(10, 1), DurationSeconds * 1.0 / StatsProcessed)
                ELSE NULL
            END,
            IsKilled,
            [Version],
            [Databases],
            TimeLimit,
            ModificationThreshold,
            TieredThresholds,
            SortOrder,
            QueryStorePriority,
            StatsInParallel,
            Preset,
            LongRunningThresholdMinutes
        FROM #runs
        ORDER BY StartTime DESC;
    END
    ELSE
    BEGIN
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT
            ResultSetID   = 3,
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
                    LongRunningThresholdMinutes = r2.LongRunningThresholdMinutes
                FROM #runs AS r2
                WHERE r2.RunLabel = r.RunLabel
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        FROM #runs AS r;
    END;

    /*
    ============================================================================
    RESULT SET 4: TOP TABLES BY MAINTENANCE COST
    ============================================================================
    */
    IF @SingleResultSet = 0
    BEGIN
        SELECT TOP (@TopN)
            DatabaseName = su.DatabaseName,
            SchemaName = su.SchemaName,
            TableName = su.ObjectName,
            TotalUpdates = COUNT_BIG(*),
            TotalDurationSec = SUM(su.DurationMs) / 1000,
            AvgDurationMs = AVG(su.DurationMs),
            MaxDurationMs = MAX(su.DurationMs),
            AvgModCounter = AVG(su.ModificationCounter),
            MaxSizeMB = MAX(su.SizeMB),
            MaxRowCount = MAX(su.RowCount_),
            FailCount = SUM(CASE WHEN su.ErrorNumber > 0 THEN 1 ELSE 0 END),
            IsHeap = MAX(CONVERT(integer, ISNULL(su.IsHeap, 0))),
            HasNorecompute = MAX(CONVERT(integer, ISNULL(su.HasNorecompute, 0))),
            DistinctStats = COUNT(DISTINCT su.StatisticsName)
        FROM #stat_updates AS su
        GROUP BY su.DatabaseName, su.SchemaName, su.ObjectName
        ORDER BY SUM(su.DurationMs) DESC;
    END
    ELSE
    BEGIN
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT TOP (@TopN)
            ResultSetID   = 4,
            ResultSetName = N'Top Tables',
            RowNum        = ROW_NUMBER() OVER (ORDER BY SUM(su.DurationMs) DESC),
            RowData       = (
                SELECT
                    DatabaseName     = su2.DatabaseName,
                    SchemaName       = su2.SchemaName,
                    TableName        = su2.ObjectName,
                    TotalUpdates     = COUNT_BIG(*),
                    TotalDurationSec = SUM(su2.DurationMs) / 1000,
                    AvgDurationMs    = AVG(su2.DurationMs),
                    MaxDurationMs    = MAX(su2.DurationMs),
                    AvgModCounter    = AVG(su2.ModificationCounter),
                    MaxSizeMB        = MAX(su2.SizeMB),
                    MaxRowCount      = MAX(su2.RowCount_),
                    FailCount        = SUM(CASE WHEN su2.ErrorNumber > 0 THEN 1 ELSE 0 END),
                    IsHeap           = MAX(CONVERT(integer, ISNULL(su2.IsHeap, 0))),
                    HasNorecompute   = MAX(CONVERT(integer, ISNULL(su2.HasNorecompute, 0))),
                    DistinctStats    = COUNT(DISTINCT su2.StatisticsName)
                FROM #stat_updates AS su2
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
    RESULT SET 5: FAILING STATISTICS
    ============================================================================
    */
    IF @SingleResultSet = 0
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
    ELSE
    BEGIN
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT TOP (@TopN)
            ResultSetID   = 5,
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
    RESULT SET 6: LONG-RUNNING STATISTICS
    ============================================================================
    */
    IF @SingleResultSet = 0
    BEGIN
        SELECT TOP (@TopN)
            DatabaseName = su.DatabaseName,
            SchemaName = su.SchemaName,
            TableName = su.ObjectName,
            StatisticsName = su.StatisticsName,
            UpdateCount = COUNT_BIG(*),
            AvgDurationSec = AVG(su.DurationMs) / 1000,
            MaxDurationSec = MAX(su.DurationMs) / 1000,
            MinDurationSec = MIN(su.DurationMs) / 1000,
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
            IsHeap = MAX(CONVERT(integer, ISNULL(su.IsHeap, 0)))
        FROM #stat_updates AS su
        WHERE su.ErrorNumber = 0
        AND   su.DurationMs > @LongRunningMinutes * 60 * 1000
        GROUP BY su.DatabaseName, su.SchemaName, su.ObjectName, su.StatisticsName
        ORDER BY AVG(su.DurationMs) DESC;
    END
    ELSE
    BEGIN
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT TOP (@TopN)
            ResultSetID   = 6,
            ResultSetName = N'Long-Running Statistics',
            RowNum        = ROW_NUMBER() OVER (ORDER BY AVG(su.DurationMs) DESC),
            RowData       = (
                SELECT
                    DatabaseName   = su2.DatabaseName,
                    SchemaName     = su2.SchemaName,
                    TableName      = su2.ObjectName,
                    StatisticsName = su2.StatisticsName,
                    UpdateCount    = COUNT_BIG(*),
                    AvgDurationSec = AVG(su2.DurationMs) / 1000,
                    MaxDurationSec = MAX(su2.DurationMs) / 1000,
                    MinDurationSec = MIN(su2.DurationMs) / 1000,
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
                    IsHeap         = MAX(CONVERT(integer, ISNULL(su2.IsHeap, 0)))
                FROM #stat_updates AS su2
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
    RESULT SET 7: PARAMETER CHANGE HISTORY
    ============================================================================
    */
    IF @SingleResultSet = 0
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
    ELSE
    BEGIN
        INSERT INTO #single_rs (ResultSetID, ResultSetName, RowNum, RowData)
        SELECT
            ResultSetID   = 7,
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
    RESULT SET 8: OBFUSCATION MAP (conditional)
    ============================================================================
    */
    IF @Obfuscate = 1
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

    /*
    ============================================================================
    CLEANUP
    ============================================================================
    */
    DROP TABLE IF EXISTS #runs;
    DROP TABLE IF EXISTS #stat_updates;
    DROP TABLE IF EXISTS #recommendations;
    DROP TABLE IF EXISTS #obfuscation_map;

    RETURN;
END;
GO
