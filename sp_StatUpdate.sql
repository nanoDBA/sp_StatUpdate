SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/*

sp_StatUpdate v3 - Priority-Based Statistics Maintenance

Copyright (c) 2026 Community Contribution
https://github.com/nanoDBA/sp_StatUpdate

Purpose:    Update statistics with DMV-based priority ordering and time limits.
            Refreshes stale stats including those with NORECOMPUTE (flag preserved).

Based on:   Ola Hallengren's IndexOptimize (MIT License)
            https://ola.hallengren.com

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

Version:    3.2.2.2026.04.17 (Major.Minor.Patch.YYYY.MM.DD)
            - Version logged to CommandLog ExtendedInfo on each run
            - Query: ExtendedInfo.value('(/Parameters/Version)[1]', 'nvarchar(20)')

History:    3.2.2.2026.04.17 - Internal refactors (3 issues, behavior-preserving):
                            gh-461 @parameters_string built unconditionally in region 07B;
                              removed duplicate construction in region 08.
                            gh-462 mop-up discovery WHERE filter extracted into
                              @mop_up_where_sql -- both parallel-leader and serial
                              paths reference the shared variable.
                            gh-465 empty-schema SELECT (WHERE 1=0 sentinel, ~35 cols)
                              extracted into @empty_disc_select -- all 6 staged
                              discovery bailout paths reference the shared variable.
            3.2.1.2026.04.17 - Phase 5 bug fix (1 issue):
                            gh-492 DIRECT_STRING discovery path (@Statistics param)
                              now honors @TargetNorecompute and @ExcludeStatistics.
                              Previously both filters were only applied in the
                              staged discovery path.
            3.2.2026.04.17 - Phase 4 quality/perf batch (9 issues):
                            gh-470 per-database warning block (6 checks) now
                              short-circuits when the database has zero stats
                              in #stats_to_process -- saves up to 6 sp_executesql
                              compilations + DMV scans per skipped db.
                            gh-460 Phase 6 plan feedback query now bounded by
                              @i_qs_recent_hours + @i_qs_top_plans -- prevents
                              unbounded XML parsing on deep QS retention.
                            gh-463 five COUNT_BIG(*) full-scans of #stats_to_process
                              collapsed to one SUM(CASE) pass (NORECOMPUTE,
                              incremental, heap, memory-opt, persisted sample).
                            gh-464 orphan CommandLog cleanup materializes END
                              RunLabels into a temp table -- replaces correlated
                              per-row XML NOT EXISTS.
                            gh-466 MAX_GRANT_PERCENT hint now token-replaced
                              ({{MAX_GRANT_HINT}}) instead of sentinel-replacing
                              the literal '= 25)' (fragile).
                            gh-467 sys.partitions count for incremental stats
                              cached per (database, object_id) across consecutive
                              stats on the same table.
                            gh-468 six per-database warning checks (RLS, wide stats,
                              filter mismatch, columnstore, compressed, computed
                              cols) now log failures to @warnings instead of
                              silently swallowing in empty CATCH blocks.
                            gh-469 threshold-logic explanation block now gated
                              on @Debug = 1 (was firing on every run).
            3.1.2026.04.17 - Phase 1 correctness batch (9 issues):
                            gh-451 parallel early-return paths (FINGERPRINT_CONFLICT,
                              MAX_WORKERS, QUEUE_INIT_ERROR) now set all OUTPUT params
                              and return the summary result set.
                            gh-452 second LOCK_TIMEOUT restore after the forced-plan
                              check (sp_executesql SET LOCK_TIMEOUT persists).
                            gh-453 @QueryStore = AVG_CPU now sorts by average CPU
                              (divides SUM by SUM(count_executions)), not total CPU.
                            gh-454 @parameter_fingerprint now includes
                              @QueryStoreMinExecutions and @QueryStoreRecentHours so
                              workers with different QS criteria cannot share a queue.
                            gh-455 @MopUpPass + @StopByTime (without @TimeLimit) no
                              longer rejected at validation.
                            gh-456 bracket-quoted names in @Statistics
                              (e.g. 'dbo.[My Table].[My Stat]') are now matched --
                              brackets stripped from PARSENAME output.
                            gh-457 @log_used_pct reset per-iteration so a failed
                              sp_executesql cannot carry a stale value across dbs.
                            gh-458 @SortOrder = RANDOM + @QueryStore enabled now
                              raises a startup warning (QS boost is ignored).
                            gh-459 parallel mop-up follower now reports correct
                              MopUpFound (uses @mop_up_queued, not local
                              #stats_to_process which has 0 unprocessed rows).
            3.0.2026.04.07 - v3 architecture: preset-first API, 33 input params (was 58),
                            + 10 OUTPUT params.  Absorbed 25 params into @i_ internal
                            variables controlled by presets.  Table-driven validation,
                            6-phase staged discovery only (no fallback path), removed join
                            pattern grouping, removed global temp progress table, unified
                            mop-up filters.  Full behavioral parity with v2.37.

Key Features:
    - Preset-first API: DEFAULT, NIGHTLY, WEEKLY_FULL, OLTP_LIGHT, WAREHOUSE
    - 6-phase staged discovery (only path, no legacy fallback)
    - Queue-based parallelism for large databases
    - Priority ordering (worst stats first)
    - Incremental statistics support (ON PARTITIONS)
    - RESAMPLE option for preserving sample rates
    - Availability Group awareness (prevents write attempts on secondaries)
    - Memory-optimized table handling
    - Mop-up pass: broad sweep with remaining time budget
    - Query Store integration: CPU, DURATION, READS, MEMORY_GRANT, TEMPDB_SPILLS,
      PHYSICAL_READS, AVG_MEMORY, WAITS, AVG_CPU, EXECUTIONS

DROP-IN COMPATIBILITY with Ola Hallengren's SQL Server Maintenance Solution:
    https://ola.hallengren.com

    REQUIRED:
      - dbo.CommandLog table (https://ola.hallengren.com/scripts/CommandLog.sql)
        Used for all logging.  Set @LogToTable = 'N' if you don't have it.

    OPTIONAL (for @StatsInParallel = 'Y'):
      - dbo.Queue table (https://ola.hallengren.com/scripts/Queue.sql)
      - dbo.QueueStatistic table (auto-created on first parallel run)

    NOT REQUIRED:
      - dbo.CommandExecute - sp_StatUpdate handles its own command execution

Note:       NORECOMPUTE flag is PRESERVED on update (not cleared).
            To clear NORECOMPUTE, manually DROP + CREATE the statistic.
*/

IF OBJECT_ID(N'dbo.sp_StatUpdate', N'P') IS NULL
BEGIN
    EXECUTE (N'CREATE PROCEDURE dbo.sp_StatUpdate AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    dbo.sp_StatUpdate
(
    /*
    ============================================================================
    v3 SIMPLIFIED API
      33 input parameters (was 58 in v2) + 10 OUTPUT parameters
      25 input parameters absorbed into preset-controlled @i_ internal variables.
      Explicit params always override preset defaults.

    INPUT (33):  @Statistics, @Preset, @Databases, @Tables, @ExcludeTables,
                 @ExcludeStatistics, @TargetNorecompute, @StaleHours, @QueryStore,
                 @TimeLimit, @StopByTime, @BatchLimit, @SortOrder, @MopUpPass,
                 @StatisticsSample, @MaxDOP, @ModificationThreshold,
                 @LongRunningThresholdMinutes, @LongRunningSamplePercent,
                 @QueryStoreTopPlans, @QueryStoreMinExecutions, @QueryStoreRecentHours,
                 @MaxAGRedoQueueMB, @MaxAGWaitMinutes, @MinTempdbFreeMB,
                 @MopUpMinRemainingSeconds, @LogToTable, @Execute, @WhatIfOutputTable,
                 @FailFast, @Debug, @StatsInParallel, @Help

    OUTPUT (10): @Version, @VersionDate, @StatsFoundOut, @StatsProcessedOut,
                 @StatsSucceededOut, @StatsFailedOut, @StatsRemainingOut,
                 @DurationSecondsOut, @WarningsOut, @StopReasonOut
    ============================================================================
    */

    /* MODE 1: DIRECT -- pass specific statistics (skips DMV discovery) */
    @Statistics sysname = NULL,                     /* 'Schema.Table.Stat' or 'Table.Stat', comma-separated */

    /* MODE 2: DISCOVERY -- DMV-based candidate selection */
    @Preset nvarchar(20) = N'DEFAULT',              /* DEFAULT, NIGHTLY, WEEKLY_FULL, OLTP_LIGHT, WAREHOUSE */
    @Databases nvarchar(max) = NULL,                /* NULL = current DB. SYSTEM_DATABASES, USER_DATABASES, ALL_DATABASES, wildcards (%), exclusions (-) */
    @Tables nvarchar(max) = NULL,                   /* NULL = all tables, or comma-separated 'Schema.Table' */
    @ExcludeTables nvarchar(max) = NULL,            /* comma-separated patterns (supports %) */
    @ExcludeStatistics nvarchar(max) = NULL,        /* comma-separated patterns (supports %) */
    @TargetNorecompute nvarchar(10) = N'BOTH',       /* Y = only NORECOMPUTE, N = only regular, BOTH = all */
    @StaleHours int = NULL,                         /* minimum hours since last update (replaces @DaysStaleThreshold + @HoursStaleThreshold) */

    /* QUERY STORE -- single param replaces @QueryStorePriority + @QueryStoreMetric */
    @QueryStore nvarchar(20) = NULL,                /* OFF or metric name: CPU, DURATION, READS, EXECUTIONS, AVG_CPU, MEMORY_GRANT, TEMPDB_SPILLS, PHYSICAL_READS, AVG_MEMORY, WAITS.  NULL = preset decides. */

    /* EXECUTION CONTROL */
    @TimeLimit integer = NULL,                      /* seconds. NULL = preset decides (DEFAULT=18000, NIGHTLY=3600, etc.) */
    @StopByTime nvarchar(8) = NULL,                 /* absolute stop time: HHMM, HH:MM, HH:MM:SS. Overrides @TimeLimit. */
    @BatchLimit integer = NULL,                     /* max stats per run */
    @SortOrder nvarchar(50) = NULL,                 /* NULL = preset decides. MODIFICATION_COUNTER, DAYS_STALE, PAGE_COUNT, RANDOM, QUERY_STORE, FILTERED_DRIFT, AUTO_CREATED, ROWS */
    @MopUpPass nvarchar(1) = NULL,                  /* Y/N. NULL = preset decides.  Broad sweep after priority pass. */
    @StatisticsSample integer = NULL,               /* 1-100 (100=FULLSCAN), NULL = let SQL decide */
    @MaxDOP integer = NULL,                         /* MAXDOP for UPDATE STATISTICS (SQL 2016 SP2+). NULL = server default */
    @ModificationThreshold bigint = NULL,           /* Override preset mod threshold floor (large tables). NULL = preset decides */
    @LongRunningThresholdMinutes integer = NULL,    /* Stats historically slower than this get forced sample rate. NULL = disabled */
    @LongRunningSamplePercent integer = NULL,        /* Sample percent for long-running stats (default preset: 10%). NULL = preset decides */

    /* QUERY STORE TUNING */
    @QueryStoreTopPlans integer = NULL,             /* Limit Phase 6 XML plan parsing to top N plans. NULL = preset decides (default 500). */
    @QueryStoreMinExecutions integer = NULL,         /* Min plan executions for QS boost (default 100). NULL = preset decides. */
    @QueryStoreRecentHours integer = NULL,           /* Only consider QS plans from last N hours (default 168=7d). NULL = preset decides. */

    /* AVAILABILITY GROUP & TEMPDB SAFETY */
    @MaxAGRedoQueueMB integer = NULL,               /* Pause if AG secondary redo queue exceeds this MB.  NULL = disabled. */
    @MaxAGWaitMinutes integer = NULL,               /* Max minutes to wait for AG redo queue to drain (default 10). */
    @MinTempdbFreeMB integer = NULL,                /* Min free tempdb space in MB.  NULL = disabled. */

    /* MOP-UP TUNING */
    @MopUpMinRemainingSeconds integer = NULL,       /* Min seconds remaining to trigger mop-up.  NULL = preset decides (default 60). */

    /* LOGGING & OUTPUT */
    @LogToTable nvarchar(1) = N'Y',                 /* Y = log to dbo.CommandLog, N = skip */
    @Execute nvarchar(1) = N'Y',                    /* Y = execute, N = dry run */
    @WhatIfOutputTable nvarchar(500) = NULL,         /* table for commands when @Execute = N */
    @FailFast bit = 0,                              /* 1 = abort on first error */
    @Debug bit = 0,                                 /* 1 = verbose output */

    /* PARALLEL EXECUTION */
    @StatsInParallel nvarchar(1) = N'N',            /* Y = queue-based parallel processing */

    /* HELP & VERSION */
    @Help nvarchar(50) = N'0',                      /* 1 = show help */
    @Version varchar(20) = NULL OUTPUT,
    @VersionDate datetime = NULL OUTPUT,

    /* SUMMARY OUTPUT */
    @StatsFoundOut integer = NULL OUTPUT,
    @StatsProcessedOut integer = NULL OUTPUT,
    @StatsSucceededOut integer = NULL OUTPUT,
    @StatsFailedOut integer = NULL OUTPUT,
    @StatsRemainingOut integer = NULL OUTPUT,
    @DurationSecondsOut integer = NULL OUTPUT,
    @WarningsOut nvarchar(max) = NULL OUTPUT,
    @StopReasonOut nvarchar(50) = NULL OUTPUT
)
WITH RECOMPILE
AS
BEGIN

    /*#region 01-INIT: SET options, version constants */
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    SET ARITHABORT ON;
    SET NUMERIC_ROUNDABORT OFF;

    DECLARE
        @procedure_version varchar(20) = '3.2.2.2026.04.17',
        @procedure_version_date datetime = '20260417',
        @procedure_name sysname = OBJECT_NAME(@@PROCID),
        @procedure_schema sysname = OBJECT_SCHEMA_NAME(@@PROCID);

    SELECT
        @Version = @procedure_version,
        @VersionDate = @procedure_version_date;
    /*#endregion 01-INIT */

    /*#region 02-HELP: Condensed table-driven help */
    IF @Help IN (N'1', N'Y')
    BEGIN
        /* RS 1: Introduction */
        SELECT introduction = line_text
        FROM (VALUES
            (1, N'sp_StatUpdate v3 -- Priority-Based Statistics Maintenance'),
            (2, N'Version: ' + @procedure_version + N' (' + CONVERT(nvarchar(10), @procedure_version_date, 120) + N')'),
            (3, N'https://github.com/nanoDBA/sp_StatUpdate'),
            (4, N''),
            (5, N'v3 simplifies the API from 58 to 33 input parameters (+ 10 OUTPUT).  Use @Preset'),
            (6, N'for common configurations, override individual settings as needed.'),
            (7, N''),
            (8, N'Quick start:'),
            (9, N'  EXEC sp_StatUpdate @Databases = N''USER_DATABASES'';                           -- DEFAULT preset'),
            (10, N'  EXEC sp_StatUpdate @Preset = N''NIGHTLY'', @Databases = N''USER_DATABASES'';    -- 1hr, QS-prioritized'),
            (11, N'  EXEC sp_StatUpdate @Preset = N''WEEKLY_FULL'', @Execute = N''N'';               -- dry run'),
            (12, N'  EXEC sp_StatUpdate @QueryStore = N''CPU'', @MopUpPass = N''Y'';                 -- QS + mop-up'),
            (13, N''),
            (14, N'Run sp_StatUpdate_Diag for post-run diagnostics and recommendations.')
        ) AS h(sort_order, line_text)
        ORDER BY h.sort_order;

        /* RS 2: Preset definitions */
        SELECT
            preset_name = p.preset_name,
            time_limit = p.time_limit,
            sort_order = p.sort_order,
            query_store = p.query_store,
            mop_up = p.mop_up,
            sample = p.sample_pct,
            description = p.description
        FROM (VALUES
            (N'DEFAULT',     N'18000 (5h)', N'MODIFICATION_COUNTER', N'OFF',  N'N', N'auto', N'Balanced default. Tiered thresholds, 5h limit.'),
            (N'NIGHTLY',     N'3600 (1h)',  N'QUERY_STORE',          N'CPU',  N'Y', N'auto', N'QS-prioritized nightly job with mop-up.'),
            (N'WEEKLY_FULL', N'14400 (4h)', N'QUERY_STORE',          N'CPU',  N'Y', N'100',  N'Comprehensive weekly: FULLSCAN + lower thresholds.'),
            (N'OLTP_LIGHT',  N'1800 (30m)', N'MODIFICATION_COUNTER', N'OFF',  N'N', N'auto', N'Low-impact OLTP: high threshold, inter-stat delay.'),
            (N'WAREHOUSE',   N'unlimited',  N'ROWS',                 N'OFF',  N'N', N'100',  N'Data warehouse: FULLSCAN, no time limit.')
        ) AS p(preset_name, time_limit, sort_order, query_store, mop_up, sample_pct, description);

        /* RS 3: Parameter reference */
        SELECT
            parameter = r.parameter,
            [type] = r.data_type,
            [default] = r.default_value,
            description = r.description
        FROM (VALUES
            (N'@Statistics',        N'sysname',       N'NULL',      N'Direct mode: Schema.Table.Stat (comma-separated)'),
            (N'@Preset',            N'nvarchar(20)',   N'DEFAULT',   N'DEFAULT, NIGHTLY, WEEKLY_FULL, OLTP_LIGHT, WAREHOUSE'),
            (N'@Databases',         N'nvarchar(max)',  N'NULL',      N'NULL=current DB. USER_DATABASES, ALL_DATABASES, wildcards, exclusions'),
            (N'@Tables',            N'nvarchar(max)',  N'NULL',      N'NULL=all tables, or Schema.Table list'),
            (N'@ExcludeTables',     N'nvarchar(max)',  N'NULL',      N'Patterns to exclude (supports %)'),
            (N'@ExcludeStatistics', N'nvarchar(max)',  N'NULL',      N'Patterns to exclude (supports %)'),
            (N'@TargetNorecompute', N'nvarchar(10)',   N'BOTH',      N'BOTH=all stats (default), Y=NORECOMPUTE only, N=regular only'),
            (N'@StaleHours',        N'int',            N'NULL',      N'Min hours since last update'),
            (N'@QueryStore',        N'nvarchar(20)',   N'NULL',      N'OFF or metric: CPU, DURATION, READS, EXECUTIONS, etc.'),
            (N'@TimeLimit',         N'integer',        N'NULL',      N'Seconds. NULL=preset decides'),
            (N'@StopByTime',        N'nvarchar(8)',    N'NULL',      N'Absolute stop: HHMM or HH:MM:SS. Overrides @TimeLimit'),
            (N'@BatchLimit',        N'integer',        N'NULL',      N'Max stats per run'),
            (N'@SortOrder',         N'nvarchar(50)',   N'NULL',      N'NULL=preset. MODIFICATION_COUNTER, QUERY_STORE, etc.'),
            (N'@MopUpPass',         N'nvarchar(1)',    N'NULL',      N'Y/N. Broad sweep with remaining time'),
            (N'@StatisticsSample',  N'integer',        N'NULL',      N'1-100 (100=FULLSCAN), NULL=auto'),
            (N'@MaxDOP',            N'integer',        N'NULL',      N'MAXDOP for UPDATE STATISTICS'),
            (N'@ModificationThreshold', N'bigint',     N'NULL',      N'Override preset mod threshold floor. NULL=preset decides'),
            (N'@LongRunningThresholdMinutes', N'integer', N'NULL',   N'Stats slower than this get forced sample. NULL=disabled'),
            (N'@LongRunningSamplePercent', N'integer',  N'NULL',     N'Sample pct for long-running stats. NULL=preset (10%)'),
            (N'@QueryStoreTopPlans', N'integer',       N'NULL',      N'Limit Phase 6 XML plan parsing. NULL=preset (500)'),
            (N'@QueryStoreMinExecutions', N'integer',  N'NULL',      N'Min plan executions for QS boost. NULL=preset (100)'),
            (N'@QueryStoreRecentHours', N'integer',    N'NULL',      N'QS plans from last N hours. NULL=preset (168=7d)'),
            (N'@MaxAGRedoQueueMB',  N'integer',        N'NULL',      N'Pause if AG redo queue exceeds MB. NULL=disabled'),
            (N'@MaxAGWaitMinutes',  N'integer',        N'NULL',      N'Max minutes to wait for AG redo drain (default 10)'),
            (N'@MinTempdbFreeMB',   N'integer',        N'NULL',      N'Min free tempdb MB. NULL=disabled'),
            (N'@MopUpMinRemainingSeconds', N'integer', N'NULL',      N'Min seconds remaining to trigger mop-up. NULL=preset (60)'),
            (N'@LogToTable',        N'nvarchar(1)',    N'Y',         N'Log to dbo.CommandLog'),
            (N'@Execute',           N'nvarchar(1)',    N'Y',         N'Y=execute, N=dry run'),
            (N'@WhatIfOutputTable', N'nvarchar(500)',  N'NULL',      N'Table for dry-run commands'),
            (N'@FailFast',          N'bit',            N'0',         N'Abort on first error'),
            (N'@Debug',             N'bit',            N'0',         N'Verbose output'),
            (N'@StatsInParallel',   N'nvarchar(1)',    N'N',         N'Queue-based parallel processing'),
            (N'@Help',              N'nvarchar(50)',   N'0',         N'1 = show this help')
        ) AS r(parameter, data_type, default_value, description);

        /* RS 4: Help topics */
        SELECT
            topic = t.topic,
            detail = t.detail
        FROM (VALUES
            (N'QUICK START',   N'Use @Preset for a pre-tuned config.  Override individual params as needed.  @Execute=N for dry run.'),
            (N'PRESETS',       N'DEFAULT=balanced, NIGHTLY=1h+QS+mop-up, WEEKLY_FULL=4h+FULLSCAN, OLTP_LIGHT=30m+high-thresh, WAREHOUSE=unlimited+FULLSCAN.'),
            (N'PARALLEL',      N'@StatsInParallel=Y. Requires dbo.Queue + dbo.QueueStatistic (auto-created). Run same EXEC from multiple Agent steps.'),
            (N'QUERY STORE',   N'@QueryStore=CPU/DURATION/READS/etc. to prioritize stats on high-workload tables.  OFF to disable.'),
            (N'MOP-UP',        N'@MopUpPass=Y: after priority pass, sweep any stat with modification_counter>0.  Needs @TimeLimit + @LogToTable=Y.'),
            (N'DIRECT MODE',   N'@Statistics=''Schema.Table.Stat'' for ad-hoc updates. Skips discovery.  Comma-separate for multiple.'),
            (N'STALE HOURS',   N'@StaleHours=48 means skip stats updated within 48 hours.  Replaces v2 @DaysStaleThreshold/@HoursStaleThreshold.'),
            (N'SCHEDULING',    N'SQL Agent job with @Preset=NIGHTLY + @StopByTime=0500 for a 5 AM deadline.  Use @MopUpPass=Y for thorough coverage.'),
            (N'NORECOMPUTE',   N'NORECOMPUTE flag is PRESERVED on update (not cleared).  To clear: manually DROP+CREATE the statistic.'),
            (N'COMMANDLOG',    N'Requires dbo.CommandLog from https://ola.hallengren.com/scripts/CommandLog.sql.  Set @LogToTable=N if not installed.'),
            (N'DIAGNOSTICS',   N'Run sp_StatUpdate_Diag after for automated analysis.  @ExpertMode=1 for full detail, 0 for dashboard only.'),
            (N'VERSION',       N'v3.0 -- simplified API (33 input + 10 OUTPUT vs 58 input + 10 OUTPUT in v2).  Full behavioral parity with v2.37.  Preset-first design.')
        ) AS t(topic, detail);

        RETURN;
    END;
    /*#endregion 02-HELP */
    /*#region 03-SETUP: Re-entrancy, variables, guards, presets, validation */
    /*#region 03A-TRANCOUNT: Transaction guard + StatUpdateLock re-entrancy */
    /*
    Check transaction count BEFORE acquiring re-entrancy lock -- UPDATE STATISTICS
    acquires Sch-M locks that escalate unpredictably inside a caller's transaction.
    Must precede lock acquisition to avoid masking the real error.
    */
    IF @@TRANCOUNT <> 0
    BEGIN
        RAISERROR(N'The transaction count is not 0.  sp_StatUpdate must not be called within an open transaction.', 16, 1) WITH NOWAIT;
        SET @StopReasonOut = N'PARAMETER_ERROR';
        SET @StatsFoundOut = 0;
        SET @StatsProcessedOut = 0;
        SET @StatsSucceededOut = 0;
        SET @StatsFailedOut = 0;
        SET @StatsRemainingOut = 0;
        SET @DurationSecondsOut = 0;
        SELECT
            Status = N'ERROR', StatusMessage = N'Open transaction detected.',
            StatsFound = 0, StatsProcessed = 0, StatsSucceeded = 0, StatsFailed = 0,
            StatsToctou = 0, StatsSkipped = 0, StatsRemaining = 0,
            DatabasesProcessed = 0, DurationSeconds = 0,
            StopReason = N'PARAMETER_ERROR', RunLabel = CONVERT(nvarchar(100), NULL),
            Version = @procedure_version;
        RETURN 1;
    END;

    /*
    ============================================================================
    RE-ENTRANCY GUARD (queue-table pattern, bd -j9d)
    Prevents concurrent non-parallel runs from corrupting shared state
    (orphan cleanup, progress tables, CommandLog bracketing).
    Uses a row in dbo.StatUpdateLock with (SessionID, LoginTime) liveness
    tuple instead of sp_getapplock.  Dead holders are auto-reclaimed via
    sys.dm_exec_sessions -- no ALREADY_RUNNING cascade when sessions die.
    Parallel mode workers skip this guard -- they coordinate via dbo.Queue +
    dbo.QueueStatistic.
    ============================================================================
    */
    IF NOT EXISTS (
        SELECT 1 FROM sys.objects AS o
        JOIN sys.schemas AS s ON s.schema_id = o.schema_id
        WHERE o.type = N'U' AND s.name = N'dbo' AND o.name = N'StatUpdateLock'
    )
    BEGIN
        CREATE TABLE dbo.StatUpdateLock (
            Resource sysname NOT NULL,
            SessionID smallint NOT NULL,
            LoginTime datetime NOT NULL,
            AcquiredAt datetime2(3) NOT NULL DEFAULT SYSDATETIME(),
            CONSTRAINT PK_StatUpdateLock PRIMARY KEY CLUSTERED (Resource)
        );
    END;

    IF @StatsInParallel = N'N'
    BEGIN
        DECLARE @my_login_time datetime =
            (SELECT s.login_time FROM sys.dm_exec_sessions AS s WHERE s.session_id = @@SPID);

        DECLARE @existing_sid smallint, @existing_lt datetime;

        BEGIN TRANSACTION;

        SELECT @existing_sid = SessionID, @existing_lt = LoginTime
        FROM dbo.StatUpdateLock WITH (UPDLOCK, HOLDLOCK)
        WHERE Resource = N'sp_StatUpdate';

        IF @existing_sid IS NULL
        BEGIN
            INSERT INTO dbo.StatUpdateLock (Resource, SessionID, LoginTime)
            VALUES (N'sp_StatUpdate', @@SPID, @my_login_time);
        END
        ELSE IF EXISTS (
            SELECT 1 FROM sys.dm_exec_sessions
            WHERE session_id = @existing_sid AND login_time = @existing_lt
        )
        BEGIN
            COMMIT TRANSACTION;
            RAISERROR(N'Another instance of sp_StatUpdate is already running (non-parallel mode). Use @StatsInParallel=''Y'' for concurrent execution.', 16, 1);
            SET @StopReasonOut = N'ALREADY_RUNNING';
            SET @StatsFoundOut = 0;
            SET @StatsProcessedOut = 0;
            SET @StatsSucceededOut = 0;
            SET @StatsFailedOut = 0;
            SET @StatsRemainingOut = 0;
            SET @DurationSecondsOut = 0;
            SELECT
                Status = N'ERROR', StatusMessage = N'Another instance already running.',
                StatsFound = 0, StatsProcessed = 0, StatsSucceeded = 0, StatsFailed = 0,
                StatsToctou = 0, StatsSkipped = 0, StatsRemaining = 0,
                DatabasesProcessed = 0, DurationSeconds = 0,
                StopReason = N'ALREADY_RUNNING', RunLabel = CONVERT(nvarchar(100), NULL),
                Version = @procedure_version;
            RETURN;
        END
        ELSE
        BEGIN
            /* Dead holder -- reclaim */
            UPDATE dbo.StatUpdateLock
            SET SessionID = @@SPID, LoginTime = @my_login_time, AcquiredAt = SYSDATETIME()
            WHERE Resource = N'sp_StatUpdate';
        END;

        COMMIT TRANSACTION;
    END;
    /*#endregion 03A-TRANCOUNT */
    /*#region 03B-VARIABLES: Version detection, trace flags, table variables, temp tables */
    /*
    ============================================================================
    VARIABLE DECLARATIONS
    ============================================================================
    */
    DECLARE
        @start_time datetime2(7) = SYSDATETIME(),
        @empty_line nvarchar(max) = N'',
        @error_number integer = 0,
        @return_code integer = 0,
        /*
        Capture original LOCK_TIMEOUT to restore after stat updates (P1 #23 v1.9)
        @@LOCK_TIMEOUT returns -1 for infinite wait, or timeout in milliseconds
        */
        @original_lock_timeout integer = @@LOCK_TIMEOUT;

    /*
    SQL Server version detection
    Major version: 13 = 2016, 14 = 2017, 15 = 2019, 16 = 2022, 17 = 2025
    Build number used for feature detection (e.g., PERSIST_SAMPLE_PERCENT, MAXDOP in UPDATE STATISTICS)
    Future versions (>= 17) inherit SQL 2022 feature set -- all >= comparisons forward-compatible.
    */
    DECLARE
        @sql_version numeric(18, 10) =
            CONVERT
            (
                numeric(18, 10),
                LEFT
                (
                    CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')),
                    CHARINDEX
                    (
                        N'.',
                        CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion'))
                    ) - 1
                ) +
                N'.' +
                REPLACE
                (
                    RIGHT
                    (
                        CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')),
                        LEN(CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion'))) -
                        CHARINDEX
                        (
                            N'.',
                            CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion'))
                        )
                    ),
                    N'.',
                    N''
                )
            ),
        @sql_major_version integer =
            CONVERT
            (
                integer,
                LEFT
                (
                    CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')),
                    CHARINDEX
                    (
                        N'.',
                        CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion'))
                    ) - 1
                )
            ),
        @sql_build_number integer =
            CONVERT
            (
                integer,
                PARSENAME(CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')), 2)
            ),
        /*
        Feature availability flags based on SQL Server version/build:
        - PERSIST_SAMPLE_PERCENT: SQL 2016 SP1 CU4+ (build 4446+) or SQL 2017+
        - MAXDOP in UPDATE STATISTICS: SQL 2016 SP2+ (build 5026+) or SQL 2017 CU3+ (build 3015+)
        */
        @supports_persist_sample bit = 0,
        @supports_maxdop_stats bit = 0,
        /* SQL 2022+ feature detection (includes SQL 2025/v17+) */
        @supports_auto_drop bit = 0;

    /* Set AUTO_DROP support flag (SQL 2022+ / v16+) */
    IF @sql_major_version >= 16
        SET @supports_auto_drop = 1;

    /*
    Phase 1 Environment Detection (v2.0)
    Trace flags affecting statistics behavior - detected once at startup.
    */
    DECLARE
        @trace_flags table
        (
            TraceFlag int NOT NULL,
            Status bit NOT NULL,
            Global bit NOT NULL,
            Session bit NOT NULL
        );

    DECLARE
        @tf_2371_active bit = 0,    /* Dynamic thresholds (built-in for SQL 2016+ CL 130+) */
        @tf_2389_active bit = 0,    /* Ascending key detection (legacy) */
        @tf_2390_active bit = 0,    /* Descending key detection (legacy) */
        @tf_4139_active bit = 0,    /* Universal histogram amendment (modern) */
        @tf_9481_active bit = 0,    /* Force legacy CE */
        @tf_warnings nvarchar(max) = N'';

    /* Query active trace flags */
    INSERT INTO @trace_flags (TraceFlag, Status, Global, Session)
    EXEC (N'DBCC TRACESTATUS(-1) WITH NO_INFOMSGS');

    /* Check for statistics-relevant trace flags */
    SELECT
        @tf_2371_active = MAX(CASE WHEN TraceFlag = 2371 AND Status = 1 THEN 1 ELSE 0 END),
        @tf_2389_active = MAX(CASE WHEN TraceFlag = 2389 AND Status = 1 THEN 1 ELSE 0 END),
        @tf_2390_active = MAX(CASE WHEN TraceFlag = 2390 AND Status = 1 THEN 1 ELSE 0 END),
        @tf_4139_active = MAX(CASE WHEN TraceFlag = 4139 AND Status = 1 THEN 1 ELSE 0 END),
        @tf_9481_active = MAX(CASE WHEN TraceFlag = 9481 AND Status = 1 THEN 1 ELSE 0 END)
    FROM @trace_flags;

    /* Build trace flag warnings for debug output */
    IF @tf_2371_active = 1 AND @sql_major_version >= 13
        SET @tf_warnings = @tf_warnings + N'TF 2371 unnecessary (built-in for SQL 2016+ CL 130+); ';
    IF @tf_2389_active = 1 OR @tf_2390_active = 1
        SET @tf_warnings = @tf_warnings + N'TF 2389/2390 are legacy ascending key flags - consider TF 4139 instead; ';
    IF @tf_4139_active = 1
        SET @tf_warnings = @tf_warnings + N'TF 4139 active (histogram amendment for ascending keys); ';
    IF @tf_9481_active = 1
        SET @tf_warnings = @tf_warnings + N'TF 9481 forces legacy CE 70 globally - consider db-scoped config instead; ';

    /*
    Error collection table (show all errors at once)
    */
    DECLARE
        @errors table
        (
            id integer IDENTITY(1, 1) PRIMARY KEY,
            error_message nvarchar(max) NOT NULL,
            error_severity integer NOT NULL
        );

    /*
    Selected databases tables (Ola Hallengren pattern)
    Supports: SYSTEM_DATABASES, USER_DATABASES, ALL_DATABASES,
              AVAILABILITY_GROUP_DATABASES, wildcards (%), exclusions (-)
    */
    DECLARE
        @SelectedDatabases table
        (
            DatabaseItem nvarchar(max),
            DatabaseType char(1),        /* S=System, U=User, NULL=Any */
            AvailabilityGroup bit,       /* 1=AG databases only */
            StartPosition integer,
            Selected bit                 /* 1=Include, 0=Exclude */
        );

    DECLARE
        @tmpDatabases table
        (
            ID integer IDENTITY(1, 1) PRIMARY KEY,
            DatabaseName sysname NOT NULL,
            DatabaseType char(1),        /* S=System, U=User */
            AvailabilityGroup bit,
            Selected bit NOT NULL DEFAULT 0,
            Completed bit NOT NULL DEFAULT 0
        );

    DECLARE
        @CurrentDatabaseID integer = NULL,
        @CurrentDatabaseName sysname = NULL,
        @database_count integer = 0;

    /*
    Current item variables
    */
    DECLARE
        @current_database sysname = NULL,
        @current_schema_name sysname = NULL,
        @current_table_name sysname = NULL,
        @current_stat_name sysname = NULL,
        @current_object_id integer = NULL,
        @current_stats_id integer = NULL,
        @current_no_recompute bit = NULL,
        @current_is_incremental bit = NULL,
        @current_is_memory_optimized bit = NULL,
        @current_is_heap bit = NULL,
        @current_auto_created bit = NULL,
        @current_modification_counter bigint = NULL,
        @current_row_count bigint = NULL,
        @current_days_stale integer = NULL,
        @current_page_count bigint = NULL,
        @current_persisted_sample_percent float = NULL,
        @absolute_sampled_rows bigint = NULL, /*P1c: computed actual sampled rows from @current_row_count * @current_persisted_sample_percent*/
        @p246_pct int = NULL,
        @p246_msg nvarchar(500) = NULL,
        @current_histogram_steps int = NULL,
        @current_partition_number integer = NULL,
        @current_forwarded_records bigint = NULL,
        /* Replication and temporal table awareness */
        @current_is_published bit = NULL,
        @current_is_tracked_by_cdc bit = NULL,
        @current_temporal_type tinyint = NULL,
        /* Filtered statistics metadata */
        @current_has_filter bit = NULL,
        @current_filter_definition nvarchar(max) = NULL,
        @current_unfiltered_rows bigint = NULL,
        @current_filtered_drift_ratio float = NULL,
        /* Query Store priority metadata */
        @current_qs_plan_count integer = NULL,
        @current_qs_total_executions bigint = NULL,
        @current_qs_total_cpu_ms bigint = NULL,
        @current_qs_total_duration_ms bigint = NULL,
        @current_qs_total_logical_reads bigint = NULL,
        @current_qs_total_memory_grant_kb bigint = NULL,
        @current_qs_total_tempdb_pages bigint = NULL,
        @current_qs_total_physical_reads bigint = NULL,
        @current_qs_total_logical_writes bigint = NULL,
        @current_qs_total_wait_time_ms bigint = NULL,
        @current_qs_max_dop smallint = NULL,
        @current_qs_active_feedback_count int = NULL,
        @current_qs_last_execution datetime2(3) = NULL,
        @current_qs_priority_boost bigint = NULL;

    /*
    TOCTOU check variables (verify stat still exists before execution)
    */
    DECLARE
        @toctou_sql nvarchar(500) = N'',
        @toctou_exists int = 0;

    /*
    Command building
    */
    DECLARE
        @current_command nvarchar(max) = N'',
        @current_command_type nvarchar(60) = N'UPDATE_STATISTICS',
        @current_start_time datetime2(7) = NULL,
        @current_end_time datetime2(7) = NULL,
        @current_error_number integer = NULL,
        @current_error_message nvarchar(max) = NULL,
        @current_extended_info xml = NULL,
        @current_commandlog_id integer = NULL;

    /*
    Output message helpers
    */
    DECLARE
        @norecompute_display nvarchar(20) = N'',
        @duration_ms integer = 0,
        @progress_msg nvarchar(500) = N'',
        @persisted_pct_msg integer = 0,
        @iteration_time datetime2(7) = NULL, /*Captured once per loop iteration for consistent timing*/
        @log_error_msg nvarchar(4000) = NULL; /*For TRY/CATCH - truncate at assignment to leave room for prefix*/

    /* P3e (v2.4): ETR (Estimated Time Remaining) tracking variables */
    DECLARE
        @etr_total_ms bigint = 0,          /* Running total ms across all completed stats */
        @etr_completed int = 0,            /* Count of stats completed (successful executions) */
        @etr_suffix nvarchar(50) = N'',    /* ETR display suffix appended to Complete message */
        @etr_avg_ms bigint = 0,            /* Average ms per stat for current run */
        @etr_remaining_count int = 0,      /* Stats remaining in queue */
        @etr_ms bigint = 0,               /* Estimated remaining ms */
        @etr_display nvarchar(20) = N'';   /* Human-readable ETR string (~Xh Ym, ~Xm, ~Xs) */

    /*
    Run identification for tracking completion
    */
    DECLARE
        @run_label nvarchar(100) = CONVERT(nvarchar(128), SERVERPROPERTY(N'ServerName'))
            + N'_' + CONVERT(nvarchar(20), @start_time, 112)
            + N'_' + REPLACE(CONVERT(nvarchar(20), @start_time, 108), N':', N''), /* #429 */
        @stop_reason nvarchar(50) = NULL,
        @commandlog_exists bit = CASE WHEN OBJECT_ID(N'dbo.CommandLog', N'U') IS NOT NULL THEN 1 ELSE 0 END,
        @commandlog_3part nvarchar(300) = QUOTENAME(DB_NAME()) + N'.dbo.CommandLog', /* v2.26: 3-part name for mop-up dynamic SQL after USE <userdb> */
        @mop_up_done bit = 0, /* v2.24: prevents mop-up re-entry after GOTO */
        @in_mop_up bit = 0, /* v2.24: set during mop-up pass for QualifyReason tagging */
        @mop_up_stats_found int = 0, /* v2.24: mop-up candidates discovered */
        @mop_up_stats_processed int = 0, /* v2.24: mop-up stats processed (for summary XML) */
        @mop_lock_result int = NULL, /* v2.24: parallel mop-up app lock result */
        @mop_lock_resource nvarchar(255) = NULL; /* v2.24: parallel mop-up app lock name */

    /*
    Counters
    */
    DECLARE
        @stats_processed integer = 0,
        @stats_succeeded integer = 0,
        @stats_failed integer = 0,
        @stats_toctou integer = 0,
        @stats_skipped integer = 0,
        @consecutive_failures integer = 0,
        @total_pages_processed bigint = 0, /* v2.24: accumulated page count for volume reporting */
        @warnings nvarchar(max) = N''; /* Collected warnings for @WarningsOut OUTPUT */

    /*
    Queue-based parallel processing variables
    */
    DECLARE
        @queue_id integer = NULL,
        @queue_start_time datetime2(7) = NULL,
        @parameters_string nvarchar(max) = N'',
        @claimed_work bit = 0,
        /*
        Currently claimed table (parallel mode only).
        Worker claims one table at a time, processes all its stats, then claims next.
        */
        @claimed_table_database sysname = NULL,
        @claimed_table_schema sysname = NULL,
        @claimed_table_name sysname = NULL,
        @claimed_table_object_id integer = NULL,
        @claimed_table_stats_updated integer = 0,
        @claimed_table_stats_failed integer = 0,
        @claimed_table_stats_skipped integer = 0;

    /*
    Availability Group variables
    */
    DECLARE
        @is_ag_secondary bit = 0,
        @ag_role_desc nvarchar(60) = NULL;

    /*
    AG redo queue monitoring variables (v2.7, #18)
    */
    DECLARE
        @ag_redo_queue_mb decimal(10, 2) = NULL,
        @ag_wait_start datetime2(7) = NULL,
        @ag_is_primary bit = 0,
        @ag_redo_initial_mb bigint = NULL,
        @ag_wait_msg nvarchar(500) = N'',
        @ag_recovered_msg nvarchar(500) = N'';

    /*
    Tempdb pressure monitoring variables (v2.7, #34)
    */
    DECLARE
        @tempdb_free_mb bigint = NULL,
        @tempdb_msg nvarchar(500) = N'';

    /*
    Long-running stats table (for adaptive sampling)
    Stores stats that historically took longer than @LongRunningThresholdMinutes
    */
    DECLARE @long_running_stats TABLE
    (
        database_name sysname NOT NULL,
        schema_name sysname NOT NULL,
        table_name sysname NOT NULL,
        stat_name sysname NOT NULL,
        max_duration_minutes int NOT NULL,
        last_occurrence datetime2(7) NOT NULL,
        occurrence_count int NOT NULL DEFAULT 1,
        PRIMARY KEY NONCLUSTERED (database_name, schema_name, table_name, stat_name)
    );

    /*
    ============================================================================
    TEMP TABLE FOR STATS TO PROCESS
    ============================================================================
    */
    CREATE TABLE
        #stats_to_process
    (
        id integer IDENTITY(1, 1) PRIMARY KEY,
        database_name sysname NOT NULL,
        schema_name sysname NOT NULL,
        table_name sysname NOT NULL,
        stat_name sysname NOT NULL,
        object_id integer NOT NULL,
        stats_id integer NOT NULL,
        no_recompute bit NOT NULL DEFAULT 0,
        is_incremental bit NOT NULL DEFAULT 0,
        is_memory_optimized bit NOT NULL DEFAULT 0,
        is_heap bit NOT NULL DEFAULT 0,
        auto_created bit NOT NULL DEFAULT 0,
        /* Table metadata for replication/CDC/temporal awareness */
        is_published bit NOT NULL DEFAULT 0, /*transactional replication*/
        is_tracked_by_cdc bit NOT NULL DEFAULT 0, /*Change Data Capture*/
        temporal_type tinyint NOT NULL DEFAULT 0, /*0=none, 1=history, 2=system-versioned*/
        modification_counter bigint NOT NULL DEFAULT 0,
        row_count bigint NOT NULL DEFAULT 0,
        days_stale integer NOT NULL DEFAULT 0,
        page_count bigint NOT NULL DEFAULT 0,
        partition_number integer NULL,
        persisted_sample_percent float NULL, /*existing persisted sample (warn if overriding)*/
        histogram_steps int NULL, /*number of histogram steps for diagnostic insight*/
        /* Filtered statistics metadata */
        has_filter bit NOT NULL DEFAULT 0,
        filter_definition nvarchar(max) NULL,
        unfiltered_rows bigint NULL, /*total rows in table, vs rows matching filter*/
        filtered_drift_ratio AS /*computed: unfiltered_rows / NULLIF(row_count, 0) - measures selectivity drift*/
            CASE WHEN row_count > 0 AND unfiltered_rows IS NOT NULL
                 THEN CONVERT(float, unfiltered_rows) / row_count
                 ELSE NULL
            END,
        /* Query Store priority metadata */
        qs_plan_count integer NULL, /*distinct plans referencing this stat*/
        qs_total_executions bigint NULL, /*total executions of plans using stat*/
        qs_total_cpu_ms bigint NULL, /*total CPU time in ms (avg_cpu_time * count_executions / 1000)*/
        qs_total_duration_ms bigint NULL, /*total elapsed time in ms*/
        qs_total_logical_reads bigint NULL, /*total logical I/O reads*/
        qs_total_memory_grant_kb bigint NULL, /*total memory grant KB (avg_query_max_used_memory * 8KB * executions)*/
        qs_total_tempdb_pages bigint NULL, /*total tempdb pages (avg_tempdb_space_used * executions, SQL 2017+)*/
        qs_total_physical_reads bigint NULL, /*total physical I/O reads (avg_num_physical_io_reads * executions, SQL 2017+)*/
        qs_total_logical_writes bigint NULL, /*total logical write pages (avg_logical_io_writes * executions, diagnostic only)*/
        qs_total_wait_time_ms bigint NULL, /*total stats-relevant wait time ms (BufferIO+Memory+SortAndTempDb from sys.query_store_wait_stats, SQL 2017+)*/
        qs_max_dop smallint NULL, /*maximum observed DOP (last_dop, diagnostic only)*/
        qs_active_feedback_count int NULL, /*SQL 2022+: count of active plan feedback entries (CE/grant/DOP)*/
        qs_last_execution datetime2(3) NULL, /*most recent plan execution*/
        qs_priority_boost bigint NOT NULL DEFAULT 0, /*calculated boost for QS stats*/
        priority integer NOT NULL DEFAULT 0,
        processed bit NOT NULL DEFAULT 0
    );

    /*
    Index for efficient "get next unprocessed stat" query pattern:
    WHERE processed = 0 ORDER BY priority
    */
    CREATE NONCLUSTERED INDEX
        IX_stats_to_process_processed_priority
    ON #stats_to_process
        (processed, priority)
    INCLUDE
        (database_name, schema_name, table_name, stat_name);
    /*#endregion 03B-VARIABLES */
    /*#region 03C-PREREQS: Version check, SET options, CommandLog, Queue auto-create, AG, Azure, RCSI */
    /*
    ============================================================================
    CORE REQUIREMENTS CHECKS
    ============================================================================
    */

    /*
    Check SQL Server version (STRING_SPLIT requires 2016+)
    */
    IF @sql_version < 13
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'sp_StatUpdate requires SQL Server 2016 or later (STRING_SPLIT dependency). Current version: ' +
                CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')),
            error_severity = 16;
    END;

    /*
    Set feature availability flags based on SQL Server version/build
    Reference: Microsoft Tiger Toolbox usp_AdaptiveIndexDefrag version detection
    */
    SELECT
        @supports_persist_sample =
            CASE
                WHEN @sql_major_version >= 14 THEN 1                            /* SQL 2017+ */
                WHEN @sql_major_version = 13 AND @sql_build_number >= 4446 THEN 1 /* SQL 2016 SP1 CU4+ */
                ELSE 0
            END,
        @supports_maxdop_stats =
            CASE
                WHEN @sql_major_version >= 15 THEN 1                            /* SQL 2019+ */
                WHEN @sql_major_version = 14 AND @sql_build_number >= 3015 THEN 1 /* SQL 2017 CU3+ (14.0.3015.0, KB4041809) */
                WHEN @sql_major_version = 13 AND @sql_build_number >= 5026 THEN 1 /* SQL 2016 SP2 RTM+ (13.0.5026.0, KB4041809) -- NOT SP1 */
                /* NOTE: SQL 2016 SP1 (13.0.4001) and below do NOT support MAXDOP in UPDATE STATISTICS.
                         The feature was first shipped in SQL 2016 SP2 RTM (build 13.0.5026.0).
                         KB4041809: https://support.microsoft.com/kb/4041809 */
                ELSE 0
            END;

    /*
    Enforce SET options required by UPDATE STATISTICS on indexed views / computed columns.
    Error 1934 fires when these are off (common via JDBC, linked servers, legacy ODBC).
    Proactively SET them here -- values revert automatically when the proc returns.
    ANSI_NULLS is captured at proc CREATE time (preamble), so only ANSI_WARNINGS
    and ARITHABORT need runtime enforcement.
    */
    IF SESSIONPROPERTY(N'ANSI_WARNINGS') <> 1
    BEGIN
        SET ANSI_WARNINGS ON;
        RAISERROR(N'Note: SET ANSI_WARNINGS ON (was OFF, required for UPDATE STATISTICS)', 10, 1) WITH NOWAIT;
    END;

    IF SESSIONPROPERTY(N'ARITHABORT') <> 1
    BEGIN
        SET ARITHABORT ON;
        RAISERROR(N'Note: SET ARITHABORT ON (was OFF, required for UPDATE STATISTICS)', 10, 1) WITH NOWAIT;
    END;

    /*
    Check CommandLog exists if logging enabled
    */
    IF  @LogToTable = N'Y'
    AND NOT EXISTS
        (
            SELECT
                1
            FROM sys.objects AS o
            JOIN sys.schemas AS s
              ON s.schema_id = o.schema_id
            WHERE o.type = N'U'
            AND   s.name = N'dbo'
            AND   o.name = N'CommandLog'
        )
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The table dbo.CommandLog does not exist. Set @LogToTable = N''N'' or create the table from https://ola.hallengren.com/scripts/CommandLog.sql',
            error_severity = 16;
    END;

    /*
    v2.3: Check CommandLog schema compatibility when logging is enabled.
    Non-blocking warning if expected columns are missing.
    Required columns: CommandType, DatabaseName, SchemaName, ObjectName, StatisticName, ExtendedInfo.
    */
    IF  @LogToTable = N'Y'
    AND @commandlog_exists = 1
    BEGIN
        DECLARE @commandlog_missing_cols nvarchar(max) = N'';
        IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.CommandLog') AND name = N'CommandType')
            SET @commandlog_missing_cols += N'CommandType, ';
        IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.CommandLog') AND name = N'DatabaseName')
            SET @commandlog_missing_cols += N'DatabaseName, ';
        IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.CommandLog') AND name = N'SchemaName')
            SET @commandlog_missing_cols += N'SchemaName, ';
        IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.CommandLog') AND name = N'ObjectName')
            SET @commandlog_missing_cols += N'ObjectName, ';
        IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.CommandLog') AND name = N'StatisticsName')
            SET @commandlog_missing_cols += N'StatisticsName, ';
        IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.CommandLog') AND name = N'ExtendedInfo')
            SET @commandlog_missing_cols += N'ExtendedInfo, ';

        IF LEN(@commandlog_missing_cols) > 0
        BEGIN
            SET @commandlog_missing_cols = LEFT(@commandlog_missing_cols, LEN(@commandlog_missing_cols) - 2); /* trim trailing ", " */
            DECLARE @commandlog_warn nvarchar(1000) =
                N'WARNING: dbo.CommandLog schema may be incompatible. Expected columns missing: '
                + @commandlog_missing_cols + N'. Logging may fail.';
            RAISERROR(@commandlog_warn, 10, 1) WITH NOWAIT;
        END;

        /*
        v2.5: CommandLog index advisory (#31).
        Large CommandLog tables (5M+ rows, common on busy instances running multiple maintenance
        jobs) cause full scans on @i_cleanup_orphaned_runs, @LongRunningThresholdMinutes, and progress
        queries. A nonclustered index leading on CommandType eliminates these scans.
        */
        IF NOT EXISTS
           (
               SELECT 1
               FROM sys.indexes AS i
               JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
               JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
               WHERE i.object_id = OBJECT_ID(N'dbo.CommandLog')
               AND   i.type IN (1, 2) /* clustered or nonclustered */
               AND   c.name = N'CommandType'
               AND   ic.key_ordinal = 1
           )
        BEGIN
            /* #150: Build advisory DDL dynamically based on columns that actually exist in this schema */
            DECLARE
                @cmdlog_object_id int = OBJECT_ID(N'dbo.CommandLog'),
                @advisory_include nvarchar(500) = N'';

            SELECT @advisory_include = @advisory_include +
                CASE WHEN @advisory_include = N'' THEN N'' ELSE N', ' END + c.name
            FROM (VALUES
                (N'DatabaseName'), (N'SchemaName'), (N'ObjectName'),
                (N'StatisticsName'), (N'EndTime'), (N'ErrorNumber')
            ) AS cols(name)
            JOIN sys.columns AS c
              ON c.object_id = @cmdlog_object_id
             AND c.name = cols.name COLLATE DATABASE_DEFAULT;

            RAISERROR(N'ADVISORY: dbo.CommandLog has no index leading on CommandType. On large tables this causes full scans during orphan cleanup and adaptive sampling. Consider:', 10, 1) WITH NOWAIT;

            IF @advisory_include <> N''
                RAISERROR(N'  CREATE NONCLUSTERED INDEX IX_CommandLog_CommandType_StartTime ON dbo.CommandLog (CommandType, StartTime) INCLUDE (%s);', 10, 1, @advisory_include) WITH NOWAIT;
            ELSE
                RAISERROR(N'  CREATE NONCLUSTERED INDEX IX_CommandLog_CommandType_StartTime ON dbo.CommandLog (CommandType, StartTime);', 10, 1) WITH NOWAIT;
        END;
    END;

    /*
    Check Queue tables exist if parallel processing enabled
    */
    IF  @StatsInParallel = N'Y'
    AND NOT EXISTS
        (
            SELECT
                1
            FROM sys.objects AS o
            JOIN sys.schemas AS s
              ON s.schema_id = o.schema_id
            WHERE o.type = N'U'
            AND   s.name = N'dbo'
            AND   o.name = N'Queue'
        )
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The table dbo.Queue does not exist. Download https://ola.hallengren.com/scripts/Queue.sql',
            error_severity = 16;
    END;

    /*
    Auto-create QueueStatistic table if it doesn't exist (requires dbo.Queue)
    */
    IF  @StatsInParallel = N'Y'
    AND NOT EXISTS
        (
            SELECT
                1
            FROM sys.objects AS o
            JOIN sys.schemas AS s
              ON s.schema_id = o.schema_id
            WHERE o.type = N'U'
            AND   s.name = N'dbo'
            AND   o.name = N'QueueStatistic'
        )
    AND EXISTS
        (
            SELECT
                1
            FROM sys.objects AS o
            JOIN sys.schemas AS s
              ON s.schema_id = o.schema_id
            WHERE o.type = N'U'
            AND   s.name = N'dbo'
            AND   o.name = N'Queue'
        )
    BEGIN
        RAISERROR(N'Creating dbo.QueueStatistic table for parallel processing...', 10, 1) WITH NOWAIT;

        CREATE TABLE dbo.QueueStatistic
        (
            QueueID integer NOT NULL,
            DatabaseName sysname NOT NULL,
            SchemaName sysname NOT NULL,
            ObjectName sysname NOT NULL,
            ObjectID integer NOT NULL,
            TablePriority integer NOT NULL DEFAULT 0,
            StatsCount integer NOT NULL DEFAULT 1,
            MaxModificationCounter bigint NOT NULL DEFAULT 0,
            TableStartTime datetime2(7) NULL,
            TableEndTime datetime2(7) NULL,
            SessionID smallint NULL,
            RequestID integer NULL,
            RequestStartTime datetime NULL,
            StatsUpdated integer NULL,
            StatsFailed integer NULL,
            StatsSkipped integer NULL,
            LastStatCompletedAt datetime2(3) NULL, /* v2.3: timestamp of last individual stat completion per worker */
            ClaimLoginTime datetime NULL, /* bd -h9a: login_time of claiming session; pairs with SessionID to survive SPID reuse */
            CONSTRAINT PK_QueueStatistic
                PRIMARY KEY CLUSTERED (QueueID, DatabaseName, SchemaName, ObjectName)
        );

        ALTER TABLE dbo.QueueStatistic
            ADD CONSTRAINT FK_QueueStatistic_Queue
            FOREIGN KEY (QueueID) REFERENCES dbo.Queue (QueueID);

        CREATE NONCLUSTERED INDEX IX_QueueStatistic_Unclaimed
            ON dbo.QueueStatistic (QueueID, TablePriority)
            INCLUDE (DatabaseName, SchemaName, ObjectName, ObjectID, TableStartTime, TableEndTime)
            WHERE TableStartTime IS NULL;

        /* #174: Disable lock escalation to prevent READPAST hint from losing effectiveness */
        ALTER TABLE dbo.QueueStatistic SET (LOCK_ESCALATION = DISABLE);

        RAISERROR(N'Created dbo.QueueStatistic table and indexes.', 10, 1) WITH NOWAIT;
    END;

    /*
    v2.3: Backward-compatible migration -- add LastStatCompletedAt if it doesn't exist.
    Existing installations may not have this column from older versions.
    */
    IF  @StatsInParallel = N'Y'
    AND OBJECT_ID(N'dbo.QueueStatistic', N'U') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.QueueStatistic') AND name = N'LastStatCompletedAt')
    BEGIN
        ALTER TABLE dbo.QueueStatistic ADD LastStatCompletedAt datetime2(3) NULL;
        IF @Debug = 1
            RAISERROR(N'Added LastStatCompletedAt column to dbo.QueueStatistic (v2.3 migration).', 10, 1) WITH NOWAIT;
    END;

    /*
    bd -h9a: Backward-compatible migration -- add ClaimLoginTime if it doesn't exist.
    Paired with SessionID to disambiguate SPID reuse after failover or restart.
    */
    IF  @StatsInParallel = N'Y'
    AND OBJECT_ID(N'dbo.QueueStatistic', N'U') IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.QueueStatistic') AND name = N'ClaimLoginTime')
    BEGIN
        ALTER TABLE dbo.QueueStatistic ADD ClaimLoginTime datetime NULL;
        IF @Debug = 1
            RAISERROR(N'Added ClaimLoginTime column to dbo.QueueStatistic (bd -h9a migration).', 10, 1) WITH NOWAIT;
    END;
    /*
    #142: Server-level AG readable secondary detection.
    If this server is an AG secondary for ALL databases, UPDATE STATISTICS will fail for everything.
    Check sys.dm_hadr_availability_replica_states early to prevent an error storm.
    */
    DECLARE @is_ag_secondary_server bit = 0;
    IF EXISTS (SELECT 1 FROM sys.all_objects WHERE name = N'dm_hadr_availability_replica_states' AND schema_id = SCHEMA_ID(N'sys'))
    BEGIN
        DECLARE @local_primary_count int = 0, @local_secondary_count int = 0;
        SELECT
            @local_primary_count = SUM(CASE WHEN ars.role_desc = N'PRIMARY' THEN 1 ELSE 0 END),
            @local_secondary_count = SUM(CASE WHEN ars.role_desc = N'SECONDARY' THEN 1 ELSE 0 END)
        FROM sys.dm_hadr_availability_replica_states AS ars
        WHERE ars.is_local = 1;

        IF @local_secondary_count > 0 AND @local_primary_count = 0
        BEGIN
            SET @is_ag_secondary_server = 1;
            RAISERROR(N'WARNING: This server is an AG readable secondary for all availability groups. UPDATE STATISTICS will fail for AG databases.', 10, 1) WITH NOWAIT;
            SET @WarningsOut = ISNULL(@WarningsOut, N'') + N'AG_SECONDARY_SERVER: All local AGs are secondary replicas; ';
        END;
    END;

    /* #144: Azure elastic pool DTU/vCore check.
       On Azure SQL DB (EngineEdition=5), check if running in an elastic pool
       where stats maintenance may consume shared pool resources. */
    IF CONVERT(int, SERVERPROPERTY(N'EngineEdition')) = 5
    BEGIN
        BEGIN TRY
            DECLARE @elastic_pool_name nvarchar(128);
            EXEC sys.sp_executesql
                N'SELECT @pool = elastic_pool_name FROM sys.database_service_objectives',
                N'@pool nvarchar(128) OUTPUT',
                @pool = @elastic_pool_name OUTPUT;
            IF @elastic_pool_name IS NOT NULL
            BEGIN
                RAISERROR(N'WARNING: Running in Azure elastic pool [%s]. Statistics maintenance consumes', 10, 1, @elastic_pool_name) WITH NOWAIT;
                RAISERROR(N'  shared pool DTU/vCore -- other databases in the pool may be impacted.', 10, 1) WITH NOWAIT;
                SET @warnings += N'ELASTIC_POOL: ' + @elastic_pool_name + N'; ';
            END;
        END TRY
        BEGIN CATCH
            /* sys.database_service_objectives may not exist on all Azure tiers */
        END CATCH;
    END;

    /* #191: RCSI version store pressure warning.
       Databases using READ_COMMITTED_SNAPSHOT_ISOLATION keep row versions in tempdb.
       FULLSCAN statistics updates can increase version store usage significantly. */
    DECLARE @rcsi_db_count int = 0;
    SELECT @rcsi_db_count = COUNT(*)
    FROM sys.databases AS d
    WHERE d.is_read_committed_snapshot_on = 1
    AND   d.state = 0 /* ONLINE */
    AND   d.database_id > 4; /* user databases only */

    IF @rcsi_db_count > 0
    BEGIN
        DECLARE @version_store_mb bigint = 0;
        SELECT @version_store_mb = SUM(version_store_reserved_page_count) * 8 / 1024
        FROM tempdb.sys.dm_db_file_space_usage;

        IF @version_store_mb > 500 /* >500 MB version store already in use */
        BEGIN
            RAISERROR(N'WARNING: %d RCSI database(s) active, version store already %I64d MB.', 10, 1,
                @rcsi_db_count, @version_store_mb) WITH NOWAIT;
            RAISERROR(N'  FULLSCAN statistics updates may spike version store usage in tempdb.', 10, 1) WITH NOWAIT;
            SET @warnings += N'RCSI_VERSION_STORE: ' + CONVERT(nvarchar(20), @version_store_mb) + N' MB in use; ';
        END;
    END;

    /*#endregion 03C-PREREQS */
    /*#region 03D-PRESETS: Internal @i_ variables, preset application, explicit param overrides */
    /*
    ============================================================================
    PRESET SYSTEM: Internal variables set by presets, overridden by explicit params
    ============================================================================
    */
    DECLARE
        @i_tiered_thresholds bit = 1,
        @i_threshold_logic nvarchar(3) = N'OR',
        @i_modification_threshold bigint = 5000,
        @i_qs_enabled bit = 0,
        @i_qs_metric nvarchar(20) = N'CPU',
        @i_qs_min_executions bigint = 100,
        @i_qs_recent_hours int = 168,
        @i_qs_top_plans int = 500,
        @i_mop_up_pass nvarchar(1) = N'N',
        @i_mop_up_min_remaining int = 60,
        @i_time_limit int = 18000,
        @i_sort_order nvarchar(50) = N'MODIFICATION_COUNTER',
        @i_statistics_sample int = NULL,
        @i_delay_between_stats int = NULL,
        @i_lock_timeout int = NULL,
        @i_max_consecutive_failures int = 10,
        @i_max_ag_redo_queue_mb int = NULL,
        @i_max_ag_wait_minutes int = 10,
        @i_ascending_key_boost nvarchar(1) = N'Y',
        @i_persist_sample_percent nvarchar(1) = N'Y',
        @i_persist_sample_min_rows bigint = 1000000,
        @i_include_system_objects nvarchar(1) = N'N',
        @i_include_indexed_views nvarchar(1) = N'N',
        @i_skip_tables_with_columnstore nchar(1) = N'N',
        @i_filtered_stats_mode nvarchar(10) = N'INCLUDE',
        @i_filtered_stats_stale_factor float = 2.0,
        @i_long_running_threshold_min int = NULL,
        @i_long_running_sample_pct int = 10,
        @i_max_grant_percent int = 10,
        @i_progress_log_interval int = 50,
        @i_log_skipped nvarchar(1) = N'N',
        @i_orphaned_run_threshold_hours int = 48,
        @i_command_log_retention_days int = 90,
        @i_max_seconds_per_stat int = NULL,
        @i_update_incremental bit = 1,
        @i_dead_worker_timeout_min int = 15,
        @i_max_workers int = NULL,
        @i_min_tempdb_free_mb bigint = NULL,
        @i_modification_percent float = NULL,
        @i_min_page_count bigint = NULL,
        @i_cleanup_orphaned_runs nvarchar(1) = N'Y';

    /* Apply preset */
    IF @Preset IS NULL SET @Preset = N'DEFAULT';

    IF @Preset = N'DEFAULT'
    BEGIN
        /* All defaults above are for DEFAULT -- no changes needed */
        SET @i_time_limit = 18000;
    END
    ELSE IF @Preset = N'NIGHTLY'
    BEGIN
        SET @i_time_limit = 3600;
        SET @i_qs_enabled = 1;
        SET @i_mop_up_pass = N'Y';
        SET @i_sort_order = N'QUERY_STORE';
    END
    ELSE IF @Preset = N'WEEKLY_FULL'
    BEGIN
        SET @i_time_limit = 14400;
        SET @i_modification_threshold = 2000;
        SET @i_qs_enabled = 1;
        SET @i_mop_up_pass = N'Y';
        SET @i_sort_order = N'QUERY_STORE';
        SET @i_statistics_sample = 100;
    END
    ELSE IF @Preset = N'OLTP_LIGHT'
    BEGIN
        SET @i_time_limit = 1800;
        SET @i_modification_threshold = 10000;
        SET @i_delay_between_stats = 2;
        SET @i_lock_timeout = 10;
        SET @i_sort_order = N'MODIFICATION_COUNTER';
    END
    ELSE IF @Preset = N'WAREHOUSE'
    BEGIN
        SET @i_time_limit = NULL;
        SET @i_modification_threshold = 1000;
        SET @i_statistics_sample = 100;
        SET @i_sort_order = N'ROWS';
    END;

    /* Explicit params override preset values */
    IF @TimeLimit IS NOT NULL SET @i_time_limit = @TimeLimit;
    IF @SortOrder IS NOT NULL SET @i_sort_order = @SortOrder;
    IF @StatisticsSample IS NOT NULL SET @i_statistics_sample = @StatisticsSample;
    IF @MopUpPass IS NOT NULL SET @i_mop_up_pass = @MopUpPass;
    IF @QueryStore IS NOT NULL
    BEGIN
        IF @QueryStore = N'OFF'
            SET @i_qs_enabled = 0;
        ELSE
        BEGIN
            SET @i_qs_enabled = 1;
            SET @i_qs_metric = @QueryStore;
        END;
    END;
    /* #381: @ModificationThreshold feeds @i_modification_threshold.
       All downstream code (sp_executesql, CHECKSUM, parameters_string) must use @i_modification_threshold,
       not the raw @ModificationThreshold, so the effective preset+override value is always used. */
    IF @ModificationThreshold IS NOT NULL SET @i_modification_threshold = @ModificationThreshold;
    /* #402: Must check the PUBLIC param (was checking @i_ which is always NULL here) */
    IF @LongRunningThresholdMinutes IS NOT NULL SET @i_long_running_threshold_min = @LongRunningThresholdMinutes;
    IF @LongRunningSamplePercent IS NOT NULL SET @i_long_running_sample_pct = @LongRunningSamplePercent;

    /* #387: @QueryStoreTopPlans override -- controls Phase 6 XML plan parsing limit */
    IF @QueryStoreTopPlans IS NOT NULL SET @i_qs_top_plans = @QueryStoreTopPlans;

    /* #394: @QueryStoreMinExecutions / @QueryStoreRecentHours overrides */
    IF @QueryStoreMinExecutions IS NOT NULL SET @i_qs_min_executions = @QueryStoreMinExecutions;
    IF @QueryStoreRecentHours IS NOT NULL SET @i_qs_recent_hours = @QueryStoreRecentHours;

    /* #388: Safety params -- override internal defaults when caller provides explicit values */
    /* Note: @i_max_ag_redo_queue_mb defaults to NULL (disabled); caller opts in with a value */
    IF @MaxAGRedoQueueMB IS NOT NULL SET @i_max_ag_redo_queue_mb = @MaxAGRedoQueueMB;
    IF @MaxAGWaitMinutes IS NOT NULL SET @i_max_ag_wait_minutes = @MaxAGWaitMinutes;
    IF @MinTempdbFreeMB IS NOT NULL SET @i_min_tempdb_free_mb = @MinTempdbFreeMB;
    IF @MopUpMinRemainingSeconds IS NOT NULL SET @i_mop_up_min_remaining = @MopUpMinRemainingSeconds;

    /*#endregion 03D-PRESETS */
    /*#region 03E-VALIDATION: Y/N checks, enum validation, cross-param checks, error aggregation */
    /*
    ============================================================================
    TABLE-DRIVEN VALIDATION
    ============================================================================
    */

    /* Normalize @Tables = 'ALL' to NULL */
    IF UPPER(LTRIM(RTRIM(@Tables))) = N'ALL'
        SET @Tables = NULL;

    /* Normalize @Databases='ALL' to 'ALL_DATABASES' */
    IF UPPER(LTRIM(RTRIM(@Databases))) = N'ALL'
    BEGIN
        SET @Databases = N'ALL_DATABASES';
        IF @Debug = 1
            RAISERROR(N'Note: @Databases=''ALL'' normalized to ''ALL_DATABASES'' (excludes system DBs).', 10, 1) WITH NOWAIT;
    END;

    /* Normalize empty strings to NULL */
    IF @Statistics IS NOT NULL AND LEN(LTRIM(RTRIM(@Statistics))) = 0
    BEGIN
        SET @Statistics = NULL;
        SET @WarningsOut = ISNULL(@WarningsOut, N'') + N'EMPTY_STRING_PARAM: @Statistics='''' treated as NULL; ';
    END;
    IF @Tables IS NOT NULL AND LEN(LTRIM(RTRIM(@Tables))) = 0
        SET @Tables = NULL;
    IF @ExcludeStatistics IS NOT NULL AND LEN(LTRIM(RTRIM(@ExcludeStatistics))) = 0
        SET @ExcludeStatistics = NULL;
    IF @ExcludeTables IS NOT NULL AND LEN(LTRIM(RTRIM(@ExcludeTables))) = 0
        SET @ExcludeTables = NULL;

    /* Y/N parameter validation */
    DECLARE @yn_checks TABLE (param_name nvarchar(50), param_value nvarchar(10));
    INSERT INTO @yn_checks VALUES
        (N'@Execute', @Execute),
        (N'@LogToTable', @LogToTable),
        (N'@StatsInParallel', @StatsInParallel),
        (N'@MopUpPass (internal)', @i_mop_up_pass);

    INSERT INTO @errors (error_message, error_severity)
    SELECT N'The value for ' + c.param_name + N' is not supported.  Use Y or N.', 16
    FROM @yn_checks AS c
    WHERE c.param_value NOT IN (N'Y', N'N');

    /* Enum validation */
    IF @TargetNorecompute NOT IN (N'Y', N'N', N'BOTH')
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'@TargetNorecompute must be Y, N, or BOTH.', 16);

    IF @Preset NOT IN (N'DEFAULT', N'NIGHTLY', N'WEEKLY_FULL', N'OLTP_LIGHT', N'WAREHOUSE')
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'Invalid @Preset. Use: DEFAULT, NIGHTLY, WEEKLY_FULL, OLTP_LIGHT, WAREHOUSE.', 16);

    IF @i_sort_order NOT IN (N'MODIFICATION_COUNTER', N'DAYS_STALE', N'PAGE_COUNT', N'RANDOM',
                             N'QUERY_STORE', N'FILTERED_DRIFT', N'AUTO_CREATED', N'ROWS')
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'Invalid @SortOrder. Use: MODIFICATION_COUNTER, DAYS_STALE, PAGE_COUNT, RANDOM, QUERY_STORE, FILTERED_DRIFT, AUTO_CREATED, ROWS.', 16);

    IF @i_qs_enabled = 1
    AND @i_qs_metric NOT IN (N'CPU', N'DURATION', N'READS', N'EXECUTIONS', N'AVG_CPU',
                             N'MEMORY_GRANT', N'TEMPDB_SPILLS', N'PHYSICAL_READS', N'AVG_MEMORY', N'WAITS')
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'Invalid @QueryStore metric. Use: CPU, DURATION, READS, EXECUTIONS, AVG_CPU, MEMORY_GRANT, TEMPDB_SPILLS, PHYSICAL_READS, AVG_MEMORY, WAITS, OFF.', 16);

    /* Range validation */
    IF @i_statistics_sample IS NOT NULL AND (@i_statistics_sample < 1 OR @i_statistics_sample > 100)
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'@StatisticsSample must be 1-100 (100=FULLSCAN) or NULL.', 16);

    IF @MaxDOP IS NOT NULL AND (@MaxDOP < 0 OR @MaxDOP > 64)
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'@MaxDOP must be 0-64 or NULL.', 16);

    IF @i_time_limit IS NOT NULL AND @i_time_limit < 0
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'@TimeLimit cannot be negative.', 16);

    IF @BatchLimit IS NOT NULL AND @BatchLimit < 1
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'@BatchLimit must be >= 1.', 16);

    IF @LongRunningThresholdMinutes IS NOT NULL AND @LongRunningThresholdMinutes < 1
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'@LongRunningThresholdMinutes must be >= 1.', 16);

    IF @LongRunningSamplePercent IS NOT NULL AND (@LongRunningSamplePercent < 1 OR @LongRunningSamplePercent > 100)
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'@LongRunningSamplePercent must be 1-100.', 16);

    IF @i_max_grant_percent IS NOT NULL AND (@i_max_grant_percent < 1 OR @i_max_grant_percent > 100)
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'Max grant percent (internal config) must be 1-100 or NULL.', 16);

    IF @i_lock_timeout IS NOT NULL AND @i_lock_timeout < -1
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'Lock timeout (internal config) must be -1 (infinite) or >= 0 seconds.', 16);

    IF @i_max_consecutive_failures IS NOT NULL AND @i_max_consecutive_failures < 1
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'Max consecutive failures (internal config) must be >= 1.', 16);

    IF @i_delay_between_stats IS NOT NULL AND @i_delay_between_stats < 0
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'Delay between stats (internal config) must be >= 0.', 16);

    /* Cross-param validation */
    IF @WhatIfOutputTable IS NOT NULL AND @Execute = N'Y'
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'@WhatIfOutputTable requires @Execute = N.', 16);

    /* gh-455: @StopByTime supplies a time budget too -- converted to @i_time_limit
       further down in this region.  Accept either. */
    IF @i_mop_up_pass = N'Y' AND @i_time_limit IS NULL AND @StopByTime IS NULL
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'@MopUpPass requires a time budget (@TimeLimit or @StopByTime).', 16);

    IF @i_qs_metric = N'TEMPDB_SPILLS' AND @sql_major_version < 14
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'@QueryStore = TEMPDB_SPILLS requires SQL 2017+.', 16);

    IF @i_qs_metric = N'PHYSICAL_READS' AND @sql_major_version < 14
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'@QueryStore = PHYSICAL_READS requires SQL 2017+.', 16);

    IF @i_qs_metric = N'WAITS' AND @sql_major_version < 14
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'@QueryStore = WAITS requires SQL 2017+.', 16);

    IF @i_mop_up_pass = N'Y' AND @LogToTable = N'N'
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'@MopUpPass requires @LogToTable = Y (uses CommandLog to skip recent updates).', 16);

    /* gh-455: accept @StopByTime as time-budget source. */
    IF @i_mop_up_pass = N'Y' AND @StatsInParallel = N'Y'
    AND @i_time_limit IS NULL AND @StopByTime IS NULL
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'@MopUpPass with @StatsInParallel requires @TimeLimit or @StopByTime.', 16);

    IF @LongRunningThresholdMinutes IS NOT NULL AND @LogToTable = N'N'
        INSERT INTO @errors (error_message, error_severity)
        VALUES (N'@LongRunningThresholdMinutes requires @LogToTable = Y (needs CommandLog history).', 16);

    /* @StopByTime format validation + processing */
    IF @StopByTime IS NOT NULL
    BEGIN
        /* Normalize compact formats: HHMM -> HH:MM, HHMMSS -> HH:MM:SS */
        IF LEN(@StopByTime) = 4 AND @StopByTime LIKE N'[0-9][0-9][0-9][0-9]'
            SET @StopByTime = LEFT(@StopByTime, 2) + N':' + RIGHT(@StopByTime, 2);
        IF LEN(@StopByTime) = 6 AND @StopByTime LIKE N'[0-9][0-9][0-9][0-9][0-9][0-9]'
            SET @StopByTime = LEFT(@StopByTime, 2) + N':' + SUBSTRING(@StopByTime, 3, 2) + N':' + RIGHT(@StopByTime, 2);

        /*
        Semantic range validation after normalization.
        Accepted formats: HH:MM or HH:MM:SS (24-hour clock).
        Pattern [0-2][0-9] allows hours 00-29; hours >= 24 are rejected below.
        Minutes and seconds are bounded by [0-5][0-9] (00-59).
        */
        IF  @StopByTime NOT LIKE N'[0-2][0-9]:[0-5][0-9]'
        AND @StopByTime NOT LIKE N'[0-2][0-9]:[0-5][0-9]:[0-5][0-9]'
        BEGIN
            INSERT INTO @errors (error_message, error_severity)
            VALUES (N'@StopByTime format invalid.  Use HH:MM or HH:MM:SS (24-hour clock, e.g. ''05:00'' or ''2300'').', 16);
        END
        ELSE
        BEGIN
            /* [0-2][0-9] in the LIKE allows hours 20-29; reject anything >= 24 explicitly */
            DECLARE @stopby_hours integer = CONVERT(integer, LEFT(@StopByTime, 2));
            IF @stopby_hours >= 24
            BEGIN
                INSERT INTO @errors (error_message, error_severity)
                VALUES (N'@StopByTime hours must be 00-23.  Received: ' + LEFT(@StopByTime, 2), 16);
            END
            ELSE
            BEGIN
                BEGIN TRY
                    DECLARE
                        @stopby_time time(0) = CONVERT(time(0), @StopByTime),
                        @now_time time(0) = CONVERT(time(0), SYSDATETIME()),
                        @stopby_seconds_remaining int;

                    SET @stopby_seconds_remaining = DATEDIFF(SECOND, @now_time, @stopby_time);
                    IF @stopby_seconds_remaining < 0
                        SET @stopby_seconds_remaining = @stopby_seconds_remaining + 86400;
                    SET @i_time_limit = @stopby_seconds_remaining;
                END TRY
                BEGIN CATCH
                    INSERT INTO @errors (error_message, error_severity)
                    VALUES (N'@StopByTime value is invalid: ' + ERROR_MESSAGE(), 16);
                END CATCH;
            END;
        END;
    END;

    /*
    @WhatIfOutputTable: PARSENAME + QUOTENAME validation (SQL injection prevention)
    */
    IF @WhatIfOutputTable IS NOT NULL AND @Execute = N'N'
    BEGIN
        DECLARE
            @safe_wio_schema sysname,
            @safe_wio_table  sysname,
            @safe_wio_fqn    nvarchar(512);

        SET @safe_wio_table  = PARSENAME(@WhatIfOutputTable, 1);
        SET @safe_wio_schema = ISNULL(PARSENAME(@WhatIfOutputTable, 2), N'dbo');

        IF @safe_wio_table IS NULL
        BEGIN
            INSERT INTO @errors (error_message, error_severity)
            VALUES (N'@WhatIfOutputTable is not a valid table name.  Use schema.table or table format.', 16);
        END
        ELSE
        BEGIN
            SET @safe_wio_fqn = QUOTENAME(@safe_wio_schema) + N'.' + QUOTENAME(@safe_wio_table);

            DECLARE @whatif_create_sql nvarchar(max) = N'
                IF OBJECT_ID(N''' + REPLACE(@safe_wio_fqn, N'''', N'''''') + N''', N''U'') IS NULL
                BEGIN
                    CREATE TABLE ' + @safe_wio_fqn + N' (
                        SequenceNum int IDENTITY(1,1) PRIMARY KEY,
                        DatabaseName sysname NOT NULL,
                        SchemaName sysname NOT NULL,
                        TableName sysname NOT NULL,
                        StatName sysname NOT NULL,
                        Command nvarchar(max) NOT NULL,
                        ModificationCounter bigint NULL,
                        DaysStale int NULL,
                        PageCount bigint NULL
                    );
                END;';

            BEGIN TRY
                EXECUTE (@whatif_create_sql);
            END TRY
            BEGIN CATCH
                INSERT INTO @errors (error_message, error_severity)
                VALUES (N'Failed to create @WhatIfOutputTable: ' + ERROR_MESSAGE(), 16);
            END CATCH;
        END;
    END;

    /*
    Raise all validation errors at once
    */
    IF EXISTS
       (
           SELECT
               1
           FROM @errors
       )
    BEGIN
        /*
        Aggregate all error messages using FOR XML (SQL 2016 compatible).
        STRING_AGG would be cleaner but requires SQL 2017+.
        */
        DECLARE @all_errors nvarchar(max);

        SELECT @all_errors = STUFF((
            SELECT CHAR(13) + CHAR(10) + e.error_message
            FROM @errors AS e
            ORDER BY e.id
            FOR XML PATH(''), TYPE
        ).value('.', 'nvarchar(max)'), 1, 2, '');

        /*
        DECODE XML ENTITIES (P1 #27, v1.9 enhancement)
        FOR XML PATH encodes special characters. Decode them for readable output.
        Order matters: decode &amp; last since it affects other entities.
        v1.9: Added &quot; and &apos; for complete XML entity coverage.
        */
        SELECT @all_errors = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@all_errors,
            N'&lt;', N'<'),
            N'&gt;', N'>'),
            N'&quot;', N'"'),
            N'&apos;', N''''),
            N'&amp;', N'&');

        RAISERROR(@all_errors, 16, 1) WITH NOWAIT;
        RAISERROR(N'', 10, 1) WITH NOWAIT;
        RAISERROR(N'Documentation: https://ola.hallengren.com/sql-server-index-and-statistics-maintenance.html', 10, 1) WITH NOWAIT;

        /* #200: Set @StopReasonOut on parameter validation failure */
        SET @StopReasonOut = N'PARAMETER_ERROR';
        SET @StatsFoundOut = 0;
        SET @StatsProcessedOut = 0;
        SET @StatsSucceededOut = 0;
        SET @StatsFailedOut = 0;
        SET @StatsRemainingOut = 0;
        SET @DurationSecondsOut = DATEDIFF(SECOND, @start_time, SYSDATETIME());
        SELECT
            Status = N'ERROR', StatusMessage = N'Parameter validation failed.',
            StatsFound = 0, StatsProcessed = 0, StatsSucceeded = 0, StatsFailed = 0,
            StatsToctou = 0, StatsSkipped = 0, StatsRemaining = 0,
            DatabasesProcessed = 0,
            DurationSeconds = DATEDIFF(SECOND, @start_time, SYSDATETIME()),
            StopReason = N'PARAMETER_ERROR', RunLabel = @run_label,
            Version = @procedure_version;

        IF @StatsInParallel = N'N'
            DELETE FROM dbo.StatUpdateLock WHERE Resource = N'sp_StatUpdate' AND SessionID = @@SPID;
        RETURN 50000;
    END;
    /*#endregion 03E-VALIDATION */
    /*#endregion 03-SETUP */
    /*#region 04-DB-PARSE: Database parsing, AG secondary check */
    /*
    ============================================================================
    PARSE @Databases - Ola Hallengren pattern
    Supports: SYSTEM_DATABASES, USER_DATABASES, ALL_DATABASES,
              AVAILABILITY_GROUP_DATABASES, wildcards (%), exclusions (-)
    ============================================================================
    */
    IF @Databases IS NOT NULL
    BEGIN
        /*
        Clean input: remove newlines and trim
        */
        SELECT @Databases = LTRIM(RTRIM(REPLACE(REPLACE(@Databases, CHAR(10), N''), CHAR(13), N'')));

        /*
        Parse comma-separated items into @SelectedDatabases
        Uses recursive CTE (Ola Hallengren pattern) to preserve order.
        STRING_SPLIT order is undefined in SQL 2016-2019; this approach is deterministic.
        */
        ;WITH DatabaseSplitter AS
        (
            /*
            Anchor: first item (before first comma or entire string if no comma)
            */
            SELECT
                DatabaseItem = LTRIM(RTRIM(
                    CASE
                        WHEN CHARINDEX(N',', @Databases) > 0
                        THEN SUBSTRING(@Databases, 1, CHARINDEX(N',', @Databases) - 1)
                        ELSE @Databases
                    END
                )),
                Remainder =
                    CASE
                        WHEN CHARINDEX(N',', @Databases) > 0
                        THEN SUBSTRING(@Databases, CHARINDEX(N',', @Databases) + 1, LEN(@Databases))
                        ELSE N''
                    END,
                StartPosition = 1

            UNION ALL

            /*
            Recursive: next item from remainder
            */
            SELECT
                DatabaseItem = LTRIM(RTRIM(
                    CASE
                        WHEN CHARINDEX(N',', Remainder) > 0
                        THEN SUBSTRING(Remainder, 1, CHARINDEX(N',', Remainder) - 1)
                        ELSE Remainder
                    END
                )),
                Remainder =
                    CASE
                        WHEN CHARINDEX(N',', Remainder) > 0
                        THEN SUBSTRING(Remainder, CHARINDEX(N',', Remainder) + 1, LEN(Remainder))
                        ELSE N''
                    END,
                StartPosition = StartPosition + 1
            FROM DatabaseSplitter
            WHERE LEN(Remainder) > 0
        ),
        Databases2 AS
        (
            SELECT
                DatabaseItem =
                    CASE
                        WHEN DatabaseItem LIKE N'-%'
                        THEN LTRIM(STUFF(DatabaseItem, 1, 1, N''))
                        ELSE DatabaseItem
                    END,
                StartPosition,
                Selected =
                    CASE
                        WHEN DatabaseItem LIKE N'-%'
                        THEN CONVERT(bit, 0)
                        ELSE CONVERT(bit, 1)
                    END
            FROM DatabaseSplitter
            WHERE DatabaseItem <> N''
        ),
        Databases3 AS
        (
            SELECT
                DatabaseItem =
                    CASE
                        WHEN DatabaseItem IN (N'ALL_DATABASES', N'SYSTEM_DATABASES',
                                              N'USER_DATABASES', N'AVAILABILITY_GROUP_DATABASES')
                        THEN N'%'
                        ELSE DatabaseItem
                    END,
                DatabaseType =
                    CASE
                        WHEN DatabaseItem = N'SYSTEM_DATABASES' THEN 'S'
                        WHEN DatabaseItem = N'USER_DATABASES' THEN 'U'
                        /*
                        ALL_DATABASES excludes system DBs by default.
                        Use SYSTEM_DATABASES explicitly if you need master/msdb/model.
                        */
                        WHEN DatabaseItem = N'ALL_DATABASES' THEN 'U'
                        ELSE NULL
                    END,
                AvailabilityGroup =
                    CASE
                        WHEN DatabaseItem = N'AVAILABILITY_GROUP_DATABASES'
                        THEN CONVERT(bit, 1)
                        ELSE NULL
                    END,
                StartPosition,
                Selected
            FROM Databases2
        )
        INSERT INTO @SelectedDatabases
            (DatabaseItem, DatabaseType, AvailabilityGroup, StartPosition, Selected)
        SELECT
            DatabaseItem,
            DatabaseType,
            AvailabilityGroup,
            StartPosition,
            Selected
        FROM Databases3
        OPTION (MAXRECURSION 0); /* Unlimited - supports instances with 1000+ databases */
    END;

    /*
    Populate @tmpDatabases with all accessible databases
    */
    INSERT INTO @tmpDatabases
        (DatabaseName, DatabaseType, AvailabilityGroup, Selected, Completed)
    SELECT
        DatabaseName = d.name,
        DatabaseType =
            CASE
                WHEN d.name IN (N'master', N'msdb', N'model')
                  OR d.is_distributor = 1
                THEN 'S'
                ELSE 'U'
            END,
        AvailabilityGroup =
            CASE
                WHEN EXISTS
                (
                    SELECT 1
                    FROM sys.dm_hadr_database_replica_states AS drs
                    WHERE drs.database_id = d.database_id
                    AND   drs.is_local = 1
                )
                THEN CONVERT(bit, 1)
                ELSE CONVERT(bit, 0)
            END,
        Selected = 0,
        Completed = 0
    FROM sys.databases AS d
    WHERE d.name <> N'tempdb'
    AND   d.source_database_id IS NULL  /* Exclude database snapshots */
    AND   d.state = 0                   /* ONLINE only */
    AND   d.is_in_standby = 0           /* bd -47y: exclude log-shipping STANDBY databases */
    AND   HAS_DBACCESS(d.name) = 1;     /* Skip SINGLE_USER held by other sessions */

    /*
    Apply selections (inclusion pass first)
    */
    UPDATE td
    SET td.Selected = sd.Selected
    FROM @tmpDatabases AS td
    INNER JOIN @SelectedDatabases AS sd
        ON td.DatabaseName LIKE REPLACE(sd.DatabaseItem, N'_', N'[_]') COLLATE DATABASE_DEFAULT
        AND (td.DatabaseType = sd.DatabaseType OR sd.DatabaseType IS NULL)
        AND (td.AvailabilityGroup = sd.AvailabilityGroup OR sd.AvailabilityGroup IS NULL)
    WHERE sd.Selected = 1;

    /*
    Apply exclusions (must come after inclusions)
    */
    UPDATE td
    SET td.Selected = sd.Selected
    FROM @tmpDatabases AS td
    INNER JOIN @SelectedDatabases AS sd
        ON td.DatabaseName LIKE REPLACE(sd.DatabaseItem, N'_', N'[_]') COLLATE DATABASE_DEFAULT
        AND (td.DatabaseType = sd.DatabaseType OR sd.DatabaseType IS NULL)
        AND (td.AvailabilityGroup = sd.AvailabilityGroup OR sd.AvailabilityGroup IS NULL)
    WHERE sd.Selected = 0;

    /*
    Default to current database if @Databases is NULL
    */
    IF @Databases IS NULL
    BEGIN
        UPDATE @tmpDatabases
        SET Selected = 1
        WHERE DatabaseName = DB_NAME();
    END;

    /*
    Get count for display
    */
    SELECT @database_count = COUNT(*)
    FROM @tmpDatabases
    WHERE Selected = 1;

    /*
    Validate at least one database matched
    */
    IF @database_count = 0
    BEGIN
        RAISERROR(N'No databases matched the @Databases pattern: %s', 16, 1, @Databases) WITH NOWAIT;
        SET @StopReasonOut = N'PARAMETER_ERROR';
        SET @StatsFoundOut = 0;
        SET @StatsProcessedOut = 0;
        SET @StatsSucceededOut = 0;
        SET @StatsFailedOut = 0;
        SET @StatsRemainingOut = 0;
        SET @DurationSecondsOut = DATEDIFF(SECOND, @start_time, SYSDATETIME());
        SELECT
            Status = N'ERROR', StatusMessage = N'No databases matched @Databases pattern.',
            StatsFound = 0, StatsProcessed = 0, StatsSucceeded = 0, StatsFailed = 0,
            StatsToctou = 0, StatsSkipped = 0, StatsRemaining = 0,
            DatabasesProcessed = 0,
            DurationSeconds = DATEDIFF(SECOND, @start_time, SYSDATETIME()),
            StopReason = N'PARAMETER_ERROR', RunLabel = @run_label,
            Version = @procedure_version;
        IF @StatsInParallel = N'N'
            DELETE FROM dbo.StatUpdateLock WHERE Resource = N'sp_StatUpdate' AND SessionID = @@SPID;
        RETURN 1;
    END;

    /*
    ============================================================================
    AVAILABILITY GROUP CHECK - Exclude secondary replicas (FAIL FAST)
    ============================================================================
    Check AG status immediately after database selection, before parameter
    validation. If all selected databases are on AG secondaries, fail fast
    instead of wasting cycles validating parameters we'll never use.
    */
    DECLARE
        @ag_secondary_count integer = 0,
        @ag_secondary_list nvarchar(max) = N'';

    DECLARE @ag_secondary_cache TABLE (DatabaseName sysname PRIMARY KEY);

    INSERT INTO @ag_secondary_cache (DatabaseName)
    SELECT DISTINCT
        d.name
    FROM sys.dm_hadr_availability_replica_states AS ars
    JOIN sys.dm_hadr_database_replica_states AS drs
        ON drs.replica_id = ars.replica_id
    JOIN sys.databases AS d
        ON d.database_id = drs.database_id
    WHERE ars.is_local = 1
    AND   ars.role_desc = N'SECONDARY';

    /*
    Build display list and exclude AG secondaries from selection
    */
    SELECT
        @ag_secondary_list = @ag_secondary_list + td.DatabaseName + N', '
    FROM @tmpDatabases AS td
    WHERE td.Selected = 1
    AND   EXISTS (SELECT 1 FROM @ag_secondary_cache AS c WHERE c.DatabaseName = td.DatabaseName);

    UPDATE td
    SET td.Selected = 0
    FROM @tmpDatabases AS td
    WHERE td.Selected = 1
    AND   EXISTS (SELECT 1 FROM @ag_secondary_cache AS c WHERE c.DatabaseName = td.DatabaseName);

    SELECT @ag_secondary_count = ROWCOUNT_BIG();

    /*
    Update database count after AG exclusion and validate
    */
    SELECT @database_count = COUNT(*)
    FROM @tmpDatabases
    WHERE Selected = 1;

    IF @database_count = 0
    AND @ag_secondary_count > 0
    BEGIN
        RAISERROR(N'All selected databases are on AG secondary replicas. Statistics cannot be updated on readable secondaries.', 16, 1) WITH NOWAIT;
        SET @StopReasonOut = N'AG_SECONDARY';
        SET @StatsFoundOut = 0;
        SET @StatsProcessedOut = 0;
        SET @StatsSucceededOut = 0;
        SET @StatsFailedOut = 0;
        SET @StatsRemainingOut = 0;
        SET @DurationSecondsOut = DATEDIFF(SECOND, @start_time, SYSDATETIME());
        SELECT
            Status = N'ERROR',
            StatusMessage = N'All selected databases are on AG secondary replicas.',
            StatsFound = 0, StatsProcessed = 0, StatsSucceeded = 0, StatsFailed = 0,
            StatsToctou = 0, StatsSkipped = 0, StatsRemaining = 0,
            DatabasesProcessed = 0,
            DurationSeconds = DATEDIFF(SECOND, @start_time, SYSDATETIME()),
            StopReason = N'AG_SECONDARY',
            RunLabel = @run_label,
            Version = @procedure_version;
        IF @StatsInParallel = N'N'
            DELETE FROM dbo.StatUpdateLock WHERE Resource = N'sp_StatUpdate' AND SessionID = @@SPID;
        RETURN 1;
    END;

    /* #161: Warn when @Databases matched zero databases (no AG secondaries -- likely misspelling) */
    IF  @database_count = 0
    AND @ag_secondary_count = 0
    AND @Databases IS NOT NULL
    BEGIN
        RAISERROR(N'WARNING: @Databases=''%s'' matched 0 databases. Check spelling. Valid keywords: ALL_DATABASES, USER_DATABASES, SYSTEM_DATABASES, AVAILABILITY_GROUP_DATABASES. Wildcards: %%', 10, 1, @Databases) WITH NOWAIT;
        SET @WarningsOut = ISNULL(@WarningsOut, N'') +
            N'NO_MATCHING_DATABASES: @Databases matched 0 databases; ';
    END;
    /*#endregion 04-DB-PARSE */
    /*#region 05-HEADER: Header output (server info, params, debug) */
    /*
    ============================================================================
    HEADER OUTPUT
    ============================================================================
    */
    DECLARE
        @server_name nvarchar(128) = CONVERT(nvarchar(128), SERVERPROPERTY(N'ServerName')),
        @product_version nvarchar(128) = CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')),
        @edition nvarchar(128) = CONVERT(nvarchar(128), SERVERPROPERTY(N'Edition')),
        @engine_edition int = CONVERT(int, SERVERPROPERTY(N'EngineEdition')),
        /* EngineEdition: 3=Enterprise, 5=Azure SQL DB, 8=Azure SQL MI, 9=Azure SQL Edge */
        @is_azure_sql bit = CASE WHEN CONVERT(int, SERVERPROPERTY(N'EngineEdition')) IN (5, 8, 9) THEN 1 ELSE 0 END,
        @Tables_display nvarchar(max) = ISNULL(@Tables, N'ALL'),
        @TimeLimit_display nvarchar(20) = ISNULL(CONVERT(nvarchar(20), @i_time_limit), N'None'),
        @BatchLimit_display nvarchar(20) = ISNULL(CONVERT(nvarchar(20), @BatchLimit), N'None'),
        @LockTimeout_display nvarchar(20) = ISNULL(CONVERT(nvarchar(20), @i_lock_timeout), N'None'),
        @MaxConsecutiveFailures_display nvarchar(20) = ISNULL(CONVERT(nvarchar(20), @i_max_consecutive_failures), N'None'),
        @start_time_display nvarchar(30) = CONVERT(nvarchar(30), @start_time, 121);

    RAISERROR(N'', 10, 1) WITH NOWAIT;
    RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
    RAISERROR(N' sp_StatUpdate - Priority-Based Statistics Maintenance', 10, 1) WITH NOWAIT;
    RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
    RAISERROR(N'', 10, 1) WITH NOWAIT;
    RAISERROR(N'Server:      %s', 10, 1, @server_name) WITH NOWAIT;
    RAISERROR(N'Version:     %s', 10, 1, @product_version) WITH NOWAIT;
    RAISERROR(N'Edition:     %s', 10, 1, @edition) WITH NOWAIT;

    /*
    Azure SQL Platform Warning
    DTU/vCore consumption warning for Azure SQL environments.
    */
    IF @is_azure_sql = 1
    BEGIN
        /*
        Azure platform distinction (#39): EngineEdition 5 = Azure SQL DB, 8 = Azure SQL MI, 9 = Azure SQL Edge.
        MI supports incremental stats, Resource Governor, and most on-prem features. SQL DB does not.
        */
        DECLARE @azure_platform nvarchar(50) = CASE @engine_edition
            WHEN 5 THEN N'Azure SQL Database'
            WHEN 8 THEN N'Azure SQL Managed Instance'
            WHEN 9 THEN N'Azure SQL Edge'
            ELSE N'Azure SQL (unknown edition)'
        END;
        RAISERROR(N'Platform:    %s (EngineEdition=%d)', 10, 1, @azure_platform, @engine_edition) WITH NOWAIT;

        IF @engine_edition = 5
        BEGIN
            RAISERROR(N'  Note: Azure SQL DB -- DTU/vCore impact, no Resource Governor, no incremental stats.', 10, 1) WITH NOWAIT;
            RAISERROR(N'  Consider running during off-peak hours or scaling up temporarily.', 10, 1) WITH NOWAIT;
            SET @warnings += N'AZURE_SQL_DB: DTU/vCore impact, limited feature set; ';
        END
        ELSE IF @engine_edition = 8
        BEGIN
            RAISERROR(N'  Note: Azure SQL MI -- most on-prem features supported (incremental stats, RG, etc.).', 10, 1) WITH NOWAIT;
            RAISERROR(N'  vCore consumption still applies during maintenance windows.', 10, 1) WITH NOWAIT;
            SET @warnings += N'AZURE_SQL_MI: vCore impact during maintenance; ';
        END;
    END;

    /*
    Trace flag status (Phase 1.3 - v2.0)
    Show statistics-relevant trace flags if any are active.
    */
    IF @tf_2371_active = 1 OR @tf_2389_active = 1 OR @tf_2390_active = 1 OR @tf_4139_active = 1 OR @tf_9481_active = 1
    BEGIN
        DECLARE @tf_list nvarchar(100) = N'';
        IF @tf_2371_active = 1 SET @tf_list = @tf_list + N'2371,';
        IF @tf_2389_active = 1 SET @tf_list = @tf_list + N'2389,';
        IF @tf_2390_active = 1 SET @tf_list = @tf_list + N'2390,';
        IF @tf_4139_active = 1 SET @tf_list = @tf_list + N'4139,';
        IF @tf_9481_active = 1 SET @tf_list = @tf_list + N'9481,';
        SET @tf_list = LEFT(@tf_list, LEN(@tf_list) - 1); /* Remove trailing comma */
        RAISERROR(N'TraceFlags:  %s (stats-relevant)', 10, 1, @tf_list) WITH NOWAIT;
    END;

    /*
    SQL 2022 AUTO_DROP note (Phase 1.2 - v2.0)
    */
    IF @supports_auto_drop = 1
        RAISERROR(N'AUTO_DROP:   Available (SQL 2022+) - stats auto-drop on schema change by default', 10, 1) WITH NOWAIT;

    /* #173: Linux case-sensitive collation warning */
    DECLARE @server_collation nvarchar(128) = CONVERT(nvarchar(128), SERVERPROPERTY(N'Collation'));
    IF @server_collation LIKE N'%_CS_%'
    BEGIN
        RAISERROR(N'Collation:   %s (CASE-SENSITIVE)', 10, 1, @server_collation) WITH NOWAIT;
        RAISERROR(N'  Note: Case-sensitive collation -- object name matching in @Tables, @ExcludeTables,', 10, 1) WITH NOWAIT;
        RAISERROR(N'  @Statistics, @ExcludeStatistics uses COLLATE DATABASE_DEFAULT. Ensure filter', 10, 1) WITH NOWAIT;
        RAISERROR(N'  values match the exact case of database object names.', 10, 1) WITH NOWAIT;
        SET @warnings += N'CASE_SENSITIVE_COLLATION: ' + @server_collation + N'; ';
    END;

    RAISERROR(N'Procedure:   %s', 10, 1, @procedure_version) WITH NOWAIT;
    RAISERROR(N'Start time:  %s', 10, 1, @start_time_display) WITH NOWAIT;

    /* P3d (v2.4): Show execute mode in banner -- always visible including dry runs */
    DECLARE @execute_mode_display nvarchar(60) = CASE @Execute WHEN N'N' THEN N'DRY RUN -- no updates will be executed' ELSE N'EXECUTE' END;
    RAISERROR(N'Mode:        %s', 10, 1, @execute_mode_display) WITH NOWAIT;

    /*
    Show database count and list if Debug mode
    */
    DECLARE @Databases_display nvarchar(max) = ISNULL(@Databases, N'(current: ' + DB_NAME() + N')');
    RAISERROR(N'Databases:   %d selected', 10, 1, @database_count) WITH NOWAIT;

    IF @ag_secondary_count > 0
    BEGIN
        RAISERROR(N'AG Warning:  %d database(s) excluded (secondary replicas)', 10, 1, @ag_secondary_count) WITH NOWAIT;
    END;

    RAISERROR(N'', 10, 1) WITH NOWAIT;
    RAISERROR(N'Parameters:', 10, 1) WITH NOWAIT;
    RAISERROR(N'  @Databases               = %s', 10, 1, @Databases_display) WITH NOWAIT;
    RAISERROR(N'  @Tables                  = %s', 10, 1, @Tables_display) WITH NOWAIT;
    RAISERROR(N'  @TargetNorecompute       = %s', 10, 1, @TargetNorecompute) WITH NOWAIT;
    RAISERROR(N'  @ModificationThreshold   = %I64d', 10, 1, @i_modification_threshold) WITH NOWAIT;
    IF @i_modification_percent IS NOT NULL
    BEGIN
        DECLARE @ModPctDisplay nvarchar(20) = CONVERT(nvarchar(20), @i_modification_percent);
        RAISERROR(N'  ModificationPercent       = %s', 10, 1, @ModPctDisplay) WITH NOWAIT;
    END;

    /*
    Bit parameters must be cast to integer for RAISERROR display
    */
    DECLARE
        @TieredThresholds_int integer = @i_tiered_thresholds,
        @UpdateIncremental_int integer = @i_update_incremental,
        @FailFast_int integer = @FailFast,
        @Debug_int integer = @Debug;

    IF @Preset IS NOT NULL
        RAISERROR(N'  @Preset                  = %s', 10, 1, @Preset) WITH NOWAIT;
    RAISERROR(N'  TieredThresholds          = %d', 10, 1, @TieredThresholds_int) WITH NOWAIT;
    RAISERROR(N'  ThresholdLogic            = %s', 10, 1, @i_threshold_logic) WITH NOWAIT;
    IF @StaleHours IS NOT NULL
    BEGIN
        RAISERROR(N'  @StaleHours      = %d', 10, 1, @StaleHours) WITH NOWAIT;
    END;
    RAISERROR(N'  MinPageCount              = %I64d', 10, 1, @i_min_page_count) WITH NOWAIT;
    RAISERROR(N'  IncludeSystemObjects      = %s', 10, 1, @i_include_system_objects) WITH NOWAIT;
    IF @i_statistics_sample IS NOT NULL
    BEGIN
        RAISERROR(N'  @StatisticsSample         = %d%%', 10, 1, @i_statistics_sample) WITH NOWAIT;
    END;
    RAISERROR(N'  UpdateIncremental         = %d', 10, 1, @UpdateIncremental_int) WITH NOWAIT;
    RAISERROR(N'  @TimeLimit                = %s seconds', 10, 1, @TimeLimit_display) WITH NOWAIT;
    RAISERROR(N'  @BatchLimit              = %s stats', 10, 1, @BatchLimit_display) WITH NOWAIT;
    RAISERROR(N'  MaxConsecutiveFailures    = %s', 10, 1, @MaxConsecutiveFailures_display) WITH NOWAIT;
    IF @i_mop_up_pass = N'Y'
    BEGIN
        RAISERROR(N'  @MopUpPass                = Y (broad sweep with remaining time)', 10, 1) WITH NOWAIT;
        RAISERROR(N'  @MopUpMinRemainingSeconds = %d', 10, 1, @i_mop_up_min_remaining) WITH NOWAIT;
    END;
    IF @i_max_ag_redo_queue_mb IS NOT NULL
    BEGIN
        RAISERROR(N'  @MaxAGRedoQueueMB         = %d MB', 10, 1, @i_max_ag_redo_queue_mb) WITH NOWAIT;
        RAISERROR(N'  @MaxAGWaitMinutes         = %d', 10, 1, @i_max_ag_wait_minutes) WITH NOWAIT;
    END;
    IF @i_min_tempdb_free_mb IS NOT NULL
    BEGIN
        DECLARE @MinTempdbFreeMB_display nvarchar(20) = CONVERT(nvarchar(20), @i_min_tempdb_free_mb);
        RAISERROR(N'  @MinTempdbFreeMB          = %s MB', 10, 1, @MinTempdbFreeMB_display) WITH NOWAIT;
    END;
    RAISERROR(N'  LockTimeout               = %s seconds', 10, 1, @LockTimeout_display) WITH NOWAIT;
    RAISERROR(N'  @SortOrder                = %s', 10, 1, @i_sort_order) WITH NOWAIT;
    RAISERROR(N'  @StatsInParallel         = %s', 10, 1, @StatsInParallel) WITH NOWAIT;
    RAISERROR(N'  @Execute                 = %s', 10, 1, @Execute) WITH NOWAIT;
    RAISERROR(N'  @FailFast                = %d', 10, 1, @FailFast_int) WITH NOWAIT;
    IF @ExcludeTables IS NOT NULL
        RAISERROR(N'  @ExcludeTables           = %s', 10, 1, @ExcludeTables) WITH NOWAIT;
    IF @ExcludeStatistics IS NOT NULL
        RAISERROR(N'  @ExcludeStatistics       = %s', 10, 1, @ExcludeStatistics) WITH NOWAIT;
    IF @LongRunningThresholdMinutes IS NOT NULL
    BEGIN
        RAISERROR(N'  @LongRunningThreshold    = %d minutes', 10, 1, @i_long_running_threshold_min) WITH NOWAIT;
        RAISERROR(N'  @LongRunningSamplePct    = %d%%', 10, 1, @i_long_running_sample_pct) WITH NOWAIT;
    END;

    /*
    Query Store parameters (when enabled)
    */
    IF @i_qs_enabled = 1
    BEGIN
        DECLARE @i_qs_enabled_int int = CONVERT(int, @i_qs_enabled);
        RAISERROR(N'  QueryStore                = %d', 10, 1, @i_qs_enabled_int) WITH NOWAIT;
        RAISERROR(N'  QueryStoreMetric          = %s', 10, 1, @i_qs_metric) WITH NOWAIT;
        RAISERROR(N'  @QueryStoreMinExecutions  = %I64d', 10, 1, @i_qs_min_executions) WITH NOWAIT;
        RAISERROR(N'  @QueryStoreRecentHours    = %d', 10, 1, @i_qs_recent_hours) WITH NOWAIT;
        IF @i_qs_top_plans IS NOT NULL
            RAISERROR(N'  @QueryStoreTopPlans       = %d', 10, 1, @i_qs_top_plans) WITH NOWAIT;
        ELSE
            RAISERROR(N'  @QueryStoreTopPlans       = NULL (unlimited)', 10, 1) WITH NOWAIT;

        /* gh-458: RANDOM sort discards qs_priority_boost -- QS enrichment is
           wasted work.  Warn so the user knows. */
        IF @i_sort_order = N'RANDOM'
        BEGIN
            RAISERROR(N'  WARNING: @SortOrder = RANDOM ignores qs_priority_boost -- QS enrichment has no effect on ordering.', 10, 1) WITH NOWAIT;
        END;

        /* #189: Pre-run QS READ_ONLY warning -- alert DBA before discovery, not after */
        DECLARE @qs_readonly_count int = 0;
        SELECT @qs_readonly_count = COUNT(*)
        FROM @tmpDatabases AS td
        WHERE td.Selected = 1
        AND EXISTS (
            SELECT 1
            FROM sys.databases AS d
            WHERE d.name = td.DatabaseName COLLATE DATABASE_DEFAULT
            AND   d.is_query_store_on = 1
        );
        /* Note: actual_state check requires per-database context (done in Phase 6).
           This is a lighter pre-check using sys.databases.is_query_store_on. */
        IF @qs_readonly_count > 0
            RAISERROR(N'  Note: QS state per-database checked during Phase 6. READ_ONLY databases may yield stale priority data.', 10, 1) WITH NOWAIT;
    END;

    /*
    Additional parameters (when non-default or relevant)
    */
    IF @i_filtered_stats_mode <> N'INCLUDE'
        RAISERROR(N'  FilteredStatsMode         = %s', 10, 1, @i_filtered_stats_mode) WITH NOWAIT;
    IF @MaxDOP IS NOT NULL
        RAISERROR(N'  @MaxDOP                  = %d', 10, 1, @MaxDOP) WITH NOWAIT;
    IF @i_delay_between_stats IS NOT NULL
        RAISERROR(N'  DelayBetweenStats         = %d seconds', 10, 1, @i_delay_between_stats) WITH NOWAIT;
    RAISERROR(N'  @LogToTable              = %s', 10, 1, @LogToTable) WITH NOWAIT;
    RAISERROR(N'  CleanupOrphanedRuns       = %s', 10, 1, @i_cleanup_orphaned_runs) WITH NOWAIT;
    RAISERROR(N'  @Debug                   = %d', 10, 1, @Debug_int) WITH NOWAIT;
    IF @StatsInParallel = N'Y' AND @i_dead_worker_timeout_min IS NOT NULL
        RAISERROR(N'  @DeadWorkerTimeout       = %d minutes', 10, 1, @i_dead_worker_timeout_min) WITH NOWAIT;

    /*
    Threshold Interaction Explanation (debug mode only -- gh-469)
    Helps users understand how the threshold parameters work together.
    */
    IF @Debug_int = 1
    BEGIN
        DECLARE @mod_pct_str nvarchar(20) = CONVERT(nvarchar(20), ISNULL(@i_modification_percent, 0));

        RAISERROR(N'', 10, 1) WITH NOWAIT;
        RAISERROR(N'Threshold logic explanation:', 10, 1) WITH NOWAIT;
        IF @i_tiered_thresholds = 1
        BEGIN
            RAISERROR(N'  TieredThresholds=1: Stats qualify if mod_count >= tier threshold OR sqrt threshold', 10, 1) WITH NOWAIT;
            RAISERROR(N'    Tier thresholds: 0-500 rows=500 mods, 501-10K=20%%, 10K-100K=15%%, 100K-1M=10%%, 1M+=5%%', 10, 1) WITH NOWAIT;
            RAISERROR(N'    @ModificationThreshold=%I64d acts as floor (only affects large tables)', 10, 1, @i_modification_threshold) WITH NOWAIT;
        END;
        ELSE IF @i_modification_percent IS NOT NULL
        BEGIN
            RAISERROR(N'  TieredThresholds=0: Using SQRT formula: mod_count >= %s * SQRT(row_count)', 10, 1, @mod_pct_str) WITH NOWAIT;
        END;
        ELSE
        BEGIN
            RAISERROR(N'  TieredThresholds=0: Using fixed threshold: mod_count >= %I64d', 10, 1, @i_modification_threshold) WITH NOWAIT;
        END;

        IF @StaleHours IS NOT NULL
        BEGIN
            RAISERROR(N'  OR days since update >= %d', 10, 1, @StaleHours) WITH NOWAIT;
        END;
    END;

    /*
    Hardware context and warning collection (always runs for @WarningsOut)
    RAISERROR output gated behind @Debug; warning collection always executes.
    */
    DECLARE
        @hw_cpu_count int,
        @hw_memory_mb bigint,
        @hw_numa_nodes int,
        @hw_uptime_hours int,
        /* P1d fix (v2.4): process-level memory and visible schedulers for container awareness */
        @process_memory_kb bigint,     /* SQL Server process memory (container limit, not host RAM) */
        @visible_schedulers int,       /* Online schedulers visible to SQL (may differ from cpu_count in containers) */
        /* #208: container memory flag -- set always, not just in debug mode */
        @is_container_memory bit = 0;  /* 1 = SQL process memory differs >20% from host physical_memory_kb */

    SELECT
        @hw_cpu_count = cpu_count,
        @hw_memory_mb = physical_memory_kb / 1024,
        @hw_numa_nodes = numa_node_count,
        @hw_uptime_hours = DATEDIFF(HOUR, sqlserver_start_time, SYSDATETIME()) /* P1d: SQL Server uptime, not OS uptime */
    FROM sys.dm_os_sys_info;

    /* P1d: Process memory from sys.dm_os_process_memory (reflects container memory limit) */
    SELECT @process_memory_kb = physical_memory_in_use_kb
    FROM sys.dm_os_process_memory;

    /* P1d: Visible online schedulers (may be less than cpu_count in container deployments) */
    SELECT @visible_schedulers = COUNT(*)
    FROM sys.dm_os_schedulers
    WHERE status = N'VISIBLE ONLINE';

    IF @hw_uptime_hours < 24
        SET @warnings += N'LOW_UPTIME: SQL Server restarted < 24h ago; ';

    DECLARE @active_backups int = 0;
    SELECT @active_backups = COUNT(*)
    FROM sys.dm_exec_requests AS r
    WHERE r.command LIKE N'BACKUP%';

    IF @active_backups > 0
        SET @warnings += N'BACKUP_RUNNING: ' + CONVERT(nvarchar(10), @active_backups) + N' backup(s) active; ';

    /* #184: Peak hours plan cache warning -- UPDATE STATISTICS clears plan cache entries
       for affected tables, which can cause recompilations during peak load. */
    DECLARE @current_hour int = DATEPART(HOUR, SYSDATETIME());
    IF @current_hour >= 8 AND @current_hour < 18
    BEGIN
        RAISERROR(N'WARNING: Running during business hours (%d:00). UPDATE STATISTICS clears plan cache', 10, 1, @current_hour) WITH NOWAIT;
        RAISERROR(N'  entries for affected tables -- expect query recompilations. Consider scheduling off-peak.', 10, 1) WITH NOWAIT;
        SET @warnings += N'PEAK_HOURS: running at ' + CONVERT(nvarchar(5), @current_hour) + N':00 (plan cache impact); ';
    END;

    /*
    #208: Container memory detection -- always runs, not gated on @Debug.
    physical_memory_kb in sys.dm_os_sys_info reflects the *host* physical RAM.
    Inside a Linux container with a cgroup memory limit (e.g. 4 GB on a 256 GB host)
    this value is misleading for capacity planning and can trigger incorrect warnings.

    Detection: SQL process memory (dm_os_process_memory.physical_memory_in_use_kb)
    will diverge significantly from host physical_memory_kb when a container limit is active.
    A >20% divergence is used as the threshold (same heuristic as previous debug-only note).

    WARNING is always emitted (not just debug) and added to @WarningsOut so automation
    can detect it programmatically. The diagnostic note in the debug banner is also
    upgraded from "Note" to a labelled WARNING when the flag is set.
    */
    IF @process_memory_kb IS NOT NULL
    AND @hw_memory_mb > 0
    AND ABS(1.0 - CONVERT(float, @process_memory_kb) / CONVERT(float, @hw_memory_mb * 1024)) > 0.20
    BEGIN
        SET @is_container_memory = 1;
        DECLARE @host_ram_gb bigint = @hw_memory_mb / 1024;
        DECLARE @process_rss_mb bigint = @process_memory_kb / 1024;
        DECLARE @container_warn nvarchar(2000) =
            N'WARNING: Container detected -- OOMKill risk. Host RAM: '
            + CONVERT(nvarchar(20), @host_ram_gb) + N' GB, SQL process RSS: '
            + CONVERT(nvarchar(20), @process_rss_mb) + N' MB. '
            + N'Configure max server memory based on container cgroup limit, not host RAM.'
            + CASE WHEN @visible_schedulers <> @hw_cpu_count
                   THEN N' CPU mismatch: ' + CONVERT(nvarchar(10), @visible_schedulers)
                        + N' visible schedulers vs ' + CONVERT(nvarchar(10), @hw_cpu_count)
                        + N' host cores -- verify --cpus container flag.'
                   ELSE N''
              END
            + N' (#208, #247)';
        RAISERROR(@container_warn, 10, 1) WITH NOWAIT;
        SET @warnings += N'CONTAINER_MEMORY: OOMKill risk -- host RAM '
            + CONVERT(nvarchar(20), @host_ram_gb) + N' GB, process RSS '
            + CONVERT(nvarchar(20), @process_rss_mb) + N' MB; configure max server memory from cgroup limit; ';
    END;

    /* #251: Advisory -- suggest index on CommandLog.StartTime when retention window is large */
    IF  @commandlog_exists = 1
    AND @i_command_log_retention_days > 30
    AND NOT EXISTS (
        SELECT 1
        FROM sys.indexes AS i
        INNER JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        INNER JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE i.object_id = OBJECT_ID(N'dbo.CommandLog')
        AND   c.name = N'StartTime'
        AND   ic.key_ordinal = 1
    )
    BEGIN
        RAISERROR(N'INFO: @i_command_log_retention_days=%d -- consider adding an index on dbo.CommandLog(StartTime) for faster history lookups. (#251)', 10, 1, @i_command_log_retention_days) WITH NOWAIT;
    END;

    /*
    Trace flag status and hardware context in debug mode (Phase 1.3 - v2.0)
    Show active statistics-relevant trace flags and recommendations.
    */
    IF @Debug = 1
    BEGIN
        RAISERROR(N'', 10, 1) WITH NOWAIT;
        IF LEN(@tf_warnings) > 0
        BEGIN
            RAISERROR(N'Statistics-Relevant Trace Flags: Active', 10, 1) WITH NOWAIT;
            RAISERROR(N'  %s', 10, 1, @tf_warnings) WITH NOWAIT;
        END
        ELSE
        BEGIN
            RAISERROR(N'Statistics-Relevant Trace Flags: None active', 10, 1) WITH NOWAIT;
        END;

        RAISERROR(N'', 10, 1) WITH NOWAIT;
        RAISERROR(N'Environment (hardware context):', 10, 1) WITH NOWAIT;
        DECLARE @hw_memory_gb bigint = @hw_memory_mb / 1024;
        RAISERROR(N'  CPU cores: %d (visible schedulers: %d), NUMA nodes: %d, Uptime: %d hours (SQL Server)', 10, 1,
            @hw_cpu_count, @visible_schedulers, @hw_numa_nodes, @hw_uptime_hours) WITH NOWAIT;
        DECLARE
            @proc_memory_mb bigint = @process_memory_kb / 1024,
            @proc_memory_gb bigint = @process_memory_kb / 1024 / 1024;
        RAISERROR(N'  Host memory: %I64d MB (%I64d GB) | Process (SQL) memory: %I64d MB', 10, 1,
            @hw_memory_mb, @hw_memory_gb, @proc_memory_mb) WITH NOWAIT;
        /* #208: Container memory -- use flag set above (always-on detection) */
        IF @is_container_memory = 1
            RAISERROR(N'  WARNING: Container memory limit active -- host RAM (%I64d GB) reported by physical_memory_kb. SQL process memory in use: %I64d MB. Capacity planning and memory warnings should use process memory, not host RAM. (#208)', 10, 1, @hw_memory_gb, @proc_memory_mb) WITH NOWAIT;

        IF @hw_uptime_hours < 24
            RAISERROR(N'  Note: SQL Server restarted < 24h ago. Query Store/usage stats may be incomplete.', 10, 1) WITH NOWAIT;

        IF @active_backups > 0
            RAISERROR(N'  Warning: %d backup operation(s) currently running. Statistics updates may compete for I/O.', 10, 1, @active_backups) WITH NOWAIT;
    END;

    /*
    v2.7: AG primary detection (#18).
    Query runs always (not just debug) so @i_max_ag_redo_queue_mb loop check works.
    Also captures initial redo queue for debug banner.
    */
    IF EXISTS
    (
        SELECT 1
        FROM sys.dm_hadr_availability_replica_states AS ars
        WHERE ars.is_local = 1
        AND   ars.role_desc = N'PRIMARY'
    )
    BEGIN
        SET @ag_is_primary = 1;

        SELECT @ag_redo_initial_mb = MAX(drs.redo_queue_size) / 1024.0
        FROM sys.dm_hadr_database_replica_states AS drs
        WHERE drs.is_local = 0
        AND   drs.is_primary_replica = 0;

        IF @ag_redo_initial_mb IS NOT NULL AND @ag_redo_initial_mb > 100
            SET @WarningsOut = ISNULL(@WarningsOut, N'') +
                N'AG_REDO_ELEVATED: Redo queue ' + CONVERT(nvarchar(20), @ag_redo_initial_mb) + N' MB at startup; ';
    END;

    /*
    v2.7: Tempdb free space detection (#34).
    Query runs always for warnings and debug banner.
    */
    SELECT @tempdb_free_mb =
        SUM(unallocated_extent_page_count) * 8 / 1024.0
    FROM tempdb.sys.dm_db_file_space_usage;

    IF @tempdb_free_mb IS NOT NULL AND @tempdb_free_mb < 100
        SET @WarningsOut = ISNULL(@WarningsOut, N'') +
            N'TEMPDB_LOW: Only ' + CONVERT(nvarchar(20), @tempdb_free_mb) + N' MB free at startup; ';

    /*
    v2.5: Resource Governor detection (#35).
    Query runs always (not just debug) so @MaxDOP conflict warning works.
    Display is debug-only; warning is always shown.
    */
    DECLARE
        @rg_workload_group sysname,
        @rg_max_dop int;

    SELECT
        @rg_workload_group = wg.name,
        @rg_max_dop = wg.max_dop
    FROM sys.dm_exec_sessions AS s
    JOIN sys.dm_resource_governor_workload_groups AS wg
      ON wg.group_id = s.group_id
    WHERE s.session_id = @@SPID;

    IF @Debug = 1 AND @rg_workload_group IS NOT NULL
    BEGIN
        IF @rg_max_dop > 0 /* 0 = no RG cap */
            RAISERROR(N'  Resource Governor: workload group [%s], MAX_DOP=%d', 10, 1,
                @rg_workload_group, @rg_max_dop) WITH NOWAIT;
        ELSE
            RAISERROR(N'  Resource Governor: workload group [%s] (no DOP cap)', 10, 1,
                @rg_workload_group) WITH NOWAIT;
    END;

    /* v2.7: AG redo queue and tempdb in debug banner */
    IF @Debug = 1
    BEGIN
        IF @ag_is_primary = 1
        BEGIN
            IF @ag_redo_initial_mb IS NOT NULL
                RAISERROR(N'  AG redo queue:   %I64d MB (max across secondaries at startup)', 10, 1, @ag_redo_initial_mb) WITH NOWAIT;
            ELSE
                RAISERROR(N'  AG primary:      Yes (secondaries offline or no redo data)', 10, 1) WITH NOWAIT;
        END;
        IF @tempdb_free_mb IS NOT NULL
            RAISERROR(N'  Tempdb free:     %I64d MB', 10, 1, @tempdb_free_mb) WITH NOWAIT;
    END;

    /*
    v2.5: Resource Governor @MaxDOP conflict warning (#35).
    Always shown (not just debug) when @MaxDOP > RG MAX_DOP -- a real production footgun.
    */
    IF  @MaxDOP IS NOT NULL
    AND @rg_max_dop IS NOT NULL
    BEGIN
        /* #194: When workload group max_dop=0 (uncapped), fall back to server-level MAXDOP */
        DECLARE @effective_max_dop int = @rg_max_dop;
        IF @effective_max_dop = 0
            SELECT @effective_max_dop = CONVERT(int, value_in_use)
            FROM sys.configurations
            WHERE name = N'max degree of parallelism';

        IF  @effective_max_dop > 0
        AND @MaxDOP > @effective_max_dop
        BEGIN
            DECLARE @rg_warn nvarchar(500) =
                N'WARNING: @MaxDOP=' + CONVERT(nvarchar(10), @MaxDOP) +
                CASE WHEN @rg_max_dop = 0
                    THEN N' but server-level MAXDOP=' + CONVERT(nvarchar(10), @effective_max_dop)
                    ELSE N' but Resource Governor workload group [' + @rg_workload_group +
                         N'] caps MAX_DOP=' + CONVERT(nvarchar(10), @effective_max_dop)
                END +
                N'. @MaxDOP hint will be silently capped by SQL Server.';
            RAISERROR(@rg_warn, 10, 1) WITH NOWAIT;

            SET @WarningsOut = ISNULL(@WarningsOut, N'') +
                N'RG_MAXDOP_CAP: RG caps @MaxDOP from ' + CONVERT(nvarchar(10), @MaxDOP) +
                N' to ' + CONVERT(nvarchar(10), @effective_max_dop) + N'; ';
        END;
    END;

    /*
    One-time note when @i_persist_sample_percent=Y but no sample rate specified
    (Moved from per-stat loop to reduce noise in debug output)
    */
    IF @Debug = 1 AND @i_persist_sample_percent = N'Y' AND @i_statistics_sample IS NULL
    BEGIN
        RAISERROR(N'Note: @i_persist_sample_percent=Y ignored (no @i_statistics_sample specified)', 10, 1) WITH NOWAIT;
    END;

    RAISERROR(N'', 10, 1) WITH NOWAIT;
    /*#endregion 05-HEADER */
    /*#region 06-RUN-INIT: CommandLog START, orphan cleanup, session context */

    /* #164: Set session context so sp_StatUpdate sessions are identifiable in dm_exec_requests */
    BEGIN TRY
        EXEC sys.sp_set_session_context @key = N'sp_StatUpdate_RunLabel', @value = @run_label, @read_only = 0;
    END TRY
    BEGIN CATCH
        /* sp_set_session_context may not be available on all versions/editions -- silently skip */
        IF @Debug = 1
            RAISERROR(N'  Note: sp_set_session_context not available -- session context identifier not set.', 10, 1) WITH NOWAIT;
    END CATCH;

    /*
    ============================================================================
    LOG RUN_HEADER TO COMMANDLOG
    ============================================================================
    */
    IF  @LogToTable = N'Y'
    AND @Execute = N'Y'
    AND @commandlog_exists = 1
    BEGIN
        DECLARE
            @parameters_xml xml =
            (
                SELECT
                    /* Identity */
                    @procedure_version AS [Version],
                    @run_label AS RunLabel,
                    @@SPID AS SessionID,

                    /* Scope & targeting */
                    @Preset AS Preset,
                    @Databases AS [Databases],
                    @database_count AS DatabaseCount,
                    @Tables AS [Tables],
                    @ExcludeTables AS ExcludeTables,
                    @ExcludeStatistics AS ExcludeStatistics,
                    @Statistics AS [Statistics],
                    @TargetNorecompute AS TargetNorecompute,
                    @i_include_system_objects AS IncludeSystemObjects,
                    @i_include_indexed_views AS IncludeIndexedViews,
                    @i_skip_tables_with_columnstore AS SkipTablesWithColumnstore,
                    @i_ascending_key_boost AS AscendingKeyBoost,

                    /* Thresholds */
                    @i_modification_threshold AS ModificationThreshold,
                    @i_modification_percent AS ModificationPercent,
                    @i_tiered_thresholds AS TieredThresholds,
                    @i_threshold_logic AS ThresholdLogic,
                    @StaleHours AS StaleHours,
                    @i_min_page_count AS MinPageCount,

                    /* Filtered stats */
                    @i_filtered_stats_mode AS FilteredStatsMode,
                    @i_filtered_stats_stale_factor AS FilteredStatsStaleFactor,

                    /* Query Store */
                    CASE WHEN @i_qs_enabled = 1 THEN N'Y' ELSE N'N' END AS QueryStorePriority,
                    @i_qs_metric AS QueryStoreMetric,
                    @i_qs_min_executions AS QueryStoreMinExecutions,
                    @i_qs_recent_hours AS QueryStoreRecentHours,
                    @i_qs_top_plans AS QueryStoreTopPlans,

                    /* Update behavior */
                    @i_statistics_sample AS StatisticsSample,
                    @i_persist_sample_percent AS PersistSamplePercent,
                    @i_persist_sample_min_rows AS PersistSampleMinRows,
                    @MaxDOP AS MaxDOP,
                    @i_max_grant_percent AS MaxGrantPercent,
                    @i_update_incremental AS UpdateIncremental,

                    /* Execution control */
                    @i_time_limit AS TimeLimit,
                    @StopByTime AS StopByTime,
                    @i_max_seconds_per_stat AS MaxSecondsPerStat,
                    @BatchLimit AS BatchLimit,
                    @i_sort_order AS SortOrder,
                    @i_delay_between_stats AS DelayBetweenStats,
                    @i_max_consecutive_failures AS MaxConsecutiveFailures,
                    @i_mop_up_pass AS MopUpPass,
                    @i_mop_up_min_remaining AS MopUpMinRemainingSeconds,
                    @FailFast AS FailFast,


                    /* Adaptive sampling */
                    @LongRunningThresholdMinutes AS LongRunningThresholdMinutes,
                    @LongRunningSamplePercent AS LongRunningSamplePercent,

                    /* Environmental safety */
                    @i_max_ag_redo_queue_mb AS MaxAGRedoQueueMB,
                    @i_max_ag_wait_minutes AS MaxAGWaitMinutes,
                    @i_min_tempdb_free_mb AS MinTempdbFreeMB,


                    /* Logging & output */
                    @i_lock_timeout AS LockTimeout,
                    @LogToTable AS LogToTable,
                    @i_log_skipped AS LogSkippedToCommandLog,
                    @i_progress_log_interval AS ProgressLogInterval,

                    /* Parallel */
                    @StatsInParallel AS StatsInParallel,
                    @i_max_workers AS MaxWorkers,
                    @i_dead_worker_timeout_min AS DeadWorkerTimeoutMinutes,

                    /* Cleanup */
                    @i_cleanup_orphaned_runs AS CleanupOrphanedRuns,
                    @i_orphaned_run_threshold_hours AS OrphanedRunThresholdHours,
                    @i_command_log_retention_days AS CommandLogRetentionDays,

                    /* Meta / diagnostic (#242) */
                    @Execute AS [Execute],
                    @Debug AS [Debug],
                    @WhatIfOutputTable AS WhatIfOutputTable
                FOR
                    XML RAW(N'Parameters'),
                    ELEMENTS XSINIL
            );

        /*
        START marker must succeed - abort if it fails
        This ensures SP_STATUPDATE_START is always the first entry for any run
        */
        INSERT INTO
            dbo.CommandLog
        (
            DatabaseName,
            SchemaName,
            ObjectName,
            ObjectType,
            Command,
            CommandType,
            StartTime,
            ExtendedInfo
        )
        VALUES
        (
            ISNULL(@Databases, DB_NAME()),
            N'dbo',
            N'sp_StatUpdate',
            N'P',
            N'EXECUTE dbo.sp_StatUpdate @Databases = N''' + ISNULL(@Databases, DB_NAME()) + N''''
                + CASE WHEN @Tables IS NOT NULL THEN N', @Tables = N''' + @Tables + N'''' ELSE N'' END
                + CASE WHEN @Statistics IS NOT NULL THEN N', @Statistics = N''' + @Statistics + N'''' ELSE N'' END
                + N', @TimeLimit = ' + CONVERT(nvarchar(10), @i_time_limit)
                + N', @SortOrder = N''' + @i_sort_order + N''''
                + CASE WHEN @i_qs_enabled = 1 THEN N', @QueryStore = N''' + @i_qs_metric + N'''' ELSE N'' END
                + N';',
            N'SP_STATUPDATE_START',
            @start_time,
            @parameters_xml
        );

        RAISERROR(N'Run: %s (logged to CommandLog)', 10, 1, @run_label) WITH NOWAIT;
        RAISERROR(N'', 10, 1) WITH NOWAIT;
    END;


    /*
    ============================================================================
    CLEANUP ORPHANED RUNS (runs that started but never ended - killed jobs)
    ============================================================================
    When @i_cleanup_orphaned_runs = 'Y', find SP_STATUPDATE_START entries without
    matching SP_STATUPDATE_END and insert END markers with StopReason='KILLED'.
    This helps with run analysis and prevents orphaned entries from accumulating.

    v1.9: Added 24-hour threshold to avoid interfering with concurrent runs.
    Only orphans >24h old are cleaned - this avoids marking concurrent runs
    (that are still actively running) as KILLED.
    */
    IF  @LogToTable = N'Y'
    AND @commandlog_exists = 1
    BEGIN
        DECLARE @orphaned_count int = 0;

        /*
        Insert SP_STATUPDATE_END for each orphaned SP_STATUPDATE_START
        Match on RunLabel from ExtendedInfo XML to identify the specific run.
        Only clean up entries older than @i_orphaned_run_threshold_hours to avoid
        interfering with concurrent runs that are still in progress.

        gh-464: materialize END RunLabels into a temp table once, then LEFT JOIN.
                Previously the cleanup did a correlated NOT EXISTS that forced
                per-outer-row XML parsing of every END row -- unbounded on busy
                CommandLogs (millions of rows + per-row .value() cost).
        */
        CREATE TABLE #orphan_end_labels
        (
            RunLabel nvarchar(100) NOT NULL PRIMARY KEY WITH (IGNORE_DUP_KEY = ON)
        );

        INSERT INTO #orphan_end_labels (RunLabel)
        SELECT cl_end.ExtendedInfo.value('(/Summary/RunLabel)[1]', 'nvarchar(100)')
        FROM dbo.CommandLog AS cl_end
        WHERE cl_end.CommandType = N'SP_STATUPDATE_END'
        AND   cl_end.ExtendedInfo IS NOT NULL
        AND   cl_end.ExtendedInfo.exist('(/Summary/RunLabel)[1]') = 1
        /* Only labels that could match START entries within the candidate window */
        AND   cl_end.StartTime >= DATEADD(HOUR, -(@i_orphaned_run_threshold_hours + 24), SYSDATETIME());

        INSERT INTO dbo.CommandLog
        (
            DatabaseName,
            SchemaName,
            ObjectName,
            ObjectType,
            Command,
            CommandType,
            StartTime,
            EndTime,
            ExtendedInfo
        )
        SELECT
            cl_start.DatabaseName,
            N'dbo',
            N'sp_StatUpdate',
            N'P',
            N'sp_StatUpdate killed (orphaned run cleanup)',
            N'SP_STATUPDATE_END',
            cl_start.StartTime,
            SYSDATETIME(),
            (
                SELECT
                    cl_start.ExtendedInfo.value('(/Parameters/Version)[1]', 'nvarchar(20)') AS [Version],
                    cl_start.ExtendedInfo.value('(/Parameters/RunLabel)[1]', 'nvarchar(100)') AS RunLabel,
                    0 AS StatsFound,
                    0 AS StatsProcessed,
                    0 AS StatsSucceeded,
                    0 AS StatsFailed,
                    0 AS StatsRemaining,
                    0 AS DurationSeconds,
                    N'KILLED' AS StopReason
                FOR XML RAW(N'Summary'), ELEMENTS
            )
        FROM dbo.CommandLog AS cl_start
        WHERE cl_start.CommandType = N'SP_STATUPDATE_START'
        /* #148: Only clean orphans older than @i_orphaned_run_threshold_hours (avoids concurrent run interference) */
        AND   cl_start.StartTime < DATEADD(HOUR, -@i_orphaned_run_threshold_hours, SYSDATETIME())
        /* Must have ExtendedInfo with RunLabel to avoid NULL=NULL matching */
        AND   cl_start.ExtendedInfo IS NOT NULL
        AND   cl_start.ExtendedInfo.exist('(/Parameters/RunLabel)[1]') = 1
        AND   NOT EXISTS
              (
                  SELECT 1
                  FROM #orphan_end_labels AS oel
                  WHERE oel.RunLabel =
                        cl_start.ExtendedInfo.value('(/Parameters/RunLabel)[1]', 'nvarchar(100)')
              );

        SELECT @orphaned_count = @@ROWCOUNT;

        DROP TABLE #orphan_end_labels;

        IF @orphaned_count > 0
        BEGIN
            RAISERROR(N'Cleaned up %d orphaned run(s) from CommandLog', 10, 1, @orphaned_count) WITH NOWAIT;
            RAISERROR(N'', 10, 1) WITH NOWAIT;
        END;
    END;
    /*#endregion 06-RUN-INIT */
    /*#region 07-DISCOVERY: Adaptive sampling, direct mode, staged discovery */
    /*#region 07A-ADAPTIVE: CommandLog query for historically long-running stats */
    /*
    ============================================================================
    QUERY COMMANDLOG FOR LONG-RUNNING STATS (adaptive sampling)
    ============================================================================
    */
    IF  @LongRunningThresholdMinutes IS NOT NULL
    AND @commandlog_exists = 1
    BEGIN
        /*
        Find stats that historically took longer than @LongRunningThresholdMinutes.
        These will get a forced sample rate of @LongRunningSamplePercent.
        Also includes stats that were killed (EndTime IS NULL but ErrorNumber set).
        Uses CTE to extract stat name from XML before GROUP BY (XML methods not allowed in GROUP BY).

        LIMITATION: We don't know WHY stats were slow - could be table size, I/O
        contention, blocking, or high sample rate. Wall-clock time is the best
        available proxy. Consider XE session (tools/sp_StatUpdate_XE_Session.sql)
        for deeper analysis.
        */
        ;WITH long_running_candidates AS
        (
            SELECT
                database_name = cl.DatabaseName,
                schema_name = cl.SchemaName,
                table_name = cl.ObjectName,
                stat_name = COALESCE(
                    cl.ExtendedInfo.value('(/StatInfo/StatisticName)[1]', 'sysname'),
                    cl.StatisticsName,
                    cl.IndexName
                ),
                duration_minutes = DATEDIFF(MINUTE, cl.StartTime, ISNULL(cl.EndTime, SYSDATETIME())),
                start_time = cl.StartTime
            FROM dbo.CommandLog AS cl
            WHERE cl.CommandType = N'UPDATE_STATISTICS'
            AND   (
                      /*Stats that exceeded threshold*/
                      DATEDIFF(MINUTE, cl.StartTime, ISNULL(cl.EndTime, SYSDATETIME())) >= @LongRunningThresholdMinutes
                      /*Or stats that were killed/failed (EndTime NULL but error exists)*/
                      OR (cl.EndTime IS NULL AND cl.ErrorNumber IS NOT NULL)
                  )
        )
        INSERT INTO @long_running_stats
        (
            database_name,
            schema_name,
            table_name,
            stat_name,
            max_duration_minutes,
            last_occurrence,
            occurrence_count
        )
        SELECT
            database_name,
            schema_name,
            table_name,
            stat_name,
            max_duration_minutes = MAX(duration_minutes),
            last_occurrence = MAX(start_time),
            occurrence_count = COUNT(*)
        FROM long_running_candidates
        WHERE stat_name IS NOT NULL
        GROUP BY
            database_name,
            schema_name,
            table_name,
            stat_name;

        DECLARE @long_running_count int = (SELECT COUNT(*) FROM @long_running_stats);

        IF @long_running_count > 0
        BEGIN
            DECLARE @lr_msg nvarchar(500);
            SET @lr_msg = N'Adaptive Sampling: Found ' + CONVERT(nvarchar(10), @long_running_count) +
                          N' stats with historical duration >= ' + CONVERT(nvarchar(10), @LongRunningThresholdMinutes) +
                          N' min (will use ' + CONVERT(nvarchar(10), @LongRunningSamplePercent) + N'%% sample)';
            RAISERROR(@lr_msg, 10, 1) WITH NOWAIT;
            RAISERROR(N'', 10, 1) WITH NOWAIT;
        END;
    END;

    /*#endregion 07A-ADAPTIVE */
    /*#region 07B-MODE-DETECT: Mode detection, DIRECT_STRING db loop, @parameters_string, parallel skip */
    /*
    ============================================================================
    DETERMINE MODE: DIRECT (table), DIRECT (string), or DISCOVERY
    ============================================================================
    */
    DECLARE
        @mode nvarchar(20) =
            CASE
                WHEN @Statistics IS NOT NULL
                THEN N'DIRECT_STRING'
                ELSE N'DISCOVERY'
            END;

    RAISERROR(N'Mode: %s', 10, 1, @mode) WITH NOWAIT;
    RAISERROR(N'', 10, 1) WITH NOWAIT;

    /* P2b startup QS check removed: always ran against master's QS (disabled).
       Per-database forced plan check at end-of-run (line ~8340) covers this correctly. */

    /*
    ============================================================================
    MODE 1B: DIRECT_STRING - Parse @Statistics parameter
    ============================================================================
    */
    IF @mode = N'DIRECT_STRING'
    BEGIN
        RAISERROR(N'Parsing explicit statistics list...', 10, 1) WITH NOWAIT;

        /*
        Materialize parsed stats into temp table so dynamic SQL can see it.
        PARSENAME positions: 3=schema, 2=table, 1=stat
        */
        CREATE TABLE
            #parsed_stats
        (
            parsed_schema sysname NULL,
            parsed_table sysname NULL,
            parsed_stat sysname NOT NULL
        );

        INSERT INTO
            #parsed_stats
        (
            parsed_schema,
            parsed_table,
            parsed_stat
        )
        /* gh-456: PARSENAME preserves brackets ('[My Stat]' stays bracketed).
           sys.stats.name never contains bracket chars, so the join silently
           returns 0 rows.  Strip [] from each part. */
        SELECT
            parsed_schema = REPLACE(REPLACE(PARSENAME(LTRIM(RTRIM(ss.value)), 3), N'[', N''), N']', N''),
            parsed_table  = REPLACE(REPLACE(PARSENAME(LTRIM(RTRIM(ss.value)), 2), N'[', N''), N']', N''),
            parsed_stat   = REPLACE(REPLACE(PARSENAME(LTRIM(RTRIM(ss.value)), 1), N'[', N''), N']', N'')
        FROM STRING_SPLIT(@Statistics, N',') AS ss
        WHERE LTRIM(RTRIM(ss.value)) <> N''
        AND   PARSENAME(LTRIM(RTRIM(ss.value)), 1) IS NOT NULL;

        DECLARE
            @requested_count integer =
            (
                SELECT
                    COUNT_BIG(*)
                FROM #parsed_stats
            );

        /*
        Loop over selected databases -- #parsed_stats visible to dynamic SQL.
        */
        DECLARE
            @direct_string_sql nvarchar(max),
            @ds_skip_msg nvarchar(4000);

        WHILE EXISTS (SELECT 1 FROM @tmpDatabases WHERE Selected = 1 AND Completed = 0)
        BEGIN
            SELECT TOP (1)
                @CurrentDatabaseID = ID,
                @CurrentDatabaseName = DatabaseName
            FROM @tmpDatabases
            WHERE Selected = 1
            AND   Completed = 0
            ORDER BY ID;

            RAISERROR(N'  Scanning database: %s', 10, 1, @CurrentDatabaseName) WITH NOWAIT;

            BEGIN TRY
                SET @direct_string_sql = N'
USE ' + QUOTENAME(@CurrentDatabaseName) + N';

INSERT INTO
    #stats_to_process
(
    database_name,
    schema_name,
    table_name,
    stat_name,
    object_id,
    stats_id,
    no_recompute,
    is_incremental,
    is_memory_optimized,
    is_published,
    is_tracked_by_cdc,
    temporal_type,
    is_heap,
    auto_created,
    modification_counter,
    row_count,
    days_stale,
    page_count,
    persisted_sample_percent,
    histogram_steps,
    has_filter,
    filter_definition,
    unfiltered_rows,
    priority
)
SELECT
    database_name = DB_NAME(),
    schema_name = ISNULL(ps.parsed_schema, OBJECT_SCHEMA_NAME(s.object_id)),
    table_name = ISNULL(ps.parsed_table, OBJECT_NAME(s.object_id)),
    stat_name = s.name,
    object_id = s.object_id,
    stats_id = s.stats_id,
    no_recompute = s.no_recompute,
    is_incremental = s.is_incremental,
    is_memory_optimized = ISNULL(t.is_memory_optimized, 0),
    is_published = ISNULL(t.is_published, 0),
    is_tracked_by_cdc = ISNULL(t.is_tracked_by_cdc, 0),
    temporal_type = ISNULL(t.temporal_type, 0),
    is_heap =
        CASE
            WHEN EXISTS
                 (
                     SELECT
                         1
                     FROM sys.indexes AS i
                     WHERE i.object_id = s.object_id
                     AND   i.index_id = 0
                 )
            THEN 1
            ELSE 0
        END,
    auto_created = s.auto_created,
    modification_counter = ISNULL(sp.modification_counter, 0),
    row_count = ISNULL(sp.rows, 0),
    days_stale = ISNULL(DATEDIFF(DAY, sp.last_updated, SYSDATETIME()), 9999),
    page_count = ISNULL(pgs.total_pages, 0),
    persisted_sample_percent = sp.persisted_sample_percent,
    histogram_steps = sp.steps,
    has_filter = s.has_filter,
    filter_definition = s.filter_definition,
    unfiltered_rows = sp.unfiltered_rows,
    priority = ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
FROM #parsed_stats AS ps
JOIN sys.stats AS s
  ON s.name COLLATE DATABASE_DEFAULT = ps.parsed_stat COLLATE DATABASE_DEFAULT
 AND (
         ps.parsed_table IS NULL
      OR OBJECT_NAME(s.object_id) COLLATE DATABASE_DEFAULT = ps.parsed_table COLLATE DATABASE_DEFAULT
     )
 AND (
         ps.parsed_schema IS NULL
      OR OBJECT_SCHEMA_NAME(s.object_id) COLLATE DATABASE_DEFAULT = ps.parsed_schema COLLATE DATABASE_DEFAULT
     )
LEFT JOIN sys.tables AS t
  ON t.object_id = s.object_id
CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
OUTER APPLY
(
    SELECT
        total_pages = SUM(p.used_page_count)
    FROM sys.dm_db_partition_stats AS p
    WHERE p.object_id = s.object_id
    AND   p.index_id IN (0, 1)
) AS pgs
WHERE (OBJECTPROPERTY(s.object_id, N''IsUserTable'') = 1 OR @i_include_system_objects_param = N''Y'')
/* gh-492: NORECOMPUTE filter (was missing in DIRECT_STRING path) */
AND   (
          (@TargetNorecompute_param = N''N'' AND s.no_recompute = 0)
       OR (@TargetNorecompute_param = N''Y'' AND s.no_recompute = 1)
       OR @TargetNorecompute_param = N''BOTH''
      )
/* gh-492: Statistics exclusion filter (was missing in DIRECT_STRING path) */
AND   (
          @ExcludeStatistics_param IS NULL
       OR NOT EXISTS
          (
              SELECT 1
              FROM STRING_SPLIT(@ExcludeStatistics_param, N'','') AS ex
              WHERE s.name COLLATE DATABASE_DEFAULT LIKE LTRIM(RTRIM(ex.value)) COLLATE DATABASE_DEFAULT
                 OR s.name COLLATE DATABASE_DEFAULT = LTRIM(RTRIM(ex.value)) COLLATE DATABASE_DEFAULT
          )
      )
OPTION (RECOMPILE);';

                EXECUTE sys.sp_executesql
                    @direct_string_sql,
                    N'@i_include_system_objects_param nvarchar(1), @TargetNorecompute_param nvarchar(10), @ExcludeStatistics_param nvarchar(max)',
                    @i_include_system_objects_param = @i_include_system_objects,
                    @TargetNorecompute_param = @TargetNorecompute,
                    @ExcludeStatistics_param = @ExcludeStatistics;

            END TRY
            BEGIN CATCH
                SET @ds_skip_msg =
                    N'  WARNING: Skipping database ' + QUOTENAME(@CurrentDatabaseName) +
                    N' - ' + ERROR_MESSAGE();
                RAISERROR(@ds_skip_msg, 10, 1) WITH NOWAIT;
                /* #204: Add per-database skip to @WarningsOut */
                SET @warnings += N'DB_SKIPPED: ' + @CurrentDatabaseName + N'; ';
            END CATCH;

            UPDATE @tmpDatabases
            SET Completed = 1
            WHERE ID = @CurrentDatabaseID;

        END; /* End of WHILE database loop for DIRECT_STRING */

        DROP TABLE #parsed_stats;

        /*
        Warn about any stats not found
        */
        DECLARE
            @found_count integer =
            (
                SELECT
                    COUNT_BIG(*)
                FROM #stats_to_process
            );

        IF @found_count < @requested_count
        BEGIN
            RAISERROR(N'Warning: Only %d of %d requested statistics found across %d database(s)', 10, 1, @found_count, @requested_count, @database_count) WITH NOWAIT;
        END;
    END;
    /*
    ============================================================================
    PARALLEL DISCOVERY SKIP: Check if another worker already populated the queue
    ============================================================================
    */
    /* gh-461: Single construction site for @parameters_string.
       Built unconditionally here for all run modes (parallel and serial).
       Previously guarded by @StatsInParallel = N'Y', leaving non-parallel runs
       to rebuild it in region 08.  Removed -- one construction site in 07B. */
    IF @parameters_string = N''
    BEGIN
        SELECT
            @parameters_string =
                N'@Databases=' + ISNULL(LTRIM(RTRIM(@Databases)), N'') +
                N',@Tables=' + ISNULL(LTRIM(RTRIM(@Tables)), N'') +
                N',@ExcludeTables=' + ISNULL(LTRIM(RTRIM(@ExcludeTables)), N'') +
                N',@ExcludeStatistics=' + ISNULL(LTRIM(RTRIM(@ExcludeStatistics)), N'') +
                N',@TargetNorecompute=' + ISNULL(LTRIM(RTRIM(@TargetNorecompute)), N'') +
                N',@i_modification_threshold=' + ISNULL(CONVERT(nvarchar(20), @i_modification_threshold), N'') +
                N',@i_min_page_count=' + ISNULL(CONVERT(nvarchar(20), @i_min_page_count), N'') +
                N',@i_include_system_objects=' + ISNULL(LTRIM(RTRIM(@i_include_system_objects)), N'') +
                N',@i_sort_order=' + ISNULL(LTRIM(RTRIM(@i_sort_order)), N'') +
                N',@i_tiered_thresholds=' + ISNULL(CONVERT(nvarchar(5), @i_tiered_thresholds), N'') +
                N',@i_filtered_stats_mode=' + ISNULL(LTRIM(RTRIM(@i_filtered_stats_mode)), N'') +
                N',@i_qs_enabled=' + ISNULL(LTRIM(RTRIM(@i_qs_enabled)), N'') +
                N',@i_threshold_logic=' + ISNULL(LTRIM(RTRIM(@i_threshold_logic)), N'') +
                N',@StaleHours=' + ISNULL(CONVERT(nvarchar(20), @StaleHours), N'') +
                N',@i_modification_percent=' + ISNULL(CONVERT(nvarchar(20), @i_modification_percent), N'') +
                N',@i_ascending_key_boost=' + ISNULL(LTRIM(RTRIM(@i_ascending_key_boost)), N'') +
                N',@i_filtered_stats_stale_factor=' + ISNULL(CONVERT(nvarchar(20), @i_filtered_stats_stale_factor), N'') +
                N',@i_skip_tables_with_columnstore=' + ISNULL(LTRIM(RTRIM(@i_skip_tables_with_columnstore)), N'') +
                N',@i_include_indexed_views=' + ISNULL(LTRIM(RTRIM(@i_include_indexed_views)), N'') +
                N',@i_qs_metric=' + ISNULL(LTRIM(RTRIM(@i_qs_metric)), N'') +
                N',@i_qs_min_executions=' + ISNULL(CONVERT(nvarchar(20), @i_qs_min_executions), N'') +
                N',@i_qs_recent_hours=' + ISNULL(CONVERT(nvarchar(20), @i_qs_recent_hours), N'') +
                N',@i_qs_top_plans=' + ISNULL(CONVERT(nvarchar(20), @i_qs_top_plans), N'');
    END;

    DECLARE @skip_discovery bit = 0;
    IF  @StatsInParallel = N'Y'
    AND @mode = N'DISCOVERY'
    AND OBJECT_ID(N'dbo.Queue', N'U') IS NOT NULL
    AND OBJECT_ID(N'dbo.QueueStatistic', N'U') IS NOT NULL
    BEGIN
        /* @queue_id already declared in variables section */
        SELECT @queue_id = q.QueueID
        FROM dbo.Queue AS q
        WHERE q.SchemaName = N'dbo'
        AND   q.ObjectName = N'sp_StatUpdate'
        AND   q.Parameters = @parameters_string;

        IF  @queue_id IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM dbo.QueueStatistic AS qs
            WHERE qs.QueueID = @queue_id
            AND   qs.TableEndTime IS NULL
        )
        BEGIN
            SET @skip_discovery = 1;
            RAISERROR(N'Parallel mode: Active queue found (QueueID = %d) -- skipping redundant discovery.', 10, 1, @queue_id) WITH NOWAIT;
        END
        ELSE
        BEGIN
            SET @queue_id = NULL;
        END;
    END;

    /*#endregion 07B-MODE-DETECT */
    /*#region 07C-DISC-PHASES-1-4: Phase 1 candidates, Phase 2 stats properties, Phase 3 tier thresholds, Phase 4 threshold filter */
    /*
    ============================================================================
    MODE 2: DISCOVERY - DMV-based candidate selection
    ============================================================================
    */
    IF @mode = N'DISCOVERY' AND @skip_discovery = 0
    BEGIN
        RAISERROR(N'Discovering qualifying statistics via DMV...', 10, 1) WITH NOWAIT;

        DECLARE
            @discovery_sql nvarchar(max) = N'',
            @discovery_params nvarchar(max) = N'';

        /*
        ========================================================================
        LOOP OVER SELECTED DATABASES
        ========================================================================
        */
        /*
        Signal table for staged discovery failure detection.
        Created before the database loop so it persists across iterations.
        Dynamic SQL can write to it to signal unexpected row loss.
        */
        CREATE TABLE #staged_discovery_failed (reason nvarchar(500));

        WHILE EXISTS (SELECT 1 FROM @tmpDatabases WHERE Selected = 1 AND Completed = 0)
        BEGIN
            /*
            Get next database to process
            */
            SELECT TOP (1)
                @CurrentDatabaseID = ID,
                @CurrentDatabaseName = DatabaseName
            FROM @tmpDatabases
            WHERE Selected = 1
            AND   Completed = 0
            ORDER BY ID;

            RAISERROR(N'  Scanning database: %s', 10, 1, @CurrentDatabaseName) WITH NOWAIT;

            /*
            Per-database TRY/CATCH: Skip databases that become inaccessible during the run
            (SINGLE_USER acquired by another session, taken offline, etc.) instead of
            aborting the entire run or counting toward @i_max_consecutive_failures.
            */
            BEGIN TRY

            /*
            ========================================================================
            PER-DATABASE ENVIRONMENT INFO (Phase 1.1/1.4 - v2.0, debug mode only)
            ========================================================================
            Show compatibility level, effective CE version, and relevant db-scoped configs.
            */
            IF @Debug = 1
            BEGIN
                DECLARE
                    @db_compat_level int,
                    @db_ce_version int,
                    @db_legacy_ce bit = 0,
                    @db_env_sql nvarchar(max);

                /* Get compatibility level */
                SELECT @db_compat_level = compatibility_level
                FROM sys.databases
                WHERE name = @CurrentDatabaseName;

                /* Calculate effective CE version */
                SET @db_ce_version = CASE
                    WHEN @db_compat_level <= 110 THEN 70
                    WHEN @db_compat_level = 120 THEN 120
                    ELSE @db_compat_level
                END;

                RAISERROR(N'    Compat Level: %d, CE Version: %d', 10, 1, @db_compat_level, @db_ce_version) WITH NOWAIT;

                /* Check database-scoped configurations (SQL 2016+) (#298) */
                IF @sql_major_version >= 13
                BEGIN
                    DECLARE @db_qo_hotfixes bit = 0;

                    SET @db_env_sql = N'
                        SELECT @legacy_ce_out = MAX(CASE WHEN name = N''LEGACY_CARDINALITY_ESTIMATION'' AND value = 1 THEN 1 ELSE 0 END),
                               @qo_hotfixes_out = MAX(CASE WHEN name = N''QUERY_OPTIMIZER_HOTFIXES'' AND value = 1 THEN 1 ELSE 0 END)
                        FROM ' + QUOTENAME(@CurrentDatabaseName) + N'.sys.database_scoped_configurations
                        WHERE name IN (N''LEGACY_CARDINALITY_ESTIMATION'', N''QUERY_OPTIMIZER_HOTFIXES'');';

                    BEGIN TRY
                        EXEC sp_executesql @db_env_sql,
                            N'@legacy_ce_out bit OUTPUT, @qo_hotfixes_out bit OUTPUT',
                            @legacy_ce_out = @db_legacy_ce OUTPUT,
                            @qo_hotfixes_out = @db_qo_hotfixes OUTPUT;
                    END TRY
                    BEGIN CATCH
                        SET @db_legacy_ce = 0;
                        SET @db_qo_hotfixes = 0;
                    END CATCH;

                    IF @db_legacy_ce = 1
                    BEGIN
                        SET @db_ce_version = 70; /* Override CE version when legacy CE is forced via db-scoped config */
                        RAISERROR(N'    Note: LEGACY_CARDINALITY_ESTIMATION=ON (CE forced to 70)', 10, 1) WITH NOWAIT;
                    END;

                    IF @db_qo_hotfixes = 1
                        RAISERROR(N'    Note: QUERY_OPTIMIZER_HOTFIXES=ON (optimizer fixes enabled for this database)', 10, 1) WITH NOWAIT;
                END;

                /* Note if CE version differs from what compat level suggests due to trace flags */
                IF @tf_9481_active = 1 AND @db_compat_level >= 120
                    RAISERROR(N'    Note: TF 9481 forcing legacy CE despite compat level %d', 10, 1, @db_compat_level) WITH NOWAIT;

                /*
                RCSI / Snapshot Isolation detection (#53)
                Statistics updates under RCSI generate version store overhead in tempdb.
                */
                DECLARE
                    @db_rcsi bit = 0,
                    @db_snapshot bit = 0;

                SELECT
                    @db_rcsi = is_read_committed_snapshot_on,
                    @db_snapshot = snapshot_isolation_state
                FROM sys.databases
                WHERE name = @CurrentDatabaseName;

                IF @db_rcsi = 1
                    RAISERROR(N'    Note: RCSI enabled - stats updates generate version store overhead', 10, 1) WITH NOWAIT;
                IF @db_snapshot > 0
                    RAISERROR(N'    Note: Snapshot Isolation active - version store impact expected', 10, 1) WITH NOWAIT;
            END;

            /*
            ========================================================================
            STAGED DISCOVERY (6-phase approach for better performance)
            v3 uses this as the only discovery path.
            ========================================================================
              Phase 1: Collect basic candidate stats (fast - only sys.stats/objects)
              Phase 2: Batch-enrich with stats properties (one CROSS APPLY)
              Phase 3: Pre-calculate tier thresholds (no inline SQRT)
              Phase 4: Apply threshold filters (early elimination)
              Phase 5: Add page counts (only for qualifying stats)
              Phase 6: Add Query Store data (only if enabled, only for qualifying)
                       Uses batched CTE + JOIN instead of CROSS APPLY (O(1) vs O(n))

            Faster than the single-query approach for large databases (10K+ stats):
              - Expensive DMV calls only run on candidates that might qualify
              - No inline SQRT calculations in WHERE clause
              - Query Store join (most expensive) runs last and only if needed
            */
            BEGIN
                DECLARE @staged_sql nvarchar(max);
                /* gh-465: Zero-row sentinel SELECT shared by all Phase 1-6 bailout paths.
                   Column order and types are the INSERT...EXEC contract.
                   Any schema change needs updating only here. */
                DECLARE @empty_disc_select nvarchar(max) =
                    N'SELECT' + CHAR(13)+CHAR(10) +
                    N'                        database_name = DB_NAME(),' + CHAR(13)+CHAR(10) +
                    N'                        schema_name = CONVERT(sysname, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        table_name = CONVERT(sysname, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        stat_name = CONVERT(sysname, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        object_id = CONVERT(int, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        stats_id = CONVERT(int, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        no_recompute = CONVERT(bit, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        is_incremental = CONVERT(bit, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        is_memory_optimized = CONVERT(bit, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        is_heap = CONVERT(bit, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        auto_created = CONVERT(bit, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        modification_counter = CONVERT(bigint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        row_count = CONVERT(bigint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        days_stale = CONVERT(int, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        page_count = CONVERT(bigint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        persisted_sample_percent = CONVERT(float, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        histogram_steps = CONVERT(int, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        has_filter = CONVERT(bit, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        filter_definition = CONVERT(nvarchar(max), NULL),' + CHAR(13)+CHAR(10) +
                    N'                        unfiltered_rows = CONVERT(bigint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        qs_plan_count = CONVERT(int, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        qs_total_executions = CONVERT(bigint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        qs_total_cpu_ms = CONVERT(bigint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        qs_total_duration_ms = CONVERT(bigint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        qs_total_logical_reads = CONVERT(bigint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        qs_total_memory_grant_kb = CONVERT(bigint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        qs_total_tempdb_pages = CONVERT(bigint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        qs_total_physical_reads = CONVERT(bigint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        qs_total_logical_writes = CONVERT(bigint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        qs_total_wait_time_ms = CONVERT(bigint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        qs_max_dop = CONVERT(smallint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        qs_active_feedback_count = CONVERT(int, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        qs_last_execution = CONVERT(datetime2, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        qs_priority_boost = CONVERT(bigint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        is_published = CONVERT(bit, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        is_tracked_by_cdc = CONVERT(bit, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        temporal_type = CONVERT(tinyint, NULL),' + CHAR(13)+CHAR(10) +
                    N'                        priority = CONVERT(bigint, NULL)' + CHAR(13)+CHAR(10) +
                    N'                    WHERE 1 = 0;';


                SET @staged_sql = N'
                USE ' + QUOTENAME(@CurrentDatabaseName) + N';

                /*
                Phase row count tracking for validation (P1 #22)
                Prevents silent failures when phases don''t process expected rows.
                */
                DECLARE
                    @phase1_count int = 0,
                    @phase2_count int = 0,
                    @phase4_qualifying int = 0,
                    @phase5_remaining int = 0,
                    @phase_timer datetime2(7) = SYSDATETIME(),
                    @phase_ms int = 0;

                /*
                ================================================================
                PHASE 1: Collect basic candidate stats (FAST)
                Only touches sys.stats and sys.objects - no DMV cross-applies.
                ================================================================
                */
                IF OBJECT_ID(N''tempdb..#stat_candidates'') IS NOT NULL DROP TABLE #stat_candidates;
                /* bd -s4z: all columns declared upfront (was 5 ALTER TABLE ADD passes).
                   Eliminates 5 metadata lock operations per database. */
                CREATE TABLE #stat_candidates (
                    object_id int NOT NULL,
                    stats_id int NOT NULL,
                    stat_name sysname NOT NULL,
                    schema_name sysname NOT NULL,
                    table_name sysname NOT NULL,
                    no_recompute bit NOT NULL,
                    is_incremental bit NOT NULL,
                    has_filter bit NOT NULL,
                    filter_definition nvarchar(max) NULL,
                    is_memory_optimized bit NOT NULL DEFAULT 0,
                    auto_created bit NOT NULL DEFAULT 0,
                    is_published bit NOT NULL DEFAULT 0,
                    is_tracked_by_cdc bit NOT NULL DEFAULT 0,
                    temporal_type tinyint NOT NULL DEFAULT 0,
                    /* Phase 2: stats properties */
                    modification_counter bigint NULL,
                    rows bigint NULL,
                    last_updated datetime2 NULL,
                    unfiltered_rows bigint NULL,
                    persisted_sample_percent float NULL,
                    histogram_steps int NULL,
                    /* Phase 3: tier thresholds */
                    tier_threshold bigint NULL,
                    sqrt_threshold bigint NULL,
                    days_stale int NULL,
                    hours_stale int NULL,
                    /* Phase 4: qualification flag */
                    qualifies bit NOT NULL DEFAULT 0,
                    /* Phase 5: page counts */
                    page_count bigint NULL,
                    is_heap bit NULL,
                    /* Phase 6: Query Store enrichment */
                    qs_plan_count int NULL,
                    qs_total_executions bigint NULL,
                    qs_total_cpu_ms bigint NULL,
                    qs_total_duration_ms bigint NULL,
                    qs_total_logical_reads bigint NULL,
                    qs_total_memory_grant_kb bigint NULL,
                    qs_total_tempdb_pages bigint NULL,
                    qs_total_physical_reads bigint NULL,
                    qs_total_logical_writes bigint NULL,
                    qs_total_wait_time_ms bigint NULL,
                    qs_max_dop smallint NULL,
                    qs_active_feedback_count int NULL,
                    qs_last_execution datetime2 NULL,
                    qs_priority_boost bigint NULL,
                    PRIMARY KEY CLUSTERED (object_id, stats_id)
                );

                INSERT INTO #stat_candidates
                    (object_id, stats_id, stat_name, schema_name, table_name,
                     no_recompute, is_incremental, has_filter, filter_definition, is_memory_optimized, auto_created,
                     is_published, is_tracked_by_cdc, temporal_type)
                SELECT
                    s.object_id,
                    s.stats_id,
                    s.name,
                    OBJECT_SCHEMA_NAME(s.object_id),
                    OBJECT_NAME(s.object_id),
                    s.no_recompute,
                    s.is_incremental,
                    s.has_filter,
                    s.filter_definition,
                    ISNULL(t.is_memory_optimized, 0),
                    s.auto_created,
                    ISNULL(t.is_published, 0),
                    ISNULL(t.is_tracked_by_cdc, 0),
                    ISNULL(t.temporal_type, 0)
                FROM sys.stats AS s
                JOIN sys.objects AS o ON o.object_id = s.object_id
                LEFT JOIN sys.tables AS t ON t.object_id = s.object_id
                WHERE (o.is_ms_shipped = 0 OR @i_include_system_objects_param = N''Y'')
                AND   (
                          OBJECTPROPERTY(s.object_id, N''IsUserTable'') = 1
                       OR @i_include_system_objects_param = N''Y''
                       OR (o.type = N''V'' AND @i_include_indexed_views_param = N''Y''
                           AND EXISTS (SELECT 1 FROM sys.indexes AS vi WHERE vi.object_id = s.object_id AND vi.index_id = 1))
                      ) /* #49: Include indexed views when requested */
                AND   o.type NOT IN (N''ET'', N''S'') /* Exclude external tables (#54) and system tables */
                /* #55: Skip Stretch Database tables (deprecated, causes timeouts to Azure storage) */
                AND   ISNULL(OBJECTPROPERTY(s.object_id, N''TableHasRemoteDataArchive''), 0) = 0
                /* Skip tables on READ_ONLY filegroups (#65) */
                AND   NOT EXISTS
                      (
                          SELECT 1
                          FROM sys.indexes AS ri
                          JOIN sys.data_spaces AS rds ON rds.data_space_id = ri.data_space_id
                          JOIN sys.filegroups AS rfg ON rfg.data_space_id = rds.data_space_id
                          WHERE ri.object_id = s.object_id
                          AND   ri.index_id IN (0, 1) /* heap or clustered -- the base storage */
                          AND   rfg.is_read_only = 1
                      )
                /* NORECOMPUTE filter */
                AND   (
                          (@TargetNorecompute_param = N''N'' AND s.no_recompute = 0)
                       OR (@TargetNorecompute_param = N''Y'' AND s.no_recompute = 1)
                       OR @TargetNorecompute_param = N''BOTH''
                      )
                /* Table filter */
                AND   (
                          @Tables_param IS NULL
                       OR OBJECT_SCHEMA_NAME(s.object_id) + N''.'' + OBJECT_NAME(s.object_id) COLLATE DATABASE_DEFAULT IN
                          (SELECT LTRIM(RTRIM(ss.value)) COLLATE DATABASE_DEFAULT FROM STRING_SPLIT(@Tables_param, N'','') AS ss)
                       OR OBJECT_NAME(s.object_id) COLLATE DATABASE_DEFAULT IN
                          (SELECT LTRIM(RTRIM(ss.value)) COLLATE DATABASE_DEFAULT FROM STRING_SPLIT(@Tables_param, N'','') AS ss)
                      )
                /* Table exclusion filter */
                AND   (
                          @ExcludeTables_param IS NULL
                       OR NOT EXISTS
                          (
                              SELECT 1
                              FROM STRING_SPLIT(@ExcludeTables_param, N'','') AS ex
                              WHERE OBJECT_SCHEMA_NAME(s.object_id) + N''.'' + OBJECT_NAME(s.object_id) COLLATE DATABASE_DEFAULT LIKE LTRIM(RTRIM(ex.value)) COLLATE DATABASE_DEFAULT
                          )
                      )
                /* Statistics exclusion filter */
                AND   (
                          @ExcludeStatistics_param IS NULL
                       OR NOT EXISTS
                          (
                              SELECT 1
                              FROM STRING_SPLIT(@ExcludeStatistics_param, N'','') AS ex
                              WHERE s.name COLLATE DATABASE_DEFAULT LIKE LTRIM(RTRIM(ex.value)) COLLATE DATABASE_DEFAULT
                                 OR s.name COLLATE DATABASE_DEFAULT = LTRIM(RTRIM(ex.value)) COLLATE DATABASE_DEFAULT
                          )
                      )
                /* Filtered stats mode filter */
                AND   (
                          @i_filtered_stats_mode_param = N''INCLUDE''
                       OR @i_filtered_stats_mode_param = N''PRIORITY''
                       OR (@i_filtered_stats_mode_param = N''EXCLUDE'' AND s.has_filter = 0)
                       OR (@i_filtered_stats_mode_param = N''ONLY'' AND s.has_filter = 1)
                      )
                /* v2.3: Skip tables with columnstore indexes when @i_skip_tables_with_columnstore = Y */
                /* Columnstore indexes (type 5=nonclustered columnstore, 6=clustered columnstore) */
                /* may have plan stability concerns when rowstore stats are updated alongside NCCIs */
                AND   (
                          @i_skip_tables_with_columnstore_param = N''N''
                       OR NOT EXISTS
                          (
                              SELECT 1
                              FROM sys.indexes AS ci
                              WHERE ci.object_id = s.object_id
                              AND   ci.type IN (5, 6) /* 5 = nonclustered columnstore, 6 = clustered columnstore */
                          )
                      );

                SELECT @phase1_count = @@ROWCOUNT,
                       @phase_ms = DATEDIFF(MILLISECOND, @phase_timer, SYSDATETIME());

                IF @Debug_param = 1
                    RAISERROR(N''    Phase 1 (candidates): %d stats (%d ms)'', 10, 1, @phase1_count, @phase_ms) WITH NOWAIT;

                /* v2.24: Early exit when Phase 1 finds 0 candidates -- avoids 4 ALTER TABLE ADD + 4 UPDATE statements */
                IF @phase1_count = 0
                BEGIN
                    IF @Debug_param = 1
                        RAISERROR(N''    No candidates found - skipping remaining phases'', 10, 1) WITH NOWAIT;

                    ' + @empty_disc_select + N'

                    DROP TABLE #stat_candidates;
                    RETURN;
                END;

                SET @phase_timer = SYSDATETIME();

                /*
                ================================================================
                PHASE 2: Enrich with stats properties (batch CROSS APPLY)
                ================================================================
                */
                UPDATE sc
                SET
                    sc.modification_counter = ISNULL(sp.modification_counter, 0),
                    sc.rows = ISNULL(sp.rows, 0),
                    sc.last_updated = sp.last_updated,
                    sc.unfiltered_rows = sp.unfiltered_rows,
                    sc.persisted_sample_percent = sp.persisted_sample_percent,
                    sc.histogram_steps = sp.steps
                FROM #stat_candidates AS sc
                CROSS APPLY sys.dm_db_stats_properties(sc.object_id, sc.stats_id) AS sp;

                SELECT @phase2_count = @@ROWCOUNT,
                       @phase_ms = DATEDIFF(MILLISECOND, @phase_timer, SYSDATETIME());

                IF @Debug_param = 1
                    RAISERROR(N''    Phase 2 (enriched): %d stats (%d ms)'', 10, 1, @phase2_count, @phase_ms) WITH NOWAIT;

                /*
                VALIDATION: CROSS APPLY filters out stats with no properties.
                This is expected for never-updated stats. Log if significant.
                Unexpected total loss (enrichment <1% of candidates) emits a WARNING and
                continues with partial data -- there is no fallback path in v3.
                */
                IF @phase1_count > 100 AND (@phase2_count * 100) < @phase1_count
                BEGIN
                    /* v3: no fallback path -- log warning and continue with partial data */
                    RAISERROR(N''    WARNING: Phase 2 enriched %d of %d candidates (<1%%) -- skipping database'', 10, 1, @phase2_count, @phase1_count) WITH NOWAIT;

                    /* Return empty result set to satisfy INSERT...EXEC schema */
                    ' + @empty_disc_select + N'

                    DROP TABLE #stat_candidates;
                    RETURN;
                END;

                IF @phase2_count < @phase1_count AND @Debug_param = 1
                BEGIN
                    DECLARE @missing_count int = @phase1_count - @phase2_count;
                    RAISERROR(N''    Note: %d stats have no properties (never updated)'', 10, 1, @missing_count) WITH NOWAIT;
                    /* #317: Log Phase 1→2 losses using set-based aggregation instead of cursor */
                    DECLARE @phase_loss_all nvarchar(4000) =
                        STUFF((
                            SELECT CHAR(13) + CHAR(10) + N''  Phase 1→2 loss: '' + QUOTENAME(schema_name) + N''.'' + QUOTENAME(table_name) + N''.'' + QUOTENAME(stat_name)
                            FROM #stat_candidates
                            WHERE modification_counter IS NULL
                            ORDER BY schema_name, table_name, stat_name
                            FOR XML PATH(N''''), TYPE
                        ).value(N''.'', N''nvarchar(4000)''), 1, 2, N'''');
                    IF @phase_loss_all IS NOT NULL
                        RAISERROR(@phase_loss_all, 10, 1) WITH NOWAIT;
                END;

                /*
                ================================================================
                PHASE 3: Pre-calculate tier thresholds (avoids inline SQRT)
                ================================================================
                */
                UPDATE #stat_candidates
                SET
                    tier_threshold =
                        CASE
                            WHEN rows <= 500 THEN 500
                            WHEN rows <= 10000 THEN (rows * 20) / 100 + 500
                            WHEN rows <= 100000 THEN (rows * 15) / 100 + 500
                            WHEN rows <= 1000000 THEN (rows * 10) / 100 + 500
                            ELSE CONVERT(bigint, CONVERT(float, rows) * 5 / 100) + 500
                        END,
                    sqrt_threshold = CONVERT(bigint, SQRT(CONVERT(float, CASE WHEN ISNULL(rows, 0) < 1 THEN 1 ELSE rows END) * 1000)),
                    days_stale = ISNULL(DATEDIFF(DAY, last_updated, SYSDATETIME()), 9999),
                    hours_stale = ISNULL(DATEDIFF(HOUR, last_updated, SYSDATETIME()), 999999); /* v2.3 */

                DECLARE @phase3_count int = (SELECT COUNT(*) FROM #stat_candidates);
                SET @phase_ms = DATEDIFF(MILLISECOND, @phase_timer, SYSDATETIME());
                IF @Debug_param = 1
                    RAISERROR(N''    Phase 3 (tier thresholds): %d stats calculated (%d ms)'', 10, 1, @phase3_count, @phase_ms) WITH NOWAIT;

                /* Defensive validation -- if any row with non-NULL rows has NULL tier_threshold,
                   the ALTER TABLE ADD or UPDATE silently failed.  Emit WARNING and continue
                   with partial data -- there is no fallback path in v3. */
                IF EXISTS (SELECT 1 FROM #stat_candidates WHERE tier_threshold IS NULL AND rows IS NOT NULL)
                BEGIN
                    /* v3: no fallback path -- log warning and continue with partial data */
                    RAISERROR(N''    WARNING: Phase 3 has NULL tier_threshold with non-NULL rows -- skipping database'', 10, 1) WITH NOWAIT;

                    ' + @empty_disc_select + N'

                    DROP TABLE #stat_candidates;
                    RETURN;
                END;

                SET @phase_timer = SYSDATETIME();

                /*
                ================================================================
                PHASE 4: Apply threshold filters (early elimination)
                ================================================================
                */
                /* Apply threshold logic (v3: OR is the only supported mode) */
                UPDATE #stat_candidates
                SET qualifies = 1
                WHERE (
                    /* Fixed modification threshold */
                    (@ModificationThreshold_param IS NOT NULL AND modification_counter >= @ModificationThreshold_param)
                    /* Modification percent (non-tiered) */
                    OR (@i_tiered_thresholds_param = 0 AND @i_modification_percent_param IS NOT NULL
                        AND modification_counter >= (@i_modification_percent_param * SQRT(CONVERT(float, ISNULL(rows, 1)))))
                    /* Tiered thresholds */
                    OR (@i_tiered_thresholds_param = 1 AND (modification_counter >= tier_threshold OR modification_counter >= sqrt_threshold))
                    /* Hours stale (v2.3) */
                    OR (@StaleHours_param IS NOT NULL AND hours_stale >= @StaleHours_param)
                    /* No thresholds = include all */
                    OR (@ModificationThreshold_param IS NULL AND @i_modification_percent_param IS NULL
                        AND @i_tiered_thresholds_param = 0 AND @StaleHours_param IS NULL)
                );

                /*
                #143: Ascending key boost -- identity column stats with ANY modifications
                bypass normal thresholds. Leading column of stat checked against
                sys.identity_columns. This prevents histogram staleness for insert-heavy
                tables where modification_counter hasn''t hit threshold yet.
                */
                IF @i_ascending_key_boost_param = N''Y''
                BEGIN
                    DECLARE @ascending_rescued int = 0;

                    UPDATE sc
                    SET sc.qualifies = 1
                    FROM #stat_candidates AS sc
                    WHERE sc.qualifies = 0
                    AND   sc.modification_counter > 0
                    AND   (
                        /* Identity columns */
                        EXISTS (
                            SELECT 1
                            FROM sys.stats_columns AS stc
                            JOIN sys.identity_columns AS ic
                              ON ic.object_id = stc.object_id AND ic.column_id = stc.column_id
                            WHERE stc.object_id = sc.object_id
                            AND   stc.stats_id = sc.stats_id
                            AND   stc.stats_column_id = 1
                        )
                        /* #277: SEQUENCE default constraints on leading column */
                        OR EXISTS (
                            SELECT 1
                            FROM sys.stats_columns AS stc
                            JOIN sys.columns AS c
                              ON c.object_id = stc.object_id AND c.column_id = stc.column_id
                            JOIN sys.default_constraints AS dc
                              ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
                            WHERE stc.object_id = sc.object_id
                            AND   stc.stats_id = sc.stats_id
                            AND   stc.stats_column_id = 1
                            AND   dc.definition LIKE N''%%NEXT VALUE FOR%%''
                        )
                        /* #277: Datetime-family leading columns (naturally ascending) */
                        OR EXISTS (
                            SELECT 1
                            FROM sys.stats_columns AS stc
                            JOIN sys.columns AS c
                              ON c.object_id = stc.object_id AND c.column_id = stc.column_id
                            WHERE stc.object_id = sc.object_id
                            AND   stc.stats_id = sc.stats_id
                            AND   stc.stats_column_id = 1
                            AND   c.system_type_id IN (40, 41, 42, 43, 58, 61) /* date, time, datetime2, datetimeoffset, smalldatetime, datetime */
                        )
                    );

                    SET @ascending_rescued = @@ROWCOUNT;

                    IF @ascending_rescued > 0 AND @Debug_param = 1
                        RAISERROR(N''    Phase 4: Ascending key boost rescued %d stat(s)'', 10, 1, @ascending_rescued) WITH NOWAIT;
                END;

                /* Delete non-qualifying stats early */
                DELETE FROM #stat_candidates WHERE qualifies = 0;

                SELECT @phase4_qualifying = (SELECT COUNT(*) FROM #stat_candidates),
                       @phase_ms = DATEDIFF(MILLISECOND, @phase_timer, SYSDATETIME());

                IF @Debug_param = 1
                    RAISERROR(N''    Phase 4 (after thresholds): %d stats qualify (%d ms)'', 10, 1, @phase4_qualifying, @phase_ms) WITH NOWAIT;
                SET @phase_timer = SYSDATETIME();

                /*
                VALIDATION: If 0 stats qualify after threshold filtering, we can exit early.
                This is normal when no stats need updating.
                */
                IF @phase4_qualifying = 0
                BEGIN
                    IF @Debug_param = 1
                        RAISERROR(N''    No stats qualify - skipping remaining phases'', 10, 1) WITH NOWAIT;

                    /*
                    Return empty result set with correct schema.
                    Required: INSERT...EXEC consumes this result set, so we must
                    provide matching column schema even when returning 0 rows.
                    */
                    ' + @empty_disc_select + N'

                    DROP TABLE #stat_candidates;
                    RETURN;
                END;

                /*#endregion 07C-DISC-PHASES-1-4 */
                /*#region 07D-DISC-PHASES-5-6: Page counts, QS enrichment, plan XML parsing */
                /*
                ================================================================
                PHASE 5: Add page counts (only for qualifying stats)
                ================================================================
                */
                UPDATE sc
                SET
                    sc.page_count = ISNULL(pgs.total_pages, 0),
                    sc.is_heap = CASE WHEN ix.index_id IS NOT NULL THEN 1 ELSE 0 END
                FROM #stat_candidates AS sc
                OUTER APPLY (
                    SELECT SUM(p.used_page_count) AS total_pages
                    FROM sys.dm_db_partition_stats AS p
                    WHERE p.object_id = sc.object_id
                    AND   p.index_id IN (0, 1)
                ) AS pgs
                LEFT JOIN sys.indexes AS ix ON ix.object_id = sc.object_id AND ix.index_id = 0;

                /* NULL page count detection -- if >50% of rows have NULL page_count,
                   dm_db_partition_stats likely failed silently.  Check BEFORE MinPageCount filter
                   so the detection is independent of @i_min_page_count setting.
                   Emits WARNING and continues with partial data -- no fallback path in v3. */
                IF @phase4_qualifying > 10
                BEGIN
                    DECLARE @null_page_count int = (SELECT COUNT(*) FROM #stat_candidates WHERE page_count IS NULL);
                    IF @null_page_count > (@phase4_qualifying / 2)
                    BEGIN
                        /* v3: no fallback path -- log warning and continue with partial data */
                        RAISERROR(N''    WARNING: Phase 5 has %d of %d stats with NULL page_count -- skipping database'', 10, 1,
                            @null_page_count, @phase4_qualifying) WITH NOWAIT;

                        ' + @empty_disc_select + N'

                        DROP TABLE #stat_candidates;
                        RETURN;
                    END;
                END;

                /* Apply MinPageCount filter */
                DELETE FROM #stat_candidates WHERE ISNULL(page_count, 0) < @i_min_page_count_param;

                SELECT @phase5_remaining = (SELECT COUNT(*) FROM #stat_candidates),
                       @phase_ms = DATEDIFF(MILLISECOND, @phase_timer, SYSDATETIME());

                IF @Debug_param = 1
                    RAISERROR(N''    Phase 5 (after MinPageCount): %d stats remain (%d ms)'', 10, 1, @phase5_remaining, @phase_ms) WITH NOWAIT;
                SET @phase_timer = SYSDATETIME();

                /*
                VALIDATION: Suspicious row loss in Phase 5.
                If MinPageCount is 0 (no filtering expected) and we lost >50% of rows,
                the page count UPDATE may have failed silently.  Emits WARNING and
                continues with partial data -- no fallback path in v3.
                */
                IF  @i_min_page_count_param = 0
                AND @phase4_qualifying > 0
                AND @phase5_remaining * 2 < @phase4_qualifying
                BEGIN
                    /* v3: no fallback path -- log warning and continue with partial data */
                    RAISERROR(N''    WARNING: Phase 5 kept %d of %d rows with MinPageCount=0 -- skipping database'', 10, 1,
                        @phase5_remaining, @phase4_qualifying) WITH NOWAIT;

                    ' + @empty_disc_select + N'

                    DROP TABLE #stat_candidates;
                    RETURN;
                END;

                /*
                ================================================================
                PHASE 6: Add Query Store data (only if enabled)
                P2 #20: Skip QS operations when QS is disabled.
                ================================================================
                */
                /*
                Only populate QS data if:
                1. User requested QS priority (@i_qs_enabled = Y)
                2. Query Store is enabled on this database (actual_state IN 1, 2)
                If neither, skip the UPDATE - NULL will be handled by ISNULL in ORDER BY.
                This avoids touching all rows when QS is disabled (10K+ stat databases).

                v1.9: Warn if Query Store is READ_ONLY (state 1) as data may be stale.
                */
                /* bd -67s: consolidate 3 reads of sys.database_query_store_options into 1 */
                DECLARE @qs_actual_state tinyint, @qs_retention_days int;
                SELECT @qs_actual_state = actual_state, @qs_retention_days = stale_query_threshold_days
                FROM sys.database_query_store_options;

                IF @i_qs_enabled_param = 1
                AND @qs_actual_state IN (1, 2)
                BEGIN
                    /* v1.9: Warn if Query Store is READ_ONLY (might have stale data) */
                    IF @qs_actual_state = 1 AND @Debug_param = 1
                        RAISERROR(N''    Warning: Query Store is READ_ONLY - priority data may be stale'', 10, 1) WITH NOWAIT;

                    IF @qs_retention_days IS NOT NULL AND @i_qs_recent_hours_param > (@qs_retention_days * 24)
                    BEGIN
                        DECLARE @qs_ret_warn nvarchar(500) = N''    Warning: @i_qs_recent_hours=''
                            + CONVERT(nvarchar(10), @i_qs_recent_hours_param) + N''h exceeds QS retention ''
                            + CONVERT(nvarchar(10), @qs_retention_days * 24) + N''h -- enrichment may return no data'';
                        RAISERROR(@qs_ret_warn, 10, 1) WITH NOWAIT;
                    END;

                    /*
                    FIX #271: Plan XML table extraction for QS enrichment.
                    Previous approach joined on qsq.object_id (the MODULE id for stored
                    procs/functions) to #stat_candidates.object_id (the TABLE id from
                    sys.stats).  These are different domains -- the join produced zero
                    matches for all workload types (ad-hoc, ORM, stored proc).
                    Now we parse plan XML to find which tables each QS plan references.
                    */

                    /* Initialize qs_priority_boost to 0 for all rows first */
                    UPDATE #stat_candidates SET qs_priority_boost = 0;

                    /*#region PHASE-6-EARLY-BAILOUT */
                    /* Early bail-out: skip XML parsing if QS has no recent runtime data */
                    IF NOT EXISTS (
                        SELECT 1 FROM sys.query_store_runtime_stats_interval
                        WHERE end_time >= DATEADD(HOUR, -@i_qs_recent_hours_param, SYSDATETIME())
                    )
                    BEGIN
                        IF @Debug_param = 1
                            RAISERROR(N''    Phase 6: skipped -- no QS runtime stats in last %d hours'', 10, 1, @i_qs_recent_hours_param) WITH NOWAIT;
                    END
                    ELSE
                    BEGIN
                    /*#endregion PHASE-6-EARLY-BAILOUT */

                    /*#region PHASE-6-QS-ENRICHMENT */
                    /* Step 1: Rank plans by @i_qs_metric, take TOP N for XML parsing.
                       Step 2: Extract table references from plan XML.
                       Step 3: Filter to only tables we have stat candidates for.
                       Step 4: Aggregate runtime stats per table object_id.
                       bd -iqw: WAITS metric pre-aggregates wait stats in a CTE
                       (WaitsByPlan) instead of a correlated subquery per plan row.
                       Injected via {{WAITS_CTE}}/{{WAITS_JOIN}}/{{WAITS_ORDER}}
                       REPLACE tokens; empty on SQL 2016 (no query_store_wait_stats). */
                    ;WITH {{WAITS_CTE}}TopPlanIds AS (
                        SELECT TOP (ISNULL(@i_qs_top_plans_param, 2147483647))
                            qsp.plan_id
                        FROM sys.query_store_plan AS qsp
                        JOIN sys.query_store_runtime_stats AS qsrs
                            ON qsrs.plan_id = qsp.plan_id
                        JOIN sys.query_store_runtime_stats_interval AS qsrsi
                            ON qsrsi.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
                        {{WAITS_JOIN}}
                        WHERE qsrsi.end_time >= DATEADD(HOUR, -@i_qs_recent_hours_param, SYSDATETIME())
                        GROUP BY qsp.plan_id
                        ORDER BY CASE @i_qs_metric_param
                            WHEN N''CPU''        THEN SUM(qsrs.avg_cpu_time * qsrs.count_executions)
                            WHEN N''DURATION''    THEN SUM(qsrs.avg_duration * qsrs.count_executions)
                            WHEN N''READS''       THEN SUM(CONVERT(float, qsrs.avg_logical_io_reads * qsrs.count_executions))
                            WHEN N''EXECUTIONS''  THEN SUM(CONVERT(float, qsrs.count_executions))
                            WHEN N''AVG_CPU''     THEN SUM(qsrs.avg_cpu_time * qsrs.count_executions) / NULLIF(SUM(CONVERT(float, qsrs.count_executions)), 0) /* gh-453: avg, not total */
                            WHEN N''MEMORY_GRANT'' THEN SUM(CONVERT(float, qsrs.avg_query_max_used_memory * qsrs.count_executions))
                            WHEN N''TEMPDB_SPILLS'' THEN {{TEMPDB_SPILLS_ORDER}}
                            WHEN N''PHYSICAL_READS'' THEN {{PHYSICAL_READS_ORDER}}
                            WHEN N''AVG_MEMORY'' THEN SUM(CONVERT(float, qsrs.avg_query_max_used_memory * qsrs.count_executions)) / NULLIF(SUM(CONVERT(float, qsrs.count_executions)), 0)
                            WHEN N''WAITS'' THEN {{WAITS_ORDER}}
                            ELSE SUM(qsrs.avg_cpu_time * qsrs.count_executions)
                        END DESC
                    ),
                    PlanTableRefs AS (
                        SELECT DISTINCT
                            tpi.plan_id,
                            PARSENAME(ref.value(''@Schema'',  ''sysname''), 1) AS ref_schema,
                            PARSENAME(ref.value(''@Table'',   ''sysname''), 1) AS ref_table
                        FROM TopPlanIds AS tpi
                        JOIN sys.query_store_plan AS qsp
                            ON qsp.plan_id = tpi.plan_id
                        CROSS APPLY (SELECT TRY_CONVERT(xml, qsp.query_plan)) AS x(plan_xml)
                        CROSS APPLY x.plan_xml.nodes(''//*:Object'') AS t(ref)
                        WHERE x.plan_xml IS NOT NULL
                          AND ref.value(''@Table'', ''sysname'') IS NOT NULL
                    ),
                    FilteredRefs AS (
                        SELECT DISTINCT ptr.plan_id, sc.object_id
                        FROM PlanTableRefs AS ptr
                        JOIN (SELECT DISTINCT object_id, schema_name, table_name
                              FROM #stat_candidates) AS sc
                            ON sc.schema_name = ptr.ref_schema COLLATE DATABASE_DEFAULT
                            AND sc.table_name = ptr.ref_table COLLATE DATABASE_DEFAULT
                    ),
                    QSByTable AS (
                        SELECT
                            fr.object_id,
                            COUNT_BIG(DISTINCT fr.plan_id) AS plan_count,
                            SUM(qsrs2.count_executions) AS total_executions,
                            SUM(qsrs2.avg_cpu_time * qsrs2.count_executions) / 1000 AS total_cpu_ms,
                            SUM(qsrs2.avg_duration * qsrs2.count_executions) / 1000 AS total_duration_ms,
                            SUM(CONVERT(bigint, qsrs2.avg_logical_io_reads * qsrs2.count_executions)) AS total_logical_reads,
                            SUM(CONVERT(bigint, qsrs2.avg_query_max_used_memory * qsrs2.count_executions)) * 8 AS total_memory_grant_kb,
                            {{TEMPDB_SPILLS_EXPR}} AS total_tempdb_pages,
                            {{PHYSICAL_READS_EXPR}} AS total_physical_reads,
                            SUM(CONVERT(bigint, qsrs2.avg_logical_io_writes * qsrs2.count_executions)) AS total_logical_writes,
                            MAX(qsrs2.last_dop) AS max_dop,
                            MAX(qsrs2.last_execution_time) AS last_execution
                        FROM FilteredRefs AS fr
                        JOIN sys.query_store_runtime_stats AS qsrs2
                            ON qsrs2.plan_id = fr.plan_id
                        JOIN sys.query_store_runtime_stats_interval AS qsrsi2
                            ON qsrsi2.runtime_stats_interval_id = qsrs2.runtime_stats_interval_id
                        WHERE qsrsi2.end_time >= DATEADD(HOUR, -@i_qs_recent_hours_param, SYSDATETIME())
                        GROUP BY fr.object_id
                        HAVING SUM(qsrs2.count_executions) > 0
                    )
                    UPDATE sc
                    SET
                        sc.qs_plan_count = qs.plan_count,
                        sc.qs_total_executions = qs.total_executions,
                        sc.qs_total_cpu_ms = qs.total_cpu_ms,
                        sc.qs_total_duration_ms = qs.total_duration_ms,
                        sc.qs_total_logical_reads = qs.total_logical_reads,
                        sc.qs_total_memory_grant_kb = qs.total_memory_grant_kb,
                        sc.qs_total_tempdb_pages = qs.total_tempdb_pages,
                        sc.qs_total_physical_reads = qs.total_physical_reads,
                        sc.qs_total_logical_writes = qs.total_logical_writes,
                        sc.qs_max_dop = qs.max_dop,
                        sc.qs_last_execution = qs.last_execution,
                        sc.qs_priority_boost =
                            CASE
                                WHEN ISNULL(qs.total_executions, 0) >= @i_qs_min_executions_param
                                THEN CASE @i_qs_metric_param
                                         WHEN N''CPU'' THEN ISNULL(qs.total_cpu_ms, 0)
                                         WHEN N''DURATION'' THEN ISNULL(qs.total_duration_ms, 0)
                                         WHEN N''READS'' THEN ISNULL(qs.total_logical_reads, 0) / 1000
                                         WHEN N''EXECUTIONS'' THEN ISNULL(qs.total_executions, 0)
                                         WHEN N''AVG_CPU'' THEN ISNULL(qs.total_cpu_ms, 0) / NULLIF(qs.total_executions, 0) /* avg CPU per execution */
                                         WHEN N''MEMORY_GRANT'' THEN ISNULL(qs.total_memory_grant_kb, 0)
                                         WHEN N''TEMPDB_SPILLS'' THEN ISNULL(qs.total_tempdb_pages, 0)
                                         WHEN N''PHYSICAL_READS'' THEN ISNULL(qs.total_physical_reads, 0)
                                         WHEN N''AVG_MEMORY'' THEN ISNULL(qs.total_memory_grant_kb, 0) / NULLIF(qs.total_executions, 0)
                                         WHEN N''WAITS'' THEN 0 /* populated by separate wait stats UPDATE below */
                                         ELSE ISNULL(qs.total_cpu_ms, 0)
                                     END
                                ELSE 0
                            END
                    FROM #stat_candidates AS sc
                    INNER JOIN QSByTable AS qs ON qs.object_id = sc.object_id;

                    /* Mark unenriched stats: 0 = enrichment ran, no matching QS plans
                       (vs NULL = enrichment skipped/bailed out).  Allows diag proc W5
                       to distinguish "QS healthy, tables not in top plans" from "QS broken." */
                    UPDATE #stat_candidates SET qs_plan_count = 0 WHERE qs_plan_count IS NULL;

                    /* SQL 2022+ plan feedback awareness (#369):
                       Count active/pending plan feedback entries per table.
                       Stats updates may reset CE/grant/DOP feedback.
                       #432: use plan XML table extraction (not q.object_id which is module id).
                       gh-460: bound work via @i_qs_recent_hours time filter + @i_qs_top_plans cap.
                               Previously this processed every active/pending feedback entry with
                               no time or row budget -- unbounded XML parsing on instances with
                               deep QS retention. */
                    IF OBJECT_ID(N''sys.query_store_plan_feedback'') IS NOT NULL
                    BEGIN
                        ;WITH RecentFeedbackPlanIds AS (
                            SELECT TOP (ISNULL(@i_qs_top_plans_param, 2147483647))
                                pf2.plan_id,
                                feedback_count = COUNT(*)
                            FROM sys.query_store_plan_feedback AS pf2
                            INNER JOIN sys.query_store_plan AS p2 ON p2.plan_id = pf2.plan_id
                            WHERE pf2.state_desc IN (N''ACTIVE'', N''PENDING_VALIDATION'')
                              AND (pf2.last_updated_time IS NULL
                                   OR pf2.last_updated_time >= DATEADD(HOUR, -@i_qs_recent_hours_param, SYSDATETIME()))
                            GROUP BY pf2.plan_id
                            ORDER BY COUNT(*) DESC
                        )
                        UPDATE sc
                        SET sc.qs_active_feedback_count = fb.feedback_count
                        FROM #stat_candidates AS sc
                        INNER JOIN (
                            SELECT tref.object_id, SUM(rfp.feedback_count) AS feedback_count
                            FROM RecentFeedbackPlanIds AS rfp
                            INNER JOIN sys.query_store_plan AS p ON rfp.plan_id = p.plan_id
                            CROSS APPLY (SELECT TRY_CONVERT(xml, p.query_plan)) AS x(plan_xml)
                            CROSS APPLY x.plan_xml.nodes(''//*:Object'') AS t(ref)
                            INNER JOIN (SELECT DISTINCT object_id, schema_name, table_name
                                        FROM #stat_candidates) AS tref
                                ON tref.schema_name = PARSENAME(ref.value(''@Schema'', ''sysname''), 1) COLLATE DATABASE_DEFAULT
                                AND tref.table_name = PARSENAME(ref.value(''@Table'', ''sysname''), 1) COLLATE DATABASE_DEFAULT
                            WHERE x.plan_xml IS NOT NULL
                              AND ref.value(''@Table'', ''sysname'') IS NOT NULL
                            GROUP BY tref.object_id
                        ) AS fb ON fb.object_id = sc.object_id;
                    END;

                    /* #366: Stats-relevant wait stats enrichment (SQL 2017+).
                       Only runs when @i_qs_metric = WAITS to avoid unbounded XML parsing (#371).
                       Uses same plan XML extraction as QSByTable to correctly map
                       plan_ids to table object_ids (q.object_id is the MODULE id,
                       not the table id -- same issue #271 fixed for runtime_stats). */
                    IF OBJECT_ID(N''sys.query_store_wait_stats'') IS NOT NULL
                    AND @i_qs_metric_param = N''WAITS''
                    BEGIN
                        /* #431: compute TopPlanIds for WAITS FIRST, then parse XML only for those plans */
                        ;WITH WaitTopPlanIds AS (
                            SELECT TOP (ISNULL(@i_qs_top_plans_param, 2147483647))
                                qsp2.plan_id
                            FROM sys.query_store_plan AS qsp2
                            JOIN sys.query_store_wait_stats AS qsws2 ON qsws2.plan_id = qsp2.plan_id
                            JOIN sys.query_store_runtime_stats_interval AS qsrsi2
                                ON qsrsi2.runtime_stats_interval_id = qsws2.runtime_stats_interval_id
                            WHERE qsws2.wait_category IN (3, 12, 15)
                              AND qsrsi2.end_time >= DATEADD(HOUR, -@i_qs_recent_hours_param, SYSDATETIME())
                            GROUP BY qsp2.plan_id
                            ORDER BY SUM(qsws2.total_query_wait_time_ms) DESC
                        ),
                        WaitPlanTableRefs AS (
                            SELECT DISTINCT
                                wtpi.plan_id,
                                PARSENAME(ref.value(''@Schema'',  ''sysname''), 1) AS ref_schema,
                                PARSENAME(ref.value(''@Table'',   ''sysname''), 1) AS ref_table
                            FROM WaitTopPlanIds AS wtpi
                            JOIN sys.query_store_plan AS qsp
                                ON qsp.plan_id = wtpi.plan_id
                            CROSS APPLY (SELECT TRY_CONVERT(xml, qsp.query_plan)) AS x(plan_xml)
                            CROSS APPLY x.plan_xml.nodes(''//*:Object'') AS t(ref)
                            WHERE x.plan_xml IS NOT NULL
                              AND ref.value(''@Table'', ''sysname'') IS NOT NULL
                        ),
                        WaitFilteredRefs AS (
                            SELECT DISTINCT wptr.plan_id, sc2.object_id
                            FROM WaitPlanTableRefs AS wptr
                            JOIN (SELECT DISTINCT object_id, schema_name, table_name
                                  FROM #stat_candidates) AS sc2
                                ON sc2.schema_name = wptr.ref_schema COLLATE DATABASE_DEFAULT
                                AND sc2.table_name = wptr.ref_table COLLATE DATABASE_DEFAULT
                        ),
                        WaitByTable AS (
                            SELECT
                                wfr.object_id,
                                SUM(qsws.total_query_wait_time_ms) AS total_wait_ms
                            FROM WaitFilteredRefs AS wfr
                            JOIN sys.query_store_wait_stats AS qsws
                                ON qsws.plan_id = wfr.plan_id
                            JOIN sys.query_store_runtime_stats_interval AS qsrsi
                                ON qsrsi.runtime_stats_interval_id = qsws.runtime_stats_interval_id
                            WHERE qsrsi.end_time >= DATEADD(HOUR, -@i_qs_recent_hours_param, SYSDATETIME())
                            AND qsws.wait_category IN (3, 12, 15) /* BufferIO, Memory, SortAndTempDb */
                            GROUP BY wfr.object_id
                        )
                        UPDATE sc
                        SET sc.qs_total_wait_time_ms = wbt.total_wait_ms
                        FROM #stat_candidates AS sc
                        INNER JOIN WaitByTable AS wbt ON wbt.object_id = sc.object_id;
                    END;

                    /* #366: Update priority boost for WAITS metric (must run after wait stats UPDATE) */
                    IF @i_qs_metric_param = N''WAITS''
                    BEGIN
                        UPDATE sc
                        SET sc.qs_priority_boost = CASE
                            WHEN ISNULL(sc.qs_total_executions, 0) >= @i_qs_min_executions_param
                            THEN ISNULL(sc.qs_total_wait_time_ms, 0)
                            ELSE 0
                        END
                        FROM #stat_candidates AS sc
                        WHERE sc.qs_total_wait_time_ms IS NOT NULL;
                    END;

                    END; /* ELSE from early bail-out */
                    /*#endregion PHASE-6-QS-ENRICHMENT */
                END; /* IF @i_qs_enabled AND QS enabled */

                /* v2.36: Report how many stats were enriched with QS data (only if enrichment ran) */
                IF @i_qs_enabled_param = 1
                AND EXISTS (SELECT 1 FROM sys.database_query_store_options WHERE actual_state IN (1, 2))
                BEGIN
                    DECLARE @phase6_enriched int = (SELECT COUNT(*) FROM #stat_candidates WHERE ISNULL(qs_priority_boost, 0) > 0);
                    SET @phase_ms = DATEDIFF(MILLISECOND, @phase_timer, SYSDATETIME());
                    IF @Debug_param = 1
                        RAISERROR(N''    Phase 6 (Query Store): %d stats enriched with QS data (%d ms)'', 10, 1, @phase6_enriched, @phase_ms) WITH NOWAIT;
                END;

                /*
                ================================================================
                FINAL: Return results with priority ordering
                ================================================================
                */
                SELECT
                    database_name = DB_NAME(),
                    schema_name,
                    table_name,
                    stat_name,
                    object_id,
                    stats_id,
                    no_recompute,
                    is_incremental,
                    is_memory_optimized,
                    ISNULL(is_heap, 0) AS is_heap,
                    ISNULL(auto_created, 0) AS auto_created,
                    ISNULL(modification_counter, 0) AS modification_counter,
                    row_count = ISNULL(rows, 0),
                    ISNULL(days_stale, 9999) AS days_stale,
                    ISNULL(page_count, 0) AS page_count,
                    persisted_sample_percent,
                    histogram_steps,
                    has_filter,
                    filter_definition,
                    unfiltered_rows,
                    qs_plan_count,
                    qs_total_executions,
                    qs_total_cpu_ms,
                    qs_total_duration_ms,
                    qs_total_logical_reads,
                    qs_total_memory_grant_kb,
                    qs_total_tempdb_pages,
                    qs_total_physical_reads,
                    qs_total_logical_writes,
                    qs_total_wait_time_ms,
                    qs_max_dop,
                    qs_active_feedback_count,
                    qs_last_execution,
                    ISNULL(qs_priority_boost, 0) AS qs_priority_boost,
                    is_published,
                    is_tracked_by_cdc,
                    temporal_type,
                    priority = ROW_NUMBER() OVER (
                        ORDER BY
                            /* #167: Filtered stats priority boost -- when @i_filtered_stats_mode=PRIORITY, boost filtered stats showing drift */
                            CASE WHEN @i_filtered_stats_mode_param = N''PRIORITY''
                                      AND has_filter = 1
                                      AND ISNULL(rows, 0) > 0
                                      AND unfiltered_rows IS NOT NULL
                                      AND (CONVERT(float, unfiltered_rows) / rows) > @i_filtered_stats_stale_factor_param
                                 THEN CONVERT(bigint, 500000000000)
                                 ELSE 0
                            END +
                            CASE @i_sort_order_param
                                WHEN N''MODIFICATION_COUNTER'' THEN modification_counter + ISNULL(qs_priority_boost, 0)
                                WHEN N''DAYS_STALE'' THEN days_stale + ISNULL(qs_priority_boost, 0)
                                WHEN N''PAGE_COUNT'' THEN ISNULL(page_count, 0) + ISNULL(qs_priority_boost, 0)
                                WHEN N''RANDOM'' THEN CHECKSUM(NEWID())
                                WHEN N''QUERY_STORE'' THEN ISNULL(qs_priority_boost, 0)
                                WHEN N''FILTERED_DRIFT'' THEN
                                    CASE WHEN has_filter = 1 AND ISNULL(rows, 0) > 0 AND unfiltered_rows IS NOT NULL
                                         THEN CONVERT(bigint, IIF((CONVERT(float, unfiltered_rows) / rows) > 9000000000000.0, 9000000000000.0, (CONVERT(float, unfiltered_rows) / rows)) * 1000000)
                                         ELSE 0
                                    END + ISNULL(qs_priority_boost, 0)
                                WHEN N''AUTO_CREATED'' THEN
                                    CASE WHEN ISNULL(auto_created, 0) = 0 THEN CONVERT(bigint, 1000000000000) ELSE 0 END + ISNULL(modification_counter, 0) + ISNULL(qs_priority_boost, 0)
                                WHEN N''ROWS'' THEN ISNULL(rows, 0) + ISNULL(qs_priority_boost, 0)
                                ELSE modification_counter + ISNULL(qs_priority_boost, 0)
                            END DESC,
                            /* #185: Deterministic tie-breaker -- stable order for same priority score */
                            object_id ASC,
                            stats_id ASC
                    )
                FROM #stat_candidates
                {{MAX_GRANT_HINT}}

                DROP TABLE #stat_candidates;
                ';

                /*
                gh-466: Token-replace MAX_GRANT_PERCENT hint.  Replaces the prior
                sentinel-replace approach (REPLACE on literal '= 25)') which was
                fragile to any change in the sentinel value or surrounding syntax.
                Token is unique in @staged_sql; substitution is deterministic.
                */
                SET @staged_sql = REPLACE(@staged_sql, N'{{MAX_GRANT_HINT}}',
                    CASE WHEN @i_max_grant_percent IS NOT NULL
                        THEN N'OPTION (MAX_GRANT_PERCENT = ' + CONVERT(nvarchar(10), @i_max_grant_percent) + N');'
                        ELSE N';'
                    END);

                /* Version-gate TEMPDB_SPILLS: avg_tempdb_space_used added in SQL 2017 (14.x) */
                SET @staged_sql = REPLACE(@staged_sql, N'{{TEMPDB_SPILLS_EXPR}}',
                    CASE WHEN @sql_major_version >= 14
                    THEN N'SUM(CONVERT(bigint, qsrs2.avg_tempdb_space_used * qsrs2.count_executions))'
                    ELSE N'CONVERT(bigint, 0)'
                    END);
                SET @staged_sql = REPLACE(@staged_sql, N'{{TEMPDB_SPILLS_ORDER}}',
                    CASE WHEN @sql_major_version >= 14
                    THEN N'SUM(CONVERT(float, qsrs.avg_tempdb_space_used * qsrs.count_executions))'
                    ELSE N'SUM(CONVERT(float, 0))'
                    END);

                /* Version-gate PHYSICAL_READS: avg_num_physical_io_reads added in SQL 2017 (14.x) */
                SET @staged_sql = REPLACE(@staged_sql, N'{{PHYSICAL_READS_EXPR}}',
                    CASE WHEN @sql_major_version >= 14
                    THEN N'SUM(CONVERT(bigint, qsrs2.avg_num_physical_io_reads * qsrs2.count_executions))'
                    ELSE N'CONVERT(bigint, 0)'
                    END);
                SET @staged_sql = REPLACE(@staged_sql, N'{{PHYSICAL_READS_ORDER}}',
                    CASE WHEN @sql_major_version >= 14
                    THEN N'SUM(CONVERT(float, qsrs.avg_num_physical_io_reads * qsrs.count_executions))'
                    ELSE N'SUM(CONVERT(float, 0))'
                    END);

                /* Version-gate WAITS: sys.query_store_wait_stats added in SQL 2017 (14.x).
                   bd -iqw: pre-aggregate wait stats in a CTE + LEFT JOIN instead of
                   a correlated subquery in the ORDER BY (O(N+M) vs O(N*M) on large QS). */
                SET @staged_sql = REPLACE(@staged_sql, N'{{WAITS_CTE}}',
                    CASE WHEN @sql_major_version >= 14
                    THEN N'WaitsByPlan AS (
                        SELECT qsws.plan_id, SUM(CONVERT(float, qsws.total_query_wait_time_ms)) AS wait_ms
                        FROM sys.query_store_wait_stats AS qsws
                        JOIN sys.query_store_runtime_stats_interval AS qsrsi2
                            ON qsrsi2.runtime_stats_interval_id = qsws.runtime_stats_interval_id
                        WHERE qsws.wait_category IN (3, 12, 15)
                          AND qsrsi2.end_time >= DATEADD(HOUR, -@i_qs_recent_hours_param, SYSDATETIME())
                        GROUP BY qsws.plan_id
                    ),
                    '
                    ELSE N''
                    END);
                SET @staged_sql = REPLACE(@staged_sql, N'{{WAITS_JOIN}}',
                    CASE WHEN @sql_major_version >= 14
                    THEN N'LEFT JOIN WaitsByPlan AS wbp ON wbp.plan_id = qsp.plan_id'
                    ELSE N''
                    END);
                SET @staged_sql = REPLACE(@staged_sql, N'{{WAITS_ORDER}}',
                    CASE WHEN @sql_major_version >= 14
                    THEN N'ISNULL(MAX(wbp.wait_ms), 0)'
                    ELSE N'SUM(CONVERT(float, 0))'
                    END);

                /*
                Execute staged discovery and insert results
                */
                INSERT INTO #stats_to_process
                (
                    database_name, schema_name, table_name, stat_name, object_id, stats_id,
                    no_recompute, is_incremental, is_memory_optimized, is_heap, auto_created,
                    modification_counter, row_count, days_stale, page_count, persisted_sample_percent, histogram_steps,
                    has_filter, filter_definition, unfiltered_rows,
                    qs_plan_count, qs_total_executions, qs_total_cpu_ms, qs_total_duration_ms,
                    qs_total_logical_reads, qs_total_memory_grant_kb, qs_total_tempdb_pages, qs_total_physical_reads, qs_total_logical_writes, qs_total_wait_time_ms, qs_max_dop, qs_active_feedback_count,
                    qs_last_execution, qs_priority_boost,
                    is_published, is_tracked_by_cdc, temporal_type, priority
                )
                EXECUTE sys.sp_executesql
                    @staged_sql,
                    N'@i_sort_order_param nvarchar(50),
                      @TargetNorecompute_param nvarchar(10),
                      @ModificationThreshold_param bigint,
                      @i_modification_percent_param float,
                      @i_tiered_thresholds_param bit,
                      @StaleHours_param integer,
                      @i_min_page_count_param bigint,
                      @i_include_system_objects_param nvarchar(1),
                      @Tables_param nvarchar(max),
                      @ExcludeTables_param nvarchar(max),
                      @ExcludeStatistics_param nvarchar(max),
                      @i_filtered_stats_mode_param nvarchar(10),
                      @i_filtered_stats_stale_factor_param float,
                      @i_qs_enabled_param bit,
                      @i_qs_metric_param nvarchar(20),
                      @i_qs_min_executions_param bigint,
                      @i_qs_recent_hours_param integer,
                      @i_skip_tables_with_columnstore_param nchar(1),
                      @i_include_indexed_views_param nvarchar(1),
                      @i_ascending_key_boost_param nvarchar(1),
                      @i_qs_top_plans_param integer,
                      @Debug_param bit',
                    @i_sort_order_param = @i_sort_order,
                    @TargetNorecompute_param = @TargetNorecompute,
                    @ModificationThreshold_param = @i_modification_threshold,
                    @i_modification_percent_param = @i_modification_percent,
                    @i_tiered_thresholds_param = @i_tiered_thresholds,
                    @StaleHours_param = @StaleHours,
                    @i_min_page_count_param = @i_min_page_count,
                    @i_include_system_objects_param = @i_include_system_objects,
                    @Tables_param = @Tables,
                    @ExcludeTables_param = @ExcludeTables,
                    @ExcludeStatistics_param = @ExcludeStatistics,
                    @i_filtered_stats_mode_param = @i_filtered_stats_mode,
                    @i_filtered_stats_stale_factor_param = @i_filtered_stats_stale_factor,
                    @i_qs_enabled_param = @i_qs_enabled,
                    @i_qs_metric_param = @i_qs_metric,
                    @i_qs_min_executions_param = @i_qs_min_executions,
                    @i_qs_recent_hours_param = @i_qs_recent_hours,
                    @i_skip_tables_with_columnstore_param = @i_skip_tables_with_columnstore,
                    @i_include_indexed_views_param = @i_include_indexed_views,
                    @i_ascending_key_boost_param = @i_ascending_key_boost,
                    @i_qs_top_plans_param = @i_qs_top_plans,
                    @Debug_param = @Debug;
            END; /* End of staged discovery */
            /*#endregion 07D-DISC-PHASES-5-6 */
            /*#region 07E-POST-DB-WARNINGS: RLS, wide stats, filtered index, columnstore, CDC, computed cols */
            /*
            ================================================================
            POST-DISCOVERY PER-DATABASE DETECTION WARNINGS
            Lightweight checks on discovered stats for operational awareness.

            gh-470: Skip entire block when the current database has zero stats
            in #stats_to_process.  Avoids up to 6 sequential sp_executesql
            compilations + DMV scans per database when there's nothing to warn
            about (common when wildcards or exclusions trim to 0 stats in a db).
            ================================================================
            */
            DECLARE @pdw_stats_in_db int = 0;
            SELECT @pdw_stats_in_db = COUNT(*)
            FROM #stats_to_process AS stp
            WHERE stp.database_name = @CurrentDatabaseName COLLATE DATABASE_DEFAULT;

            IF @pdw_stats_in_db > 0
            BEGIN

            /* #60: RLS (Row-Level Security) detection -- biased histogram risk */
            IF @Debug = 1
            BEGIN
                DECLARE @rls_check_sql nvarchar(max) = N'
                    SELECT @cnt = COUNT(DISTINCT sp.target_object_id)
                    FROM ' + QUOTENAME(@CurrentDatabaseName) + N'.sys.security_policies AS sp
                    INNER JOIN ' + QUOTENAME(@CurrentDatabaseName) + N'.sys.security_predicates AS spd
                        ON spd.object_id = sp.object_id
                    WHERE sp.is_enabled = 1
                    AND spd.target_object_id IN (
                        SELECT DISTINCT stp.object_id
                        FROM #stats_to_process AS stp
                        WHERE stp.database_name = @dbname COLLATE DATABASE_DEFAULT
                    )';
                DECLARE @rls_table_count int = 0;
                BEGIN TRY
                    EXEC sp_executesql @rls_check_sql,
                        N'@dbname sysname, @cnt int OUTPUT',
                        @dbname = @CurrentDatabaseName, @cnt = @rls_table_count OUTPUT;
                    IF @rls_table_count > 0
                    BEGIN
                        DECLARE @rls_msg nvarchar(500) = N'    RLS: ' + CONVERT(nvarchar(10), @rls_table_count)
                            + N' table(s) with Row-Level Security -- histogram quality may vary by execution context';
                        RAISERROR(@rls_msg, 10, 1) WITH NOWAIT;
                        SET @warnings += N'RLS_DETECTED: ' + @CurrentDatabaseName + N'(' + CONVERT(nvarchar(10), @rls_table_count) + N' tables); ';
                    END;
                END TRY
                BEGIN CATCH
                    /* gh-468: surface rather than swallow (was: empty CATCH) */
                    SET @warnings += N'RLS_CHECK_FAILED: ' + @CurrentDatabaseName + N' (' + ISNULL(ERROR_MESSAGE(), N'unknown') + N'); ';
                    IF @Debug = 1 RAISERROR(N'    RLS check skipped: %s', 10, 1, @CurrentDatabaseName) WITH NOWAIT;
                END CATCH;
            END;

            /* #59: Wide statistics detection (>8 columns) -- tempdb/memory pressure risk */
            IF @Debug = 1
            BEGIN
                DECLARE @wide_check_sql nvarchar(max) = N'
                    SELECT @cnt = COUNT(*)
                    FROM (
                        SELECT sc2.object_id, sc2.stats_id, col_count = COUNT(*)
                        FROM ' + QUOTENAME(@CurrentDatabaseName) + N'.sys.stats_columns AS sc2
                        WHERE EXISTS (
                            SELECT 1 FROM #stats_to_process AS stp
                            WHERE stp.database_name = @dbname COLLATE DATABASE_DEFAULT
                            AND stp.object_id = sc2.object_id
                            AND stp.stats_id = sc2.stats_id
                        )
                        GROUP BY sc2.object_id, sc2.stats_id
                        HAVING COUNT(*) > 8
                    ) AS wide';
                DECLARE @wide_stat_count int = 0;
                BEGIN TRY
                    EXEC sp_executesql @wide_check_sql,
                        N'@dbname sysname, @cnt int OUTPUT',
                        @dbname = @CurrentDatabaseName, @cnt = @wide_stat_count OUTPUT;
                    IF @wide_stat_count > 0
                    BEGIN
                        DECLARE @wide_msg nvarchar(500) = N'    Wide Stats: ' + CONVERT(nvarchar(10), @wide_stat_count)
                            + N' statistic(s) with >8 columns -- FULLSCAN may use significant tempdb/memory';
                        RAISERROR(@wide_msg, 10, 1) WITH NOWAIT;
                    END;
                END TRY
                BEGIN CATCH
                    /* gh-468: surface rather than swallow */
                    SET @warnings += N'WIDESTAT_CHECK_FAILED: ' + @CurrentDatabaseName + N' (' + ISNULL(ERROR_MESSAGE(), N'unknown') + N'); ';
                    IF @Debug = 1 RAISERROR(N'    Wide-stats check skipped: %s', 10, 1, @CurrentDatabaseName) WITH NOWAIT;
                END CATCH;
            END;

            /* #38: Filtered index statistics -- detect filter_definition mismatch */
            IF @Debug = 1
            BEGIN
                DECLARE @filter_check_sql nvarchar(max) = N'
                    SELECT @cnt = COUNT(*)
                    FROM ' + QUOTENAME(@CurrentDatabaseName) + N'.sys.stats AS st
                    INNER JOIN ' + QUOTENAME(@CurrentDatabaseName) + N'.sys.indexes AS ix
                        ON ix.object_id = st.object_id AND ix.name = st.name
                    WHERE st.has_filter = 1
                    AND ix.has_filter = 1
                    AND st.filter_definition <> ix.filter_definition
                    AND EXISTS (
                        SELECT 1 FROM #stats_to_process AS stp
                        WHERE stp.database_name = @dbname COLLATE DATABASE_DEFAULT
                        AND stp.object_id = st.object_id
                        AND stp.stats_id = st.stats_id
                    )';
                DECLARE @filter_mismatch_count int = 0;
                BEGIN TRY
                    EXEC sp_executesql @filter_check_sql,
                        N'@dbname sysname, @cnt int OUTPUT',
                        @dbname = @CurrentDatabaseName, @cnt = @filter_mismatch_count OUTPUT;
                    IF @filter_mismatch_count > 0
                    BEGIN
                        DECLARE @filter_msg nvarchar(500) = N'    Filter Mismatch: ' + CONVERT(nvarchar(10), @filter_mismatch_count)
                            + N' stat(s) with stale filter_definition (differs from index filter)';
                        RAISERROR(@filter_msg, 10, 1) WITH NOWAIT;
                        SET @warnings += N'FILTER_MISMATCH: ' + @CurrentDatabaseName + N'(' + CONVERT(nvarchar(10), @filter_mismatch_count) + N'); ';
                    END;
                END TRY
                BEGIN CATCH
                    /* gh-468: surface rather than swallow */
                    SET @warnings += N'FILTER_CHECK_FAILED: ' + @CurrentDatabaseName + N' (' + ISNULL(ERROR_MESSAGE(), N'unknown') + N'); ';
                    IF @Debug = 1 RAISERROR(N'    Filter-mismatch check skipped: %s', 10, 1, @CurrentDatabaseName) WITH NOWAIT;
                END CATCH;
            END;

            /* #28/#149: Columnstore context detection -- always-on warning (promoted from debug-only) */
            BEGIN
                DECLARE @cs_check_sql nvarchar(max) = N'
                    SELECT @cnt = COUNT(DISTINCT stp.object_id)
                    FROM #stats_to_process AS stp
                    WHERE stp.database_name = @dbname COLLATE DATABASE_DEFAULT
                    AND EXISTS (
                        SELECT 1
                        FROM ' + QUOTENAME(@CurrentDatabaseName) + N'.sys.indexes AS csi
                        WHERE csi.object_id = stp.object_id
                        AND csi.type IN (5, 6) /* 5=NCCI, 6=CCI */
                    )';
                DECLARE @cs_table_count int = 0;
                BEGIN TRY
                    EXEC sp_executesql @cs_check_sql,
                        N'@dbname sysname, @cnt int OUTPUT',
                        @dbname = @CurrentDatabaseName, @cnt = @cs_table_count OUTPUT;
                    IF @cs_table_count > 0
                    BEGIN
                        DECLARE @cs_msg nvarchar(500) = N'    Columnstore: ' + CONVERT(nvarchar(10), @cs_table_count)
                            + N' table(s) with columnstore indexes -- modification_counter may underreport after bulk loads';
                        RAISERROR(@cs_msg, 10, 1) WITH NOWAIT;
                        SET @warnings += N'COLUMNSTORE_TABLES: ' + CONVERT(nvarchar(10), @cs_table_count) + N' table(s) with columnstore indexes; ';
                    END;
                END TRY
                BEGIN CATCH
                    /* gh-468: surface rather than swallow */
                    SET @warnings += N'COLUMNSTORE_CHECK_FAILED: ' + @CurrentDatabaseName + N' (' + ISNULL(ERROR_MESSAGE(), N'unknown') + N'); ';
                    IF @Debug = 1 RAISERROR(N'    Columnstore check skipped: %s', 10, 1, @CurrentDatabaseName) WITH NOWAIT;
                END CATCH;
            END;

            /* #156: Compressed tables page_count inflation warning */
            IF @i_min_page_count > 0
            BEGIN
                DECLARE @compressed_check_sql nvarchar(max) = N'
                    SELECT @cnt = COUNT(DISTINCT stp.object_id)
                    FROM #stats_to_process AS stp
                    INNER JOIN ' + QUOTENAME(@CurrentDatabaseName) + N'.sys.partitions AS p
                        ON p.object_id = stp.object_id AND p.index_id IN (0, 1)
                    WHERE stp.database_name = @dbname COLLATE DATABASE_DEFAULT
                    AND p.data_compression > 0';
                DECLARE @compressed_table_count int = 0;
                BEGIN TRY
                    EXEC sp_executesql @compressed_check_sql,
                        N'@dbname sysname, @cnt int OUTPUT',
                        @dbname = @CurrentDatabaseName, @cnt = @compressed_table_count OUTPUT;
                    IF @compressed_table_count > 0
                    BEGIN
                        DECLARE @comp_msg nvarchar(500) = N'    Compressed: ' + CONVERT(nvarchar(10), @compressed_table_count)
                            + N' table(s) -- @i_min_page_count uses compressed page counts (actual data may be larger)';
                        RAISERROR(@comp_msg, 10, 1) WITH NOWAIT;
                        SET @warnings += N'COMPRESSED_TABLES: ' + CONVERT(nvarchar(10), @compressed_table_count) + N' table(s) with compression -- @i_min_page_count uses compressed page counts; ';
                    END;
                END TRY
                BEGIN CATCH
                    /* gh-468: surface rather than swallow */
                    SET @warnings += N'COMPRESSED_CHECK_FAILED: ' + @CurrentDatabaseName + N' (' + ISNULL(ERROR_MESSAGE(), N'unknown') + N'); ';
                    IF @Debug = 1 RAISERROR(N'    Compressed-tables check skipped: %s', 10, 1, @CurrentDatabaseName) WITH NOWAIT;
                END CATCH;
            END;

            /* #30: Computed column statistics -- non-persisted computed columns have higher update cost */
            IF @Debug = 1
            BEGIN
                DECLARE @cc_check_sql nvarchar(max) = N'
                    SELECT @cnt = COUNT(*)
                    FROM #stats_to_process AS stp
                    INNER JOIN ' + QUOTENAME(@CurrentDatabaseName) + N'.sys.stats_columns AS sc
                        ON sc.object_id = stp.object_id AND sc.stats_id = stp.stats_id AND sc.stats_column_id = 1
                    INNER JOIN ' + QUOTENAME(@CurrentDatabaseName) + N'.sys.computed_columns AS cc
                        ON cc.object_id = sc.object_id AND cc.column_id = sc.column_id
                    WHERE stp.database_name = @dbname COLLATE DATABASE_DEFAULT
                    AND cc.is_persisted = 0';
                DECLARE @cc_count int = 0;
                BEGIN TRY
                    EXEC sp_executesql @cc_check_sql,
                        N'@dbname sysname, @cnt int OUTPUT',
                        @dbname = @CurrentDatabaseName, @cnt = @cc_count OUTPUT;
                    IF @cc_count > 0
                    BEGIN
                        DECLARE @cc_msg nvarchar(500) = N'    Computed Cols: ' + CONVERT(nvarchar(10), @cc_count)
                            + N' stat(s) on non-persisted computed columns -- higher scan cost';
                        RAISERROR(@cc_msg, 10, 1) WITH NOWAIT;
                    END;
                END TRY
                BEGIN CATCH
                    /* gh-468: surface rather than swallow */
                    SET @warnings += N'COMPUTEDCOL_CHECK_FAILED: ' + @CurrentDatabaseName + N' (' + ISNULL(ERROR_MESSAGE(), N'unknown') + N'); ';
                    IF @Debug = 1 RAISERROR(N'    Computed-columns check skipped: %s', 10, 1, @CurrentDatabaseName) WITH NOWAIT;
                END CATCH;
            END;

            END; /* gh-470: close IF @pdw_stats_in_db > 0 BEGIN */

            END TRY
            BEGIN CATCH
                DECLARE @db_skip_msg nvarchar(4000) =
                    N'  WARNING: Skipping database ' + QUOTENAME(@CurrentDatabaseName) +
                    N' - ' + ERROR_MESSAGE();
                RAISERROR(@db_skip_msg, 10, 1) WITH NOWAIT;
                /* #204: Add per-database skip to @WarningsOut */
                SET @warnings += N'DB_SKIPPED: ' + @CurrentDatabaseName + N'; ';
            END CATCH;

            /*
            Mark this database as completed.
            */
            UPDATE @tmpDatabases
            SET Completed = 1
            WHERE ID = @CurrentDatabaseID;

        END; /* End of WHILE database loop */

        /*#endregion 07E-POST-DB-WARNINGS */
        /*#region 07F-TABLES-AMBIGUITY: @Tables unqualified token multi-schema check */
        /*
        #209: @Tables unqualified token multi-schema ambiguity warning.
        When a token in @Tables contains no dot (no schema prefix), it matches ALL schemas
        that have a table with that name. If the token matches tables in more than one schema
        within any database in this run, emit a WARNING so the caller can use 'schema.table'
        format to be precise.

        This check does NOT change matching behavior -- all matched tables are still processed.
        It is purely diagnostic. A behavioral change (restrict to one schema) is a separate decision.

        The warning fires per (token, database) pair where multiple schemas are matched.
        Compatible with SQL 2016 (uses FOR XML PATH instead of STRING_AGG).
        */
        IF @Tables IS NOT NULL AND EXISTS (SELECT 1 FROM #stats_to_process)
        BEGIN
            DECLARE @ambig_warning nvarchar(max) = N'';

            SELECT @ambig_warning = @ambig_warning +
                N'TABLES_AMBIGUOUS: ''' + a.token + N''' matched '
                + CONVERT(nvarchar(10), a.schema_count) + N' schemas in [' + a.database_name + N'] ('
                + a.schema_list + N'). Use ''schema.table'' to target a specific table; '
            FROM (
                SELECT
                    token            = LTRIM(RTRIM(ss.value)),
                    database_name    = stp.database_name,
                    schema_count     = COUNT(DISTINCT stp.schema_name),
                    schema_list      = STUFF(
                                           (   SELECT N', ' + stp2.schema_name
                                               FROM #stats_to_process AS stp2
                                               WHERE stp2.table_name COLLATE DATABASE_DEFAULT
                                                     = LTRIM(RTRIM(ss.value)) COLLATE DATABASE_DEFAULT
                                               AND   stp2.database_name = stp.database_name
                                               GROUP BY stp2.schema_name
                                               ORDER BY stp2.schema_name
                                               FOR XML PATH(N''), TYPE
                                           ).value(N'.', N'nvarchar(max)')
                                           , 1, 2, N'')
                FROM STRING_SPLIT(@Tables, N',') AS ss
                INNER JOIN #stats_to_process AS stp
                    ON stp.table_name COLLATE DATABASE_DEFAULT
                       = LTRIM(RTRIM(ss.value)) COLLATE DATABASE_DEFAULT
                /* Unqualified token: no dot means no explicit schema prefix */
                WHERE CHARINDEX(N'.', LTRIM(RTRIM(ss.value))) = 0
                GROUP BY LTRIM(RTRIM(ss.value)), stp.database_name
                HAVING COUNT(DISTINCT stp.schema_name) > 1
            ) AS a;

            IF LEN(@ambig_warning) > 0
            BEGIN
                RAISERROR(N'WARNING: Unqualified @Tables token(s) matched multiple schemas -- all matched tables are included. Specify ''schema.table'' format to target a specific table. %s', 10, 1, @ambig_warning) WITH NOWAIT;
                SET @warnings += @ambig_warning;
            END;
        END;

    END; /* End of IF @mode = N'DISCOVERY' */
    /*#endregion 07F-TABLES-AMBIGUITY */
    /*#endregion 07-DISCOVERY */
    /*#region 08-POST-DISCOVERY: Stats report, early return, parallel queue */

    /*
    ============================================================================
    TEMPORAL HISTORY TABLE CO-SCHEDULING (#201)
    ============================================================================
    When a system-versioned temporal table (temporal_type=2) qualifies for update,
    also schedule its history table stats. Prevents cross-table cardinality
    inconsistency for FOR SYSTEM_TIME queries that join current + history tables.
    */
    IF  @mode = N'DISCOVERY'
    AND EXISTS (SELECT 1 FROM #stats_to_process WHERE temporal_type = 2)
    BEGIN
        /* #317: Converted from temporal_cursor to WHILE loop */
        DECLARE
            @temporal_db sysname,
            @temporal_sql nvarchar(max),
            @temporal_coscheduled int = 0,
            @temporal_db_idx int;

        DECLARE @temporal_db_list TABLE (idx int IDENTITY(1,1) PRIMARY KEY, database_name sysname NOT NULL);
        INSERT INTO @temporal_db_list (database_name) SELECT DISTINCT database_name FROM #stats_to_process WHERE temporal_type = 2;
        SELECT @temporal_db_idx = MIN(idx) FROM @temporal_db_list;

        WHILE @temporal_db_idx IS NOT NULL
        BEGIN
            SELECT @temporal_db = database_name FROM @temporal_db_list WHERE idx = @temporal_db_idx;
            /*
            For each database with temporal tables in the result set,
            find history table stats that aren't already scheduled.
            Uses sys.tables.history_table_id to link current → history.
            */
            SET @temporal_sql = N'
                SELECT
                    database_name = @db_param,
                    schema_name = OBJECT_SCHEMA_NAME(hs.object_id),
                    table_name = OBJECT_NAME(hs.object_id),
                    stat_name = hs.name,
                    object_id = hs.object_id,
                    stats_id = hs.stats_id,
                    no_recompute = hs.no_recompute,
                    is_incremental = hs.is_incremental,
                    is_memory_optimized = 0,
                    is_heap = CASE WHEN NOT EXISTS (
                        SELECT 1 FROM sys.indexes AS ix
                        WHERE ix.object_id = hs.object_id AND ix.index_id = 1
                    ) THEN 1 ELSE 0 END,
                    auto_created = hs.auto_created,
                    is_published = ISNULL(ht.is_published, 0),
                    is_tracked_by_cdc = ISNULL(ht.is_tracked_by_cdc, 0),
                    temporal_type = ISNULL(ht.temporal_type, 1),
                    modification_counter = ISNULL(sp.modification_counter, 0),
                    row_count = ISNULL(sp.rows, 0),
                    days_stale = ISNULL(DATEDIFF(DAY, sp.last_updated, SYSDATETIME()), 999),
                    page_count = ISNULL(
                        (SELECT SUM(ps.used_page_count)
                         FROM sys.dm_db_partition_stats AS ps
                         WHERE ps.object_id = hs.object_id AND ps.index_id IN (0, 1)), 0),
                    priority = CONVERT(bigint, 9000000000) + ROW_NUMBER() OVER (ORDER BY hs.object_id, hs.stats_id)
                FROM sys.stats AS hs
                JOIN sys.tables AS ht ON ht.object_id = hs.object_id
                CROSS APPLY sys.dm_db_stats_properties(hs.object_id, hs.stats_id) AS sp
                WHERE ht.temporal_type = 1
                AND   ht.object_id IN (
                    SELECT ct.history_table_id
                    FROM sys.tables AS ct
                    WHERE ct.temporal_type = 2
                    AND   ct.object_id IN (
                        SELECT DISTINCT stp.object_id
                        FROM #stats_to_process AS stp
                        WHERE stp.database_name = @db_param
                        AND   stp.temporal_type = 2
                    )
                )
                AND   NOT EXISTS (
                    SELECT 1
                    FROM #stats_to_process AS existing
                    WHERE existing.database_name = @db_param
                    AND   existing.object_id = hs.object_id
                    AND   existing.stats_id = hs.stats_id
                );';

            BEGIN TRY
                DECLARE @temporal_full_sql nvarchar(max) =
                    N'USE ' + QUOTENAME(@temporal_db) + N'; ' + @temporal_sql;

                INSERT INTO #stats_to_process
                (
                    database_name, schema_name, table_name, stat_name,
                    object_id, stats_id, no_recompute, is_incremental,
                    is_memory_optimized, is_heap, auto_created,
                    is_published, is_tracked_by_cdc, temporal_type,
                    modification_counter, row_count, days_stale, page_count,
                    priority
                )
                EXEC sys.sp_executesql
                    @temporal_full_sql,
                    N'@db_param sysname',
                    @db_param = @temporal_db;

                SET @temporal_coscheduled += @@ROWCOUNT;
            END TRY
            BEGIN CATCH
                IF @Debug = 1
                BEGIN
                    DECLARE @temporal_err nvarchar(500) = ERROR_MESSAGE();
                    RAISERROR(N'  Temporal co-schedule warning (%s): %s', 10, 1, @temporal_db, @temporal_err) WITH NOWAIT;
                END;
            END CATCH;

            SELECT @temporal_db_idx = MIN(idx) FROM @temporal_db_list WHERE idx > @temporal_db_idx;
        END;

        IF @temporal_coscheduled > 0
        BEGIN
            DECLARE @temporal_msg nvarchar(200);
            SET @temporal_msg = N'Temporal co-schedule: Added ' +
                CONVERT(nvarchar(10), @temporal_coscheduled) +
                N' history table stat(s) for consistency';
            RAISERROR(@temporal_msg, 10, 1) WITH NOWAIT;

            /* Assign priority to co-scheduled stats -- place them right after their parent tables */
            UPDATE stp
            SET stp.priority = parent.max_priority + 1
            FROM #stats_to_process AS stp
            CROSS APPLY (
                SELECT max_priority = MAX(p.priority)
                FROM #stats_to_process AS p
                WHERE p.database_name = stp.database_name
                AND   p.temporal_type = 2
            ) AS parent
            WHERE stp.priority = 0
            AND   stp.temporal_type = 1;
        END;
    END;

    /*
    ============================================================================
    REPORT DISCOVERED STATISTICS
    ============================================================================
    */
    IF @skip_discovery = 1
    BEGIN
        /* Worker skipped discovery -- get stats count from QueueStatistic */
        DECLARE @qs_total_stats integer = 0;
        SELECT
            @qs_total_stats = ISNULL(SUM(qs.StatsCount), 0)
        FROM dbo.QueueStatistic AS qs
        WHERE qs.QueueID = @queue_id
        AND   qs.TableEndTime IS NULL;

        RAISERROR(N'', 10, 1) WITH NOWAIT;
        RAISERROR(N'Queue has ~%d stats across unclaimed tables (skipped local discovery).', 10, 1, @qs_total_stats) WITH NOWAIT;
    END;

    DECLARE
        @total_stats integer =
        CASE WHEN @skip_discovery = 1
        THEN
        (
            SELECT ISNULL(SUM(qs.StatsCount), 0)
            FROM dbo.QueueStatistic AS qs
            WHERE qs.QueueID = @queue_id
            AND   qs.TableEndTime IS NULL
        )
        ELSE
        (
            SELECT
                COUNT_BIG(*)
            FROM #stats_to_process
        )
        END,
        @norecompute_stats integer = 0,
        @incremental_stats integer = 0,
        @heap_stats integer = 0,
        @memory_optimized_stats integer = 0,
        @persisted_sample_stats integer = 0;

    /* gh-463: single scan of #stats_to_process populates all five counters
       (was five separate COUNT_BIG full-scans). */
    SELECT
        @norecompute_stats       = SUM(CASE WHEN stp.no_recompute = 1 THEN 1 ELSE 0 END),
        @incremental_stats       = SUM(CASE WHEN stp.is_incremental = 1 THEN 1 ELSE 0 END),
        @heap_stats              = SUM(CASE WHEN stp.is_heap = 1 THEN 1 ELSE 0 END),
        @memory_optimized_stats  = SUM(CASE WHEN stp.is_memory_optimized = 1 THEN 1 ELSE 0 END),
        @persisted_sample_stats  = SUM(CASE WHEN stp.persisted_sample_percent IS NOT NULL THEN 1 ELSE 0 END)
    FROM #stats_to_process AS stp;

    IF @skip_discovery = 0
    BEGIN
        RAISERROR(N'', 10, 1) WITH NOWAIT;
        RAISERROR(N'Found %d qualifying statistics:', 10, 1, @total_stats) WITH NOWAIT;
        RAISERROR(N'  - NORECOMPUTE:      %d', 10, 1, @norecompute_stats) WITH NOWAIT;
        RAISERROR(N'  - Incremental:      %d', 10, 1, @incremental_stats) WITH NOWAIT;
        RAISERROR(N'  - On heaps:         %d', 10, 1, @heap_stats) WITH NOWAIT;
        RAISERROR(N'  - Memory-optimized: %d', 10, 1, @memory_optimized_stats) WITH NOWAIT;
        RAISERROR(N'  - Persisted sample: %d', 10, 1, @persisted_sample_stats) WITH NOWAIT;
    END;

    /* #147: CDC-tracked tables warning -- stats updates can delay CDC capture */
    DECLARE @cdc_stats int = (SELECT COUNT_BIG(*) FROM #stats_to_process WHERE is_tracked_by_cdc = 1);
    IF @cdc_stats > 0 AND @skip_discovery = 0
    BEGIN
        RAISERROR(N'  - CDC-tracked:      %d (FULLSCAN may delay CDC capture job)', 10, 1, @cdc_stats) WITH NOWAIT;
        SET @warnings += N'CDC_TABLES: ' + CONVERT(nvarchar(10), @cdc_stats) + N' stats on CDC-tracked tables; ';
    END;

    /* #192: Warn when replicated tables are a majority of candidates */
    DECLARE @replicated_stats int = (SELECT COUNT_BIG(*) FROM #stats_to_process WHERE is_published = 1);
    IF @replicated_stats > 0 AND @total_stats > 0 AND @skip_discovery = 0
    BEGIN
        DECLARE @repl_pct int = @replicated_stats * 100 / @total_stats;
        RAISERROR(N'  - Replicated:       %d (%d%%)', 10, 1, @replicated_stats, @repl_pct) WITH NOWAIT;
        IF @repl_pct > 50
        BEGIN
            RAISERROR(N'  WARNING: >50%% of candidates are on replicated tables. Statistics updates', 10, 1) WITH NOWAIT;
            RAISERROR(N'  generate log records that replicate to subscribers -- monitor replication lag.', 10, 1) WITH NOWAIT;
            SET @warnings += N'REPLICATION_MAJORITY: ' + CONVERT(nvarchar(10), @repl_pct) + N'%% of stats on replicated tables; ';
        END;
    END;

    RAISERROR(N'', 10, 1) WITH NOWAIT;

    IF @total_stats = 0
    BEGIN
        /*
        v2.26: Check if mop-up pass can run even with 0 priority stats.
        Mop-up discovers stats with modification_counter > 0 not updated in this run.
        */
        IF @i_mop_up_pass = N'Y'
        AND @LogToTable = N'Y'
        AND @i_time_limit IS NOT NULL
        AND @Execute = N'Y'
        AND @commandlog_exists = 1
        AND (DATEDIFF(SECOND, @start_time, SYSDATETIME()) + @i_mop_up_min_remaining) <= @i_time_limit
        BEGIN
            /* Mop-up is enabled and has time budget -- continue to mop-up discovery */
            RAISERROR(N'No priority statistics found. Checking mop-up pass...', 10, 1) WITH NOWAIT;
            SET @stop_reason = N'NATURAL_END'; /* Allow mop-up pass to trigger */
        END
        ELSE
        BEGIN
            /* Mop-up disabled or insufficient time -- exit */
            RAISERROR(N'No statistics qualify for update. Exiting.', 10, 1) WITH NOWAIT;

            /*
            Set OUTPUT parameters before early return
            */
            SELECT
                @StatsFoundOut = 0,
                @StatsProcessedOut = 0,
                @StatsSucceededOut = 0,
                @StatsFailedOut = 0,
                @StatsRemainingOut = 0,
                @DurationSecondsOut = DATEDIFF(SECOND, @start_time, SYSDATETIME()),
                @WarningsOut = NULLIF(@warnings, N''),
                @StopReasonOut = N'NO_QUALIFYING_STATS';

            /*
            Return summary result set even on early exit (0 qualifying stats).
            Without this, INSERT #Summary EXEC sp_StatUpdate gets no rows.
            */
            SELECT
                Status = N'SUCCESS',
                StatusMessage = N'No statistics qualify for update',
                StatsFound = 0,
                StatsProcessed = 0,
                StatsSucceeded = 0,
                StatsFailed = 0,
                StatsToctou = 0,
                StatsSkipped = 0,
                StatsRemaining = 0,
                DatabasesProcessed = @database_count,
                DurationSeconds = DATEDIFF(SECOND, @start_time, SYSDATETIME()),
                StopReason = CONVERT(nvarchar(50), N'NO_QUALIFYING_STATS'),
                RunLabel = @run_label,
                Version = @procedure_version;

            IF @StatsInParallel = N'N'
                DELETE FROM dbo.StatUpdateLock WHERE Resource = N'sp_StatUpdate' AND SessionID = @@SPID;
            RETURN 0;
        END;
    END;
    /*
    ============================================================================
    PARALLEL MODE: QUEUE INITIALIZATION
    ============================================================================

    When @StatsInParallel = 'Y':
    - First worker creates/claims Queue row and populates QueueStatistic
    - QueueStatistic stores ONE ROW PER TABLE (not per stat)
    - This ensures no two workers update stats on the same table concurrently
    - Dead workers detected via sys.dm_exec_sessions (sleeping workers between stats appear in dm_exec_sessions but not dm_exec_requests)

    Queue claim uses UPDLOCK, HOLDLOCK pattern from Ola Hallengren.
    */
    IF @StatsInParallel = N'Y'
    BEGIN
        RAISERROR(N'', 10, 1) WITH NOWAIT;
        RAISERROR(N'Parallel mode: Initializing work queue...', 10, 1) WITH NOWAIT;

        /* #430: probe heartbeat + login_time columns once before any parallel
           coordination code.  Both probes dominate every QueueStatistic touch-point
           (MAX_WORKERS count, dead-worker release, claim, per-stat heartbeat). */
        DECLARE @has_heartbeat_col bit = CASE
            WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.QueueStatistic') AND name = N'LastStatCompletedAt')
            THEN 1 ELSE 0 END;
        DECLARE @has_login_time_col bit = CASE
            WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.QueueStatistic') AND name = N'ClaimLoginTime')
            THEN 1 ELSE 0 END;

        /*
        v2.3: Backward-compatible migration -- add ParameterFingerprint to dbo.Queue if missing.
        Allows detecting conflicting parameter sets when multiple workers join the same queue.
        */
        IF  OBJECT_ID(N'dbo.Queue', N'U') IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.Queue') AND name = N'ParameterFingerprint')
        BEGIN
            BEGIN TRY
                ALTER TABLE dbo.Queue ADD ParameterFingerprint int NULL;
                IF @Debug = 1
                    RAISERROR(N'  Added ParameterFingerprint column to dbo.Queue (v2.3 migration).', 10, 1) WITH NOWAIT;
            END TRY
            BEGIN CATCH
                /* Non-blocking: if ALTER fails (e.g., permissions), proceed without fingerprint */
                IF @Debug = 1
                BEGIN
                    DECLARE @fp_alter_err nvarchar(500) = ERROR_MESSAGE();
                    RAISERROR(N'  Warning: Could not add ParameterFingerprint to dbo.Queue: %s', 10, 1, @fp_alter_err) WITH NOWAIT;
                END;
            END CATCH;
        END;

        /*
        v2.3: Compute parameter fingerprint for conflict detection.
        CHECKSUM of key threshold parameters that define what qualifies for update.
        If two workers compute different fingerprints, they have incompatible criteria.
        */
        DECLARE @parameter_fingerprint int = CHECKSUM(
            ISNULL(CONVERT(nvarchar(10), @i_tiered_thresholds), N'NULL'),
            ISNULL(CONVERT(nvarchar(20), @i_modification_threshold), N'NULL'),
            ISNULL(CONVERT(nvarchar(20), @i_modification_percent), N'NULL'),
            ISNULL(CONVERT(nvarchar(20), @StaleHours), N'NULL'),
            ISNULL(@i_qs_metric, N'NULL'),
            ISNULL(@i_mop_up_pass, N'NULL'),
            ISNULL(CONVERT(nvarchar(20), @i_mop_up_min_remaining), N'NULL'),
            ISNULL(CONVERT(nvarchar(20), @i_qs_top_plans), N'NULL'),
            /* gh-454: QS enrichment criteria affect priority ordering --
               workers with different values must not share a queue */
            ISNULL(CONVERT(nvarchar(20), @i_qs_min_executions), N'NULL'),
            ISNULL(CONVERT(nvarchar(20), @i_qs_recent_hours), N'NULL')
        );
        /* gh-461: @parameters_string is built unconditionally in region 07B now.
           By the time execution reaches here it is always populated. */
        BEGIN TRY
            /*
            Check if Queue row already exists for these parameters
            */
            SELECT
                @queue_id = q.QueueID
            FROM dbo.Queue AS q
            WHERE q.SchemaName = N'dbo'
            AND   q.ObjectName = N'sp_StatUpdate'
            AND   q.Parameters = @parameters_string;

            IF @queue_id IS NULL
            BEGIN
                /*
                No existing queue - this worker will create it.
                Use UPDLOCK, HOLDLOCK to prevent race condition.
                */
                BEGIN TRANSACTION;

                SELECT
                    @queue_id = q.QueueID
                FROM dbo.Queue AS q WITH (UPDLOCK, HOLDLOCK)
                WHERE q.SchemaName = N'dbo'
                AND   q.ObjectName = N'sp_StatUpdate'
                AND   q.Parameters = @parameters_string;

                IF @queue_id IS NULL
                BEGIN
                    INSERT INTO
                        dbo.Queue
                    (
                        SchemaName,
                        ObjectName,
                        Parameters
                    )
                    SELECT
                        SchemaName = N'dbo',
                        ObjectName = N'sp_StatUpdate',
                        Parameters = @parameters_string;

                    SET @queue_id = SCOPE_IDENTITY();

                    RAISERROR(N'  Created new queue (QueueID = %d)', 10, 1, @queue_id) WITH NOWAIT;

                    /* v2.3: Store parameter fingerprint for conflict detection */
                    /* Uses dynamic SQL to avoid compile-time column validation when column doesn't exist yet */
                    BEGIN TRY
                        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.Queue') AND name = N'ParameterFingerprint')
                        BEGIN
                            EXEC sp_executesql
                                N'UPDATE dbo.Queue SET ParameterFingerprint = @fp WHERE QueueID = @qid;',
                                N'@fp int, @qid int',
                                @fp = @parameter_fingerprint,
                                @qid = @queue_id;
                        END;
                    END TRY
                    BEGIN CATCH
                        /* Non-blocking: fingerprint is diagnostic, not critical */
                    END CATCH;
                END

                COMMIT TRANSACTION;

                /* v2.3: After transaction, check parameter fingerprint for conflict detection */
                /* (Done outside transaction to avoid leaving open txn on conflict RETURN) */
                /* Uses dynamic SQL to avoid compile-time column validation when column doesn't exist yet */
                BEGIN TRY
                    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.Queue') AND name = N'ParameterFingerprint')
                    BEGIN
                        DECLARE @stored_fingerprint int;
                        EXEC sp_executesql
                            N'SELECT @fp = ParameterFingerprint FROM dbo.Queue WHERE QueueID = @qid;',
                            N'@fp int OUTPUT, @qid int',
                            @fp = @stored_fingerprint OUTPUT,
                            @qid = @queue_id;

                        IF  @stored_fingerprint IS NOT NULL
                        AND @stored_fingerprint <> @parameter_fingerprint
                        BEGIN
                            RAISERROR(N'Conflicting sp_StatUpdate parameters detected. Worker cannot join existing queue initialized with different threshold parameters. (Stored fingerprint: %d, This worker: %d)', 16, 1,
                                @stored_fingerprint, @parameter_fingerprint);
                            /* gh-451: populate OUTPUT params + summary result set on early return */
                            SET @StopReasonOut = N'FINGERPRINT_CONFLICT';
                            SET @StatsFoundOut = 0;
                            SET @StatsProcessedOut = 0;
                            SET @StatsSucceededOut = 0;
                            SET @StatsFailedOut = 0;
                            SET @StatsRemainingOut = 0;
                            SET @DurationSecondsOut = DATEDIFF(SECOND, @start_time, SYSDATETIME());
                            SELECT
                                Status = N'ERROR',
                                StatusMessage = N'Conflicting parameter fingerprint on existing queue.',
                                StatsFound = 0, StatsProcessed = 0, StatsSucceeded = 0, StatsFailed = 0,
                                StatsToctou = 0, StatsSkipped = 0, StatsRemaining = 0,
                                DatabasesProcessed = 0,
                                DurationSeconds = DATEDIFF(SECOND, @start_time, SYSDATETIME()),
                                StopReason = N'FINGERPRINT_CONFLICT', RunLabel = @run_label,
                                Version = @procedure_version;
                            IF @StatsInParallel = N'N'
                                DELETE FROM dbo.StatUpdateLock WHERE Resource = N'sp_StatUpdate' AND SessionID = @@SPID;
                            RETURN 50001;
                        END;
                    END;
                END TRY
                BEGIN CATCH
                    /* Non-blocking: if fingerprint check fails, proceed to join the queue */
                END CATCH;
            END;

            /*
            #181: Max worker count coordination.
            Before claiming, count active workers (sessions still alive in dm_exec_sessions).
            If >= @i_max_workers, exit cleanly without joining the queue.
            bd -h9a: when ClaimLoginTime exists, a reused SPID only counts as live
            when BOTH session_id AND login_time match.  Otherwise a reused SPID
            would inflate the count and trigger a false MAX_WORKERS exit.
            */
            IF @i_max_workers IS NOT NULL AND @queue_id IS NOT NULL
            BEGIN
                DECLARE @active_worker_count int = 0;
                DECLARE @mw_sql nvarchar(max) = N'
                SELECT @cnt = COUNT(DISTINCT qs.SessionID)
                FROM dbo.QueueStatistic AS qs
                JOIN sys.dm_exec_sessions AS ses
                  ON ses.session_id = qs.SessionID'
                    + CASE WHEN @has_login_time_col = 1
                           THEN N'
                 AND (qs.ClaimLoginTime IS NULL OR ses.login_time = qs.ClaimLoginTime)'
                           ELSE N'' END + N'
                WHERE qs.QueueID = @qid
                AND   qs.TableEndTime IS NULL;';

                EXEC sys.sp_executesql
                    @mw_sql,
                    N'@qid int, @cnt int OUTPUT',
                    @qid = @queue_id,
                    @cnt = @active_worker_count OUTPUT;

                IF @active_worker_count >= @i_max_workers
                BEGIN
                    DECLARE @mw_msg nvarchar(500);
                    SET @mw_msg = N'Max parallel workers reached (' +
                        CONVERT(nvarchar(10), @active_worker_count) + N' active, limit ' +
                        CONVERT(nvarchar(10), @i_max_workers) + N'). Exiting cleanly.';
                    RAISERROR(@mw_msg, 10, 1) WITH NOWAIT;

                    /* gh-451: populate OUTPUT params + summary result set on early return */
                    SET @StopReasonOut = N'MAX_WORKERS';
                    SET @StatsFoundOut = 0;
                    SET @StatsProcessedOut = 0;
                    SET @StatsSucceededOut = 0;
                    SET @StatsFailedOut = 0;
                    SET @StatsRemainingOut = 0;
                    SET @DurationSecondsOut = DATEDIFF(SECOND, @start_time, SYSDATETIME());
                    SELECT
                        Status = N'SUCCESS',
                        StatusMessage = @mw_msg,
                        StatsFound = 0, StatsProcessed = 0, StatsSucceeded = 0, StatsFailed = 0,
                        StatsToctou = 0, StatsSkipped = 0, StatsRemaining = 0,
                        DatabasesProcessed = 0,
                        DurationSeconds = DATEDIFF(SECOND, @start_time, SYSDATETIME()),
                        StopReason = N'MAX_WORKERS', RunLabel = @run_label,
                        Version = @procedure_version;
                    IF @StatsInParallel = N'N'
                        DELETE FROM dbo.StatUpdateLock WHERE Resource = N'sp_StatUpdate' AND SessionID = @@SPID;
                    RETURN 0;
                END;

                IF @Debug = 1
                BEGIN
                    DECLARE @mw_debug nvarchar(200);
                    SET @mw_debug = N'  Active workers: ' + CONVERT(nvarchar(10), @active_worker_count) +
                        N' / ' + CONVERT(nvarchar(10), @i_max_workers) + N' max';
                    RAISERROR(@mw_debug, 10, 1) WITH NOWAIT;
                END;
            END;

            /*
            Attempt to claim the queue (become the first/lead worker).
            Only succeeds if no other worker is currently leading.
            */
            BEGIN TRANSACTION;

            UPDATE
                q
            SET
                q.QueueStartTime = SYSDATETIME(),
                q.SessionID = @@SPID,
                q.RequestID =
                (
                    SELECT
                        r.request_id
                    FROM sys.dm_exec_requests AS r
                    WHERE r.session_id = @@SPID
                ),
                q.RequestStartTime =
                (
                    SELECT
                        r.start_time
                    FROM sys.dm_exec_requests AS r
                    WHERE r.session_id = @@SPID
                )
            FROM dbo.Queue AS q
            WHERE q.QueueID = @queue_id
            /*
            Only claim if previous leader is dead (not in dm_exec_sessions).
            Using dm_exec_sessions: a sleeping leader appears in dm_exec_sessions but not dm_exec_requests.
            */
            AND   NOT EXISTS
                  (
                      SELECT
                          1
                      FROM sys.dm_exec_sessions AS s
                      WHERE s.session_id = q.SessionID
                  )
            /*
            And no active workers in QueueStatistic (sessions still alive in dm_exec_sessions)
            */
            AND   NOT EXISTS
                  (
                      SELECT
                          1
                      FROM dbo.QueueStatistic AS qs
                      JOIN sys.dm_exec_sessions AS s
                        ON s.session_id = qs.SessionID
                      WHERE qs.QueueID = @queue_id
                  );

            IF ROWCOUNT_BIG() = 1
            BEGIN
                /*
                Successfully claimed queue - populate QueueStatistic with tables.
                Aggregate stats by table, using max modification_counter for priority.
                */
                RAISERROR(N'  Claimed queue leadership - populating work items...', 10, 1) WITH NOWAIT;

                /*
                Clear any stale entries from previous (failed/killed) runs
                */
                DELETE FROM
                    dbo.QueueStatistic
                WHERE QueueID = @queue_id;

                /*
                Insert one row per table needing stats updates.
                Tables ordered by max modification_counter (most stale first).
                */
                INSERT INTO
                    dbo.QueueStatistic
                (
                    QueueID,
                    DatabaseName,
                    SchemaName,
                    ObjectName,
                    ObjectID,
                    TablePriority,
                    StatsCount,
                    MaxModificationCounter
                )
                SELECT
                    QueueID = @queue_id,
                    DatabaseName = stp.database_name,
                    SchemaName = stp.schema_name,
                    ObjectName = stp.table_name,
                    ObjectID = stp.object_id,
                    TablePriority = ROW_NUMBER() OVER (
                        ORDER BY MIN(stp.priority) ASC
                    ),
                    StatsCount = COUNT_BIG(*),
                    MaxModificationCounter = MAX(stp.modification_counter)
                FROM #stats_to_process AS stp
                GROUP BY
                    stp.database_name,
                    stp.schema_name,
                    stp.table_name,
                    stp.object_id;

                DECLARE
                    @Tables_queued integer = ROWCOUNT_BIG();

                RAISERROR(N'  Queued %d tables for processing', 10, 1, @Tables_queued) WITH NOWAIT;
            END;
            ELSE
            BEGIN
                RAISERROR(N'  Joined existing queue (QueueID = %d)', 10, 1, @queue_id) WITH NOWAIT;
            END;

            COMMIT TRANSACTION;

            /*
            Get queue start time for this run
            */
            SELECT
                @queue_start_time = q.QueueStartTime
            FROM dbo.Queue AS q
            WHERE q.QueueID = @queue_id;

        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0
            BEGIN
                ROLLBACK TRANSACTION;
            END;

            DECLARE
                @queue_error_message nvarchar(4000) = ERROR_MESSAGE();

            RAISERROR(N'ERROR: Queue initialization failed: %s', 16, 1, @queue_error_message) WITH NOWAIT;
            /* gh-451: populate OUTPUT params + summary result set on early return */
            SET @StopReasonOut = N'QUEUE_INIT_ERROR';
            SET @StatsFoundOut = 0;
            SET @StatsProcessedOut = 0;
            SET @StatsSucceededOut = 0;
            SET @StatsFailedOut = 0;
            SET @StatsRemainingOut = 0;
            SET @DurationSecondsOut = DATEDIFF(SECOND, @start_time, SYSDATETIME());
            SELECT
                Status = N'ERROR',
                StatusMessage = N'Queue initialization failed: ' + ISNULL(@queue_error_message, N'(no message)'),
                StatsFound = 0, StatsProcessed = 0, StatsSucceeded = 0, StatsFailed = 0,
                StatsToctou = 0, StatsSkipped = 0, StatsRemaining = 0,
                DatabasesProcessed = 0,
                DurationSeconds = DATEDIFF(SECOND, @start_time, SYSDATETIME()),
                StopReason = N'QUEUE_INIT_ERROR', RunLabel = @run_label,
                Version = @procedure_version;
            IF @StatsInParallel = N'N'
                DELETE FROM dbo.StatUpdateLock WHERE Resource = N'sp_StatUpdate' AND SessionID = @@SPID;
            RETURN -1;
        END CATCH;

        RAISERROR(N'', 10, 1) WITH NOWAIT;
    END;

    /*
    ============================================================================
    SHOW TOP CANDIDATES (if not executing or debug mode)
    ============================================================================
    */
    IF @Execute = N'N'
    OR @Debug = 1
    BEGIN
        RAISERROR(N'Top 20 candidates by priority:', 10, 1) WITH NOWAIT;
        RAISERROR(N'', 10, 1) WITH NOWAIT;

        SELECT TOP (20)
            priority = stp.priority,
            table_path = stp.schema_name + N'.' + stp.table_name,
            stat_name = stp.stat_name,
            norecompute =
                CASE stp.no_recompute
                    WHEN 1
                    THEN N'YES'
                    ELSE N'no'
                END,
            incremental =
                CASE stp.is_incremental
                    WHEN 1
                    THEN N'YES'
                    ELSE N'no'
                END,
            is_heap =
                CASE stp.is_heap
                    WHEN 1
                    THEN N'HEAP'
                    ELSE N''
                END,
            modifications = CONVERT(nvarchar(20), stp.modification_counter),
            days_stale = stp.days_stale,
            pages = CONVERT(nvarchar(20), stp.page_count)
        FROM #stats_to_process AS stp
        ORDER BY
            stp.priority;
    END;
    /*#endregion 08-POST-DISCOVERY */
    /*#region 09-PROCESS-LOOP: Main processing loop */
    /*#region 09A-LOOP-HEADER: Banner, ProcessLoop label, time limit check, batch limit check */
    /*
    ============================================================================
    PROCESS STATISTICS
    ============================================================================
    */
    RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
    RAISERROR(N' Processing Statistics', 10, 1) WITH NOWAIT;
    RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
    RAISERROR(N'', 10, 1) WITH NOWAIT;

    /*
    Processing loop
    */
    /* #430: @has_heartbeat_col + bd -h9a: @has_login_time_col both probed once
       earlier at the @StatsInParallel block entry.  Declarations here would
       conflict; those probes dominate this point. */

    /* gh-467: Per-table sys.partitions count cache for incremental stats.
       Avoids recompiling + rescanning sys.partitions once per stat when
       multiple incremental stats live on the same table. */
    DECLARE
        @last_partition_db sysname = N'',
        @last_partition_object_id int = NULL,
        @last_partition_count int = NULL;

    ProcessLoop:
    WHILE 1 = 1
    BEGIN
        /*
        Capture iteration time once for consistent elapsed calculations
        */
        SELECT @iteration_time = SYSDATETIME();

        /*
        Check time limit
        */
        IF  @i_time_limit IS NOT NULL
        AND DATEDIFF(SECOND, @start_time, SYSDATETIME()) >= @i_time_limit
        BEGIN
            RAISERROR(N'', 10, 1) WITH NOWAIT;
            RAISERROR(N'Time limit (%d seconds) reached. Stopping gracefully.', 10, 1, @i_time_limit) WITH NOWAIT;
            SELECT
                @stop_reason = N'TIME_LIMIT';
            BREAK;
        END;

        /*
        Check batch limit
        */
        IF  @BatchLimit IS NOT NULL
        AND @stats_processed >= @BatchLimit
        BEGIN
            RAISERROR(N'', 10, 1) WITH NOWAIT;
            RAISERROR(N'Batch limit (%d stats) reached. Stopping gracefully.', 10, 1, @BatchLimit) WITH NOWAIT;
            SELECT
                @stop_reason = N'BATCH_LIMIT';
            BREAK;
        END;

        /*#endregion 09A-LOOP-HEADER */
        /*#region 09B-SAFETY-CHECKS: Backup detection, AG redo queue wait, tempdb pressure, log space */
        /*
        ====================================================================
        AG REDO QUEUE CHECK (v2.7, #18)
        ====================================================================
        */

        /* #146: Periodic backup detection -- check every 50 stats for new backup activity */
        IF @stats_processed > 0 AND @stats_processed % 50 = 0
        BEGIN
            DECLARE @mid_run_backups int = 0;
            SELECT @mid_run_backups = COUNT(*)
            FROM sys.dm_exec_requests AS r
            WHERE r.command LIKE N'BACKUP%';

            IF @mid_run_backups > 0 AND @active_backups = 0
            BEGIN
                /* Backup started after our run began */
                RAISERROR(N'  Note: %d backup(s) now running (started after sp_StatUpdate). I/O contention possible.', 10, 1, @mid_run_backups) WITH NOWAIT;
                SET @warnings += N'BACKUP_STARTED_MID_RUN: ' + CONVERT(nvarchar(10), @mid_run_backups) + N' backup(s); ';
            END;
        END;

        /*
        Before each stat update, check if AG secondary redo queue exceeds
        threshold. If over threshold, enter a wait loop. If wait exceeds
        @i_max_ag_wait_minutes or @i_time_limit is reached during wait, stop the run.
        */
        IF  @i_max_ag_redo_queue_mb IS NOT NULL
        AND @ag_is_primary = 1
        AND @Execute = N'Y'
        BEGIN
            /* #344: Only check SYNCHRONOUS_COMMIT secondaries -- async replicas naturally lag */
            SELECT @ag_redo_queue_mb = MAX(drs.redo_queue_size) / 1024.0
            FROM sys.dm_hadr_database_replica_states AS drs
            INNER JOIN sys.availability_replicas AS ar
                ON ar.replica_id = drs.replica_id
            WHERE drs.is_local = 0
            AND   drs.is_primary_replica = 0
            AND   drs.redo_queue_size IS NOT NULL
            AND   ar.availability_mode = 1; /* SYNCHRONOUS_COMMIT */

            IF @ag_redo_queue_mb IS NOT NULL AND @ag_redo_queue_mb > @i_max_ag_redo_queue_mb
            BEGIN
                SET @ag_wait_start = SYSDATETIME();

                WHILE @ag_redo_queue_mb > @i_max_ag_redo_queue_mb
                BEGIN
                    /* Check AG wait timeout */
                    IF DATEDIFF(MINUTE, @ag_wait_start, SYSDATETIME()) >= @i_max_ag_wait_minutes
                    BEGIN
                        SELECT @stop_reason = N'AG_REDO_QUEUE';
                        RAISERROR(N'', 10, 1) WITH NOWAIT;
                        RAISERROR(N'AG redo queue exceeded %d MB for %d minutes. Stopping to protect secondaries.', 10, 1,
                            @i_max_ag_redo_queue_mb, @i_max_ag_wait_minutes) WITH NOWAIT;
                        SET @WarningsOut = ISNULL(@WarningsOut, N'') +
                            N'AG_REDO_QUEUE: Stopped after waiting ' +
                            CONVERT(nvarchar(10), @i_max_ag_wait_minutes) + N' min; ';
                        BREAK;
                    END;

                    /* Check time limit during wait */
                    IF  @i_time_limit IS NOT NULL
                    AND DATEDIFF(SECOND, @start_time, SYSDATETIME()) >= @i_time_limit
                    BEGIN
                        SELECT @stop_reason = N'TIME_LIMIT';
                        RAISERROR(N'', 10, 1) WITH NOWAIT;
                        RAISERROR(N'Time limit reached while waiting for AG redo queue to drain.', 10, 1) WITH NOWAIT;
                        BREAK;
                    END;

                    SET @ag_wait_msg =
                        N'  AG redo queue: ' + CONVERT(nvarchar(20), @ag_redo_queue_mb) +
                        N' MB (threshold: ' + CONVERT(nvarchar(20), @i_max_ag_redo_queue_mb) +
                        N' MB) -- waiting 30s...';
                    RAISERROR(@ag_wait_msg, 10, 1) WITH NOWAIT;

                    WAITFOR DELAY '00:00:30';

                    /* #141: Re-check time limit after WAITFOR to prevent overshoot */
                    IF  @i_time_limit IS NOT NULL
                    AND DATEDIFF(SECOND, @start_time, SYSDATETIME()) >= @i_time_limit
                    BEGIN
                        SELECT @stop_reason = N'TIME_LIMIT';
                        RAISERROR(N'Time limit reached during AG redo queue wait.', 10, 1) WITH NOWAIT;
                        BREAK;
                    END;

                    /* Re-check redo queue (sync replicas only) */
                    SELECT @ag_redo_queue_mb = MAX(drs.redo_queue_size) / 1024.0
                    FROM sys.dm_hadr_database_replica_states AS drs
                    INNER JOIN sys.availability_replicas AS ar
                        ON ar.replica_id = drs.replica_id
                    WHERE drs.is_local = 0
                    AND   drs.is_primary_replica = 0
                    AND   drs.redo_queue_size IS NOT NULL
                    AND   ar.availability_mode = 1; /* SYNCHRONOUS_COMMIT */

                    /* If secondaries went offline during wait, break out */
                    IF @ag_redo_queue_mb IS NULL
                    BEGIN
                        RAISERROR(N'  AG secondaries went offline during wait. Resuming.', 10, 1) WITH NOWAIT;
                        BREAK;
                    END;
                END;

                /* If stop_reason was set inside the wait loop, exit main loop */
                IF @stop_reason IS NOT NULL
                    BREAK;

                /* Redo queue drained -- log recovery */
                IF @ag_redo_queue_mb IS NOT NULL AND @ag_redo_queue_mb <= @i_max_ag_redo_queue_mb
                BEGIN
                    SET @ag_recovered_msg =
                        N'  AG redo queue drained to ' + CONVERT(nvarchar(20), @ag_redo_queue_mb) +
                        N' MB (threshold: ' + CONVERT(nvarchar(20), @i_max_ag_redo_queue_mb) +
                        N' MB). Resuming.';
                    RAISERROR(@ag_recovered_msg, 10, 1) WITH NOWAIT;
                END;
            END;
        END;

        /*
        ====================================================================
        TEMPDB PRESSURE CHECK (v2.7, #34)
        ====================================================================
        Before each stat update, check tempdb free space. Particularly
        important for FULLSCAN operations and Azure SQL.
        */
        IF  @i_min_tempdb_free_mb IS NOT NULL
        AND @Execute = N'Y'
        BEGIN
            SELECT @tempdb_free_mb =
                SUM(unallocated_extent_page_count) * 8 / 1024.0
            FROM tempdb.sys.dm_db_file_space_usage;

            IF @tempdb_free_mb < @i_min_tempdb_free_mb
            BEGIN
                SET @tempdb_msg =
                    N'  Tempdb free space: ' + CONVERT(nvarchar(20), @tempdb_free_mb) +
                    N' MB (threshold: ' + CONVERT(nvarchar(20), @i_min_tempdb_free_mb) +
                    N' MB)';

                IF @FailFast = 1
                BEGIN
                    RAISERROR(N'', 10, 1) WITH NOWAIT;
                    RAISERROR(@tempdb_msg, 10, 1) WITH NOWAIT;
                    RAISERROR(N'Tempdb pressure detected with @FailFast=1. Stopping.', 10, 1) WITH NOWAIT;
                    SELECT @stop_reason = N'TEMPDB_PRESSURE';
                    SET @WarningsOut = ISNULL(@WarningsOut, N'') +
                        N'TEMPDB_PRESSURE: Stopped, only ' +
                        CONVERT(nvarchar(20), @tempdb_free_mb) + N' MB free; ';
                    BREAK;
                END;
                ELSE
                BEGIN
                    RAISERROR(@tempdb_msg, 10, 1) WITH NOWAIT;
                    RAISERROR(N'  WARNING: Tempdb below threshold. Continuing (use @FailFast=1 to abort).', 10, 1) WITH NOWAIT;
                    SET @WarningsOut = ISNULL(@WarningsOut, N'') +
                        N'TEMPDB_LOW: ' + CONVERT(nvarchar(20), @tempdb_free_mb) +
                        N' MB free (threshold: ' + CONVERT(nvarchar(20), @i_min_tempdb_free_mb) + N' MB); ';
                END;
            END;
        END;

        /*
        ====================================================================
        TRANSACTION LOG SPACE CHECK (#61)
        ====================================================================
        For FULLSCAN operations on FULL recovery model databases, UPDATE
        STATISTICS can generate significant transaction log growth. Check
        per-database log usage before each stat update when using FULLSCAN.
        Only checks once per iteration; uses the current stat's database.
        */
        IF  @Execute = N'Y'
        AND @i_statistics_sample = 100
        BEGIN
            DECLARE @log_check_db sysname;
            DECLARE @log_used_pct float = 0;
            DECLARE @last_log_check_db sysname; /* bd -xdx: skip when same db as last check */

            /* Get the next stat's database */
            SELECT TOP (1) @log_check_db = stp.database_name
            FROM #stats_to_process AS stp
            WHERE stp.processed = 0
            ORDER BY stp.priority;

            /* bd -xdx: only re-check log space when the database changes.
               On a single-database run with 2500 stats, saves 2499 sp_executesql calls. */
            IF @log_check_db IS NOT NULL
            AND @log_check_db <> ISNULL(@last_log_check_db, N'')
            BEGIN
                SET @last_log_check_db = @log_check_db;
                DECLARE @log_sql nvarchar(500) = N'
                    SELECT @pct = used_log_space_in_percent
                    FROM ' + QUOTENAME(@log_check_db) + N'.sys.dm_db_log_space_usage';
                /* gh-457: reset per-iteration -- DECLARE initializer runs once at
                   proc entry, so a throw in sp_executesql leaves the prior db's
                   value behind, potentially triggering false LOG_SPACE_HIGH on
                   the current db. */
                SET @log_used_pct = 0;
                BEGIN TRY
                    EXEC sp_executesql @log_sql,
                        N'@pct float OUTPUT', @pct = @log_used_pct OUTPUT;

                    IF @log_used_pct > 90.0
                    BEGIN
                        DECLARE @log_msg nvarchar(500) = N'  WARNING: Transaction log ' + @log_check_db
                            + N' is ' + CONVERT(nvarchar(10), CONVERT(int, @log_used_pct))
                            + N'% full. FULLSCAN may cause log growth.';
                        RAISERROR(@log_msg, 10, 1) WITH NOWAIT;
                        SET @warnings += N'LOG_SPACE_HIGH: ' + @log_check_db + N'('
                            + CONVERT(nvarchar(10), CONVERT(int, @log_used_pct)) + N'%); ';

                        /* @FailFast integration: abort when log pressure is critical */
                        IF @FailFast = 1
                        BEGIN
                            RAISERROR(N'Transaction log pressure detected with @FailFast=1. Stopping.', 10, 1) WITH NOWAIT;
                            SELECT @stop_reason = N'LOG_SPACE_HIGH';
                            SET @WarningsOut = ISNULL(@WarningsOut, N'') +
                                N'LOG_SPACE_HIGH: Stopped, log ' + @log_check_db +
                                N' at ' + CONVERT(nvarchar(10), CONVERT(int, @log_used_pct)) + N'%; ';
                            BREAK;
                        END;
                    END;
                END TRY
                BEGIN CATCH /* Ignore - db may not be accessible */ END CATCH;
            END;
        END;

        /*#endregion 09B-SAFETY-CHECKS */
        /*#region 09C-PARALLEL-CLAIM: Dead worker release, atomic table claim, lazy mop-up discovery */
        /*
        ====================================================================
        PARALLEL MODE: Claim table from QueueStatistic
        ====================================================================
        In parallel mode, we must claim a TABLE before processing its stats.
        This ensures no two workers update stats on the same table concurrently.
        */
        IF @StatsInParallel = N'Y'
        BEGIN
            /*
            Check if we need to claim a new table (no table claimed, or finished previous)
            */
            IF @claimed_table_database IS NULL
            BEGIN
                /*
                First, release any tables claimed by dead workers.
                A worker is dead if its session is no longer in dm_exec_sessions.
                Using dm_exec_sessions (not dm_exec_requests) because workers sleeping
                between stat claims are not in dm_exec_requests but ARE in dm_exec_sessions.
                */
                /* v2.3: LastStatCompletedAt column may not exist on pre-v2.3 installations.
                   bd -h9a: ClaimLoginTime column may not exist on pre-fix installations.
                   Use dynamic SQL to avoid compile-time column validation failure. */
                DECLARE @dead_worker_sql nvarchar(max);
                /* #430: @has_heartbeat_col moved outside process loop (one-time evaluation) */

                SET @dead_worker_sql = N'
                UPDATE qs
                SET    qs.TableStartTime = NULL,
                       qs.SessionID = NULL,
                       qs.RequestID = NULL,
                       qs.RequestStartTime = NULL'
                    + CASE WHEN @has_heartbeat_col = 1
                           THEN N',
                       qs.LastStatCompletedAt = NULL'
                           ELSE N'' END
                    + CASE WHEN @has_login_time_col = 1
                           THEN N',
                       qs.ClaimLoginTime = NULL'
                           ELSE N'' END + N'
                FROM dbo.QueueStatistic AS qs
                WHERE qs.QueueID = @queue_id
                AND   qs.TableStartTime IS NOT NULL
                AND   qs.TableEndTime IS NULL
                AND   (
                          NOT EXISTS
                          (
                              SELECT 1
                              FROM sys.dm_exec_sessions AS s
                              WHERE s.session_id = qs.SessionID'
                    + CASE WHEN @has_login_time_col = 1
                           THEN N'
                                AND (qs.ClaimLoginTime IS NULL OR s.login_time = qs.ClaimLoginTime)'
                           ELSE N'' END + N'
                          )
                          OR
                          (
                              @i_dead_worker_timeout_min IS NOT NULL
                              AND DATEDIFF(MINUTE, '
                    + CASE WHEN @has_heartbeat_col = 1
                           THEN N'COALESCE(qs.LastStatCompletedAt, qs.TableStartTime)'
                           ELSE N'qs.TableStartTime' END + N',
                                  SYSDATETIME()) > @i_dead_worker_timeout_min
                          )
                      );';

                EXEC sp_executesql
                    @dead_worker_sql,
                    N'@queue_id int, @i_dead_worker_timeout_min int',
                    @queue_id = @queue_id,
                    @i_dead_worker_timeout_min = @i_dead_worker_timeout_min;

                DECLARE
                    @released_count bigint = ROWCOUNT_BIG();

                IF @released_count > 0
                AND @Debug = 1
                BEGIN
                    DECLARE @released_count_int int = @released_count;
                    RAISERROR(N'  Released %d tables from dead workers', 10, 1, @released_count_int) WITH NOWAIT;
                END;

                /*
                Claim the next unclaimed table (highest priority first).
                Uses UPDATE with OUTPUT to atomically claim and return the table.
                */
                DECLARE
                    @claimed_tables TABLE
                    (
                        DatabaseName sysname,
                        SchemaName sysname,
                        ObjectName sysname,
                        ObjectID integer
                    );

                /*
                Clear table variable from previous iterations.
                DECLARE does not reset table variables inside loops - they persist
                and OUTPUT INTO appends rather than replaces. Without this DELETE,
                SELECT TOP 1 could return stale data from a prior claim attempt.
                */
                DELETE FROM @claimed_tables;

                /*
                ROWLOCK+READPAST hints (v1.9 #25 + v2.3): Skip locked rows instead of waiting.
                ROWLOCK ensures row-level lock granularity (prevents escalation to table locks
                on large queue tables). READPAST skips rows locked by other concurrent workers.
                When multiple workers claim tables concurrently, this prevents blocking.
                */
                /* bd -h9a: snapshot login_time for this session once, pass into UPDATE.
                   Pairs with SessionID in the dead-worker check so a reused SPID (post-
                   failover or post-restart) cannot match a stale ClaimLoginTime.

                   Claim UPDATE is dynamic SQL so the ClaimLoginTime column reference
                   is only parsed when the column actually exists (gated on
                   @has_login_time_col).  Pre-h9a installations skip the ClaimLoginTime
                   assignment entirely and fall back to the original session-id-only
                   dead-worker detection. */
                DECLARE @claim_login_time datetime =
                    (SELECT s.login_time FROM sys.dm_exec_sessions AS s WHERE s.session_id = @@SPID);

                DECLARE @claim_sql nvarchar(max) = N'
                UPDATE qs
                SET    qs.TableStartTime = SYSDATETIME(),
                       qs.SessionID = @@SPID'
                    + CASE WHEN @has_login_time_col = 1
                           THEN N',
                       qs.ClaimLoginTime = @login_time_in'
                           ELSE N'' END + N',
                       qs.RequestID =
                       (
                           SELECT r.request_id
                           FROM sys.dm_exec_requests AS r
                           WHERE r.session_id = @@SPID
                       ),
                       qs.RequestStartTime =
                       (
                           SELECT r.start_time
                           FROM sys.dm_exec_requests AS r
                           WHERE r.session_id = @@SPID
                       )
                OUTPUT
                    inserted.DatabaseName,
                    inserted.SchemaName,
                    inserted.ObjectName,
                    inserted.ObjectID
                FROM dbo.QueueStatistic AS qs WITH (ROWLOCK, READPAST)
                WHERE qs.QueueID = @qid
                AND   qs.TableStartTime IS NULL
                AND   qs.TablePriority =
                      (
                          SELECT MIN(qs2.TablePriority)
                          FROM dbo.QueueStatistic AS qs2 WITH (ROWLOCK, READPAST)
                          WHERE qs2.QueueID = @qid
                          AND   qs2.TableStartTime IS NULL
                      );';

                INSERT INTO @claimed_tables (DatabaseName, SchemaName, ObjectName, ObjectID)
                EXEC sys.sp_executesql
                    @claim_sql,
                    N'@qid int, @login_time_in datetime',
                    @qid = @queue_id,
                    @login_time_in = @claim_login_time;

                SELECT TOP (1)
                    @claimed_table_database = ct.DatabaseName,
                    @claimed_table_schema = ct.SchemaName,
                    @claimed_table_name = ct.ObjectName,
                    @claimed_table_object_id = ct.ObjectID
                FROM @claimed_tables AS ct;

                IF @claimed_table_database IS NOT NULL
                BEGIN
                    SELECT
                        @claimed_table_stats_updated = 0,
                        @claimed_table_stats_failed = 0,
                        @claimed_table_stats_skipped = 0;

                    IF @Debug = 1
                    BEGIN
                        RAISERROR(N'  Claimed table: %s.%s.%s', 10, 1,
                            @claimed_table_database,
                            @claimed_table_schema,
                            @claimed_table_name) WITH NOWAIT;
                    END;

                    /*
                    Parallel mop-up: lazy per-table discovery.
                    When a worker claims a table that doesn't exist in its local
                    #stats_to_process (added to QueueStatistic by another worker's
                    mop-up discovery), discover per-stat rows for just this table.
                    */
                    IF NOT EXISTS (
                        SELECT 1 FROM #stats_to_process AS stp
                        WHERE stp.database_name = @claimed_table_database
                        AND   stp.object_id = @claimed_table_object_id
                    )
                    BEGIN
                        DECLARE @lazy_mop_sql nvarchar(max);
                        /* v2.27: CAST first element to nvarchar(max) to prevent 4000 char truncation during concatenation */
                        SET @lazy_mop_sql = CAST(N'
                        USE ' AS nvarchar(max)) + QUOTENAME(@claimed_table_database) + N';
                        SELECT
                            database_name = DB_NAME(),
                            schema_name = OBJECT_SCHEMA_NAME(s.object_id),
                            table_name = OBJECT_NAME(s.object_id),
                            stat_name = s.name,
                            object_id = s.object_id,
                            stats_id = s.stats_id,
                            no_recompute = s.no_recompute,
                            is_incremental = s.is_incremental,
                            is_memory_optimized = ISNULL(t.is_memory_optimized, 0),
                            is_heap = CASE WHEN EXISTS (SELECT 1 FROM sys.indexes AS ix WHERE ix.object_id = s.object_id AND ix.index_id = 0) THEN 1 ELSE 0 END,
                            auto_created = s.auto_created,
                            modification_counter = ISNULL(sp.modification_counter, 0),
                            row_count = ISNULL(sp.rows, 0),
                            days_stale = ISNULL(DATEDIFF(DAY, sp.last_updated, SYSDATETIME()), 9999),
                            page_count = ISNULL(pgs.total_pages, 0),
                            persisted_sample_percent = sp.persisted_sample_percent,
                            histogram_steps = sp.steps,
                            has_filter = s.has_filter,
                            filter_definition = s.filter_definition,
                            unfiltered_rows = sp.unfiltered_rows,
                            qs_plan_count = CONVERT(int, NULL),
                            qs_total_executions = CONVERT(bigint, NULL),
                            qs_total_cpu_ms = CONVERT(bigint, NULL),
                            qs_total_duration_ms = CONVERT(bigint, NULL),
                            qs_total_logical_reads = CONVERT(bigint, NULL),
                            qs_total_memory_grant_kb = CONVERT(bigint, NULL),
                            qs_total_tempdb_pages = CONVERT(bigint, NULL),
                            qs_total_physical_reads = CONVERT(bigint, NULL),
                            qs_total_logical_writes = CONVERT(bigint, NULL),
                            qs_total_wait_time_ms = CONVERT(bigint, NULL),
                            qs_max_dop = CONVERT(smallint, NULL),
                            qs_active_feedback_count = CONVERT(int, NULL),
                            qs_last_execution = CONVERT(datetime2, NULL),
                            qs_priority_boost = CONVERT(bigint, 0),
                            is_published = ISNULL(t.is_published, 0),
                            is_tracked_by_cdc = ISNULL(t.is_tracked_by_cdc, 0),
                            temporal_type = ISNULL(t.temporal_type, 0),
                            priority = ROW_NUMBER() OVER (ORDER BY ISNULL(sp.modification_counter, 0) DESC, s.stats_id ASC)
                        FROM sys.stats AS s
                        JOIN sys.objects AS o ON o.object_id = s.object_id
                        LEFT JOIN sys.tables AS t ON t.object_id = s.object_id
                        CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
                        OUTER APPLY (
                            SELECT total_pages = SUM(p.used_page_count)
                            FROM sys.dm_db_partition_stats AS p
                            WHERE p.object_id = s.object_id AND p.index_id IN (0, 1)
                        ) AS pgs
                        WHERE s.object_id = @object_id_param
                        AND   ISNULL(sp.modification_counter, 0) > 0
                        /* v2.28: Object type filter (defense-in-depth, matches main discovery) */
                        AND   (
                                  OBJECTPROPERTY(s.object_id, N''IsUserTable'') = 1
                               OR @i_include_system_objects_param = N''Y''
                               OR (o.type = N''V'' AND @i_include_indexed_views_param = N''Y''
                                   AND EXISTS (SELECT 1 FROM sys.indexes AS vi WHERE vi.object_id = s.object_id AND vi.index_id = 1))
                              )
                        AND   (o.is_ms_shipped = 0 OR @i_include_system_objects_param = N''Y'')
                        AND   o.type NOT IN (N''ET'', N''S'')
                        AND   (@TargetNorecompute_param = N''BOTH''
                               OR (@TargetNorecompute_param = N''N'' AND s.no_recompute = 0)
                               OR (@TargetNorecompute_param = N''Y'' AND s.no_recompute = 1))
                        /* v2.28: Table inclusion filter (defense-in-depth) */
                        AND   (
                                  @Tables_param IS NULL
                               OR OBJECT_SCHEMA_NAME(s.object_id) + N''.'' + OBJECT_NAME(s.object_id) COLLATE DATABASE_DEFAULT IN
                                  (SELECT LTRIM(RTRIM(ss.value)) COLLATE DATABASE_DEFAULT FROM STRING_SPLIT(@Tables_param, N'','') AS ss)
                               OR OBJECT_NAME(s.object_id) COLLATE DATABASE_DEFAULT IN
                                  (SELECT LTRIM(RTRIM(ss.value)) COLLATE DATABASE_DEFAULT FROM STRING_SPLIT(@Tables_param, N'','') AS ss)
                              )
                        /* v2.28: Table exclusion filter (defense-in-depth) */
                        AND   (
                                  @ExcludeTables_param IS NULL
                               OR NOT EXISTS
                                  (
                                      SELECT 1
                                      FROM STRING_SPLIT(@ExcludeTables_param, N'','') AS ex
                                      WHERE OBJECT_SCHEMA_NAME(s.object_id) + N''.'' + OBJECT_NAME(s.object_id) COLLATE DATABASE_DEFAULT LIKE LTRIM(RTRIM(ex.value)) COLLATE DATABASE_DEFAULT
                                  )
                              )
                        /* v2.26: Statistics exclusion filter (was missing from lazy mop-up) */
                        AND   (
                                  @ExcludeStatistics_param IS NULL
                               OR NOT EXISTS
                                  (
                                      SELECT 1
                                      FROM STRING_SPLIT(@ExcludeStatistics_param, N'','') AS ex
                                      WHERE s.name COLLATE DATABASE_DEFAULT LIKE LTRIM(RTRIM(ex.value)) COLLATE DATABASE_DEFAULT
                                         OR s.name COLLATE DATABASE_DEFAULT = LTRIM(RTRIM(ex.value)) COLLATE DATABASE_DEFAULT
                                  )
                              )
                        /* v2.27: Filtered stats mode filter */
                        AND   (
                                  @i_filtered_stats_mode_param = N''INCLUDE''
                               OR @i_filtered_stats_mode_param = N''PRIORITY''
                               OR (@i_filtered_stats_mode_param = N''EXCLUDE'' AND s.has_filter = 0)
                               OR (@i_filtered_stats_mode_param = N''ONLY'' AND s.has_filter = 1)
                              )
                        /* v2.27: Stretch Database auto-skip (#55, defense-in-depth) */
                        AND   ISNULL(OBJECTPROPERTY(s.object_id, N''TableHasRemoteDataArchive''), 0) = 0
                        /* v2.27: Skip tables on READ_ONLY filegroups (#65, defense-in-depth) */
                        AND   NOT EXISTS
                              (
                                  SELECT 1
                                  FROM sys.indexes AS ri
                                  JOIN sys.data_spaces AS rds ON rds.data_space_id = ri.data_space_id
                                  JOIN sys.filegroups AS rfg ON rfg.data_space_id = rds.data_space_id
                                  WHERE ri.object_id = s.object_id
                                  AND   ri.index_id IN (0, 1)
                                  AND   rfg.is_read_only = 1
                              )
                        /* v2.27: Skip tables with columnstore indexes (defense-in-depth) */
                        AND   (
                                  @i_skip_tables_with_columnstore_param = N''N''
                               OR NOT EXISTS
                                  (
                                      SELECT 1
                                      FROM sys.indexes AS ci
                                      WHERE ci.object_id = s.object_id
                                      AND   ci.type IN (5, 6)
                                  )
                              )
                        /* v2.27: Minimum page count filter (defense-in-depth) */
                        AND   (@i_min_page_count_param IS NULL OR ISNULL(pgs.total_pages, 0) >= @i_min_page_count_param)
                        AND   NOT EXISTS (
                            SELECT 1 FROM ' + @commandlog_3part + N' AS cl
                            WHERE cl.CommandType = N''UPDATE_STATISTICS''
                            AND   cl.DatabaseName = DB_NAME() COLLATE DATABASE_DEFAULT
                            AND   cl.SchemaName = OBJECT_SCHEMA_NAME(s.object_id) COLLATE DATABASE_DEFAULT
                            AND   cl.ObjectName = OBJECT_NAME(s.object_id) COLLATE DATABASE_DEFAULT
                            AND   cl.StatisticsName = s.name COLLATE DATABASE_DEFAULT
                            AND   cl.StartTime >= @start_time_param
                            AND   cl.ErrorNumber = 0
                        )
                        ORDER BY priority
                        OPTION (RECOMPILE);';

                        DECLARE @lazy_mop_found int = 0;

                        BEGIN TRY
                            INSERT INTO #stats_to_process
                            (
                                database_name, schema_name, table_name, stat_name, object_id, stats_id,
                                no_recompute, is_incremental, is_memory_optimized, is_heap, auto_created,
                                modification_counter, row_count, days_stale, page_count, persisted_sample_percent, histogram_steps,
                                has_filter, filter_definition, unfiltered_rows,
                                qs_plan_count, qs_total_executions, qs_total_cpu_ms, qs_total_duration_ms,
                                qs_total_logical_reads, qs_total_memory_grant_kb, qs_total_tempdb_pages, qs_total_physical_reads, qs_total_logical_writes, qs_total_wait_time_ms, qs_max_dop, qs_active_feedback_count,
                                qs_last_execution, qs_priority_boost,
                                is_published, is_tracked_by_cdc, temporal_type, priority
                            )
                            EXECUTE sys.sp_executesql
                                @lazy_mop_sql,
                                N'@object_id_param int, @i_include_system_objects_param nvarchar(1), @i_include_indexed_views_param nvarchar(1), @TargetNorecompute_param nvarchar(10), @Tables_param nvarchar(max), @ExcludeTables_param nvarchar(max), @ExcludeStatistics_param nvarchar(max), @i_filtered_stats_mode_param nvarchar(10), @i_skip_tables_with_columnstore_param nchar(1), @i_min_page_count_param bigint, @start_time_param datetime2(7)',
                                @object_id_param = @claimed_table_object_id,
                                @i_include_system_objects_param = @i_include_system_objects,
                                @i_include_indexed_views_param = @i_include_indexed_views,
                                @TargetNorecompute_param = @TargetNorecompute,
                                @Tables_param = @Tables,
                                @ExcludeTables_param = @ExcludeTables,
                                @ExcludeStatistics_param = @ExcludeStatistics,
                                @i_filtered_stats_mode_param = @i_filtered_stats_mode,
                                @i_skip_tables_with_columnstore_param = @i_skip_tables_with_columnstore,
                                @i_min_page_count_param = @i_min_page_count,
                                @start_time_param = @start_time;

                            SET @lazy_mop_found = ROWCOUNT_BIG();

                            IF @lazy_mop_found > 0
                            BEGIN
                                SET @total_stats = @total_stats + @lazy_mop_found;
                                SET @mop_up_stats_found = @mop_up_stats_found + @lazy_mop_found;
                                SET @in_mop_up = 1;

                                IF @Debug = 1
                                    RAISERROR(N'  Lazy mop-up: discovered %d stats for %s.%s.%s', 10, 1,
                                        @lazy_mop_found, @claimed_table_database, @claimed_table_schema, @claimed_table_name) WITH NOWAIT;
                            END
                            ELSE
                            BEGIN
                                /* No qualifying stats -- updated between mop-up discovery and claim.
                                   Mark table complete and release so next iteration claims a new table. */
                                UPDATE qs
                                SET    qs.TableEndTime = SYSDATETIME(),
                                       qs.StatsUpdated = 0,
                                       qs.StatsFailed = 0,
                                       qs.StatsSkipped = 0
                                FROM dbo.QueueStatistic AS qs
                                WHERE qs.QueueID = @queue_id
                                AND   qs.DatabaseName = @claimed_table_database
                                AND   qs.SchemaName = @claimed_table_schema
                                AND   qs.ObjectName = @claimed_table_name;

                                SELECT
                                    @claimed_table_database = NULL,
                                    @claimed_table_schema = NULL,
                                    @claimed_table_name = NULL,
                                    @claimed_table_object_id = NULL;

                                IF @Debug = 1
                                    RAISERROR(N'  Lazy mop-up: 0 stats for %s.%s -- skipping table', 10, 1,
                                        @claimed_table_schema, @claimed_table_name) WITH NOWAIT;
                            END;
                        END TRY
                        BEGIN CATCH
                            IF @Debug = 1
                            BEGIN
                                DECLARE @lazy_err nvarchar(500) = ERROR_MESSAGE();
                                RAISERROR(N'  Lazy mop-up warning (%s.%s): %s', 10, 1,
                                    @claimed_table_schema, @claimed_table_name, @lazy_err) WITH NOWAIT;
                            END;

                            /* Release claim on error */
                            UPDATE qs
                            SET    qs.TableEndTime = SYSDATETIME(),
                                   qs.StatsUpdated = 0,
                                   qs.StatsFailed = 0,
                                   qs.StatsSkipped = 0
                            FROM dbo.QueueStatistic AS qs
                            WHERE qs.QueueID = @queue_id
                            AND   qs.DatabaseName = @claimed_table_database
                            AND   qs.SchemaName = @claimed_table_schema
                            AND   qs.ObjectName = @claimed_table_name;

                            SELECT
                                @claimed_table_database = NULL,
                                @claimed_table_schema = NULL,
                                @claimed_table_name = NULL,
                                @claimed_table_object_id = NULL;
                        END CATCH;
                    END;
                END;
                ELSE
                BEGIN
                    /*
                    #433: Claim returned 0 rows. This can happen when two workers
                    compute the same MIN(TablePriority) and one commits first (READPAST
                    skips the locked row). Re-check for unclaimed work before declaring
                    PARALLEL_COMPLETE -- other priorities may still have unclaimed tables.
                    */
                    IF EXISTS (
                        SELECT 1
                        FROM dbo.QueueStatistic AS qs3
                        WHERE qs3.QueueID = @queue_id
                        AND   qs3.TableStartTime IS NULL
                    )
                    BEGIN
                        IF @Debug = 1
                            RAISERROR(N'  Parallel claim race: 0 rows claimed but unclaimed work exists -- retrying', 10, 1) WITH NOWAIT;
                        CONTINUE;
                    END;

                    SELECT
                        @stop_reason = N'PARALLEL_COMPLETE';
                    BREAK;
                END;
            END;
        END;

        /*#endregion 09C-PARALLEL-CLAIM */
        /*#region 09D-WORK-CLAIM: SELECT TOP 1 from #stats_to_process, mark processed, COMPLETED break */
        /*
        Claim next work item
        */
        SELECT
            @claimed_work = 0,
            @current_database = NULL;

        SELECT TOP (1)
            @current_database = stp.database_name,
            @current_schema_name = stp.schema_name,
            @current_table_name = stp.table_name,
            @current_stat_name = stp.stat_name,
            @current_object_id = stp.object_id,
            @current_stats_id = stp.stats_id,
            @current_no_recompute = stp.no_recompute,
            @current_is_incremental = stp.is_incremental,
            @current_is_memory_optimized = stp.is_memory_optimized,
            @current_is_heap = stp.is_heap,
            @current_auto_created = stp.auto_created,
            @current_modification_counter = stp.modification_counter,
            @current_row_count = stp.row_count,
            @current_days_stale = stp.days_stale,
            @current_page_count = stp.page_count,
            @current_persisted_sample_percent = stp.persisted_sample_percent,
            @current_histogram_steps = stp.histogram_steps,
            @current_forwarded_records = NULL,
            /* Filtered statistics metadata */
            @current_has_filter = stp.has_filter,
            @current_filter_definition = stp.filter_definition,
            @current_unfiltered_rows = stp.unfiltered_rows,
            @current_filtered_drift_ratio = stp.filtered_drift_ratio,
            /* Query Store priority metadata */
            @current_qs_plan_count = stp.qs_plan_count,
            @current_qs_total_executions = stp.qs_total_executions,
            @current_qs_total_cpu_ms = stp.qs_total_cpu_ms,
            @current_qs_total_duration_ms = stp.qs_total_duration_ms,
            @current_qs_total_logical_reads = stp.qs_total_logical_reads,
            @current_qs_total_memory_grant_kb = stp.qs_total_memory_grant_kb,
            @current_qs_total_tempdb_pages = stp.qs_total_tempdb_pages,
            @current_qs_total_physical_reads = stp.qs_total_physical_reads,
            @current_qs_total_logical_writes = stp.qs_total_logical_writes,
            @current_qs_total_wait_time_ms = stp.qs_total_wait_time_ms,
            @current_qs_max_dop = stp.qs_max_dop,
            @current_qs_last_execution = stp.qs_last_execution,
            @current_qs_priority_boost = stp.qs_priority_boost,
            /* Replication and temporal table awareness */
            @current_is_published = stp.is_published,
            @current_is_tracked_by_cdc = stp.is_tracked_by_cdc,
            @current_temporal_type = stp.temporal_type
        FROM #stats_to_process AS stp
        WHERE stp.processed = 0
        /*
        In parallel mode, only process stats for the currently claimed table.
        This prevents concurrent updates to the same table from different workers.
        */
        AND   (
                  @StatsInParallel = N'N'
               OR (
                      stp.database_name = @claimed_table_database
                  AND stp.schema_name = @claimed_table_schema
                  AND stp.table_name = @claimed_table_name
                  )
              )
        ORDER BY
            stp.priority;

        IF @current_database IS NOT NULL
        BEGIN
            SELECT
                @claimed_work = 1;
            /*
            Mark as in-progress
            */
            UPDATE
                stp
            SET
                stp.processed = 1
            FROM #stats_to_process AS stp
            WHERE stp.database_name = @current_database
            AND   stp.schema_name = @current_schema_name
            AND   stp.table_name = @current_table_name
            AND   stp.stat_name = @current_stat_name;
        END;

        /*
        Exit loop if no more work
        */
        IF @claimed_work = 0
        BEGIN
            SELECT
                @stop_reason = N'COMPLETED';
            BREAK;
        END;

        /*
        P2c fix (v2.4): @StopByTime window overshoot prevention.
        When @i_max_seconds_per_stat and @i_time_limit are both set, check CommandLog history
        for estimated duration of this stat. If estimated > remaining_seconds, skip.
        Conservative: if no history in CommandLog, always run it.
        */
        IF  @i_max_seconds_per_stat IS NOT NULL
        AND @i_time_limit IS NOT NULL
        AND @commandlog_exists = 1
        AND @Execute = N'Y'
        AND @current_database IS NOT NULL
        BEGIN
            DECLARE
                @p2c_estimated_seconds int = NULL,
                @p2c_remaining_seconds int,
                @p2c_skip_msg nvarchar(500);

            SELECT @p2c_remaining_seconds = @i_time_limit - DATEDIFF(SECOND, @start_time, SYSDATETIME());

            /* Estimate from CommandLog: avg of last 10 successful runs for this stat */
            SELECT TOP (1)
                @p2c_estimated_seconds = CONVERT(int, AVG(DATEDIFF(SECOND, cl.StartTime, cl.EndTime)))
            FROM dbo.CommandLog AS cl
            WHERE cl.CommandType = N'UPDATE_STATISTICS'
            AND   cl.DatabaseName = @current_database
            AND   cl.SchemaName = @current_schema_name
            AND   cl.ObjectName = @current_table_name
            AND   cl.StatisticsName = @current_stat_name
            AND   cl.EndTime IS NOT NULL
            AND   cl.ErrorNumber = 0
            AND   cl.StartTime >= DATEADD(DAY, -@i_command_log_retention_days, SYSDATETIME()); /* #251: parameterized retention */

            /* Skip if history exists AND estimated exceeds both the per-stat cap AND remaining window */
            IF  @p2c_estimated_seconds IS NOT NULL
            AND @p2c_estimated_seconds > @i_max_seconds_per_stat
            AND @p2c_estimated_seconds > @p2c_remaining_seconds
            BEGIN
                SELECT @p2c_skip_msg =
                    N'  [P2c SKIP] ' + QUOTENAME(@current_schema_name) + N'.' +
                    QUOTENAME(@current_table_name) + N'.' + QUOTENAME(@current_stat_name) +
                    N' -- estimated ' + CONVERT(nvarchar(10), @p2c_estimated_seconds) +
                    N's, remaining ' + CONVERT(nvarchar(10), @p2c_remaining_seconds) + N's';
                RAISERROR(@p2c_skip_msg, 10, 1) WITH NOWAIT;
                SET @stats_skipped += 1;
                CONTINUE; /* Already marked processed=1 above; skip execution */
            END;
        END;

        SELECT
            @stats_processed += 1,
            @current_start_time = SYSDATETIME();

        /*#endregion 09D-WORK-CLAIM */
        /*#region 09E-COMMAND-BUILD: Lock timeout, FULLSCAN/SAMPLE/RESAMPLE, MAXDOP, ON PARTITIONS, NORECOMPUTE */
        /*
        ========================================================================
        BUILD UPDATE STATISTICS COMMAND
        ========================================================================
        */
        SELECT
            @current_command = N'';

        /*
        Lock timeout
        -1 = infinite wait (pass through as-is, don't multiply)
        0+ = timeout in seconds (convert to milliseconds)
        */
        IF @i_lock_timeout IS NOT NULL
        BEGIN
            SELECT
                @current_command =
                    N'SET LOCK_TIMEOUT ' +
                    CASE
                        WHEN @i_lock_timeout = -1 THEN N'-1'
                        ELSE CONVERT(nvarchar(20), CONVERT(bigint, @i_lock_timeout) * 1000)
                    END +
                    N'; ';
        END;

        /*
        Base command
        */
        SELECT
            @current_command +=
                N'UPDATE STATISTICS ' +
                QUOTENAME(@current_database) + N'.' +
                QUOTENAME(@current_schema_name) + N'.' +
                QUOTENAME(@current_table_name) +
                N' (' + QUOTENAME(@current_stat_name) + N')';

        /*
        WITH clause options
        Sample source tracking for ExtendedInfo (answers "why this sample rate?"):
          EXPLICIT = User passed @i_statistics_sample
          ADAPTIVE = Long-running stat override (@LongRunningSamplePercent)
          ADAPTIVE_CAPPED = Adaptive but capped to ~10M rows
          AUTO = SQL Server decides (NULL)
          RESAMPLE = Using RESAMPLE (persisted or incremental)
          FULLSCAN_MEMOPT = Forced FULLSCAN for memory-optimized on SQL 2014
        */
        DECLARE
            @has_with_option bit = 0,
            @with_clause nvarchar(max) = N'',
            @is_long_running_stat bit = 0,
            @effective_sample_percent int = @i_statistics_sample,
            @sample_source nvarchar(20) = CASE
                WHEN @i_statistics_sample IS NOT NULL THEN N'EXPLICIT'
                ELSE N'AUTO'
            END;

        /*
        ADAPTIVE SAMPLING: Check if this stat is historically long-running
        If so, override the sample rate with @LongRunningSamplePercent
        */
        IF @LongRunningThresholdMinutes IS NOT NULL
        BEGIN
            IF EXISTS
            (
                SELECT 1
                FROM @long_running_stats AS lrs
                WHERE lrs.database_name = @current_database
                AND   lrs.schema_name = @current_schema_name
                AND   lrs.table_name = @current_table_name
                AND   lrs.stat_name = @current_stat_name
            )
            BEGIN
                SELECT
                    @is_long_running_stat = 1;

                /*
                CAP SAMPLE PERCENT TO AVOID EXCESSIVE ROW COUNTS
                10% of 1B rows = 100M rows, which is worse than auto-sample.
                Cap at ~10M rows sampled. For a 1B row table: 10M/1B*100 = 1%.
                Minimum 1% (SQL Server's floor for SAMPLE PERCENT).
                */
                SELECT @effective_sample_percent =
                    CASE
                        WHEN @current_row_count <= 0 THEN @i_long_running_sample_pct
                        WHEN @current_row_count <= 10000000 THEN @i_long_running_sample_pct
                        ELSE
                            CASE
                                WHEN CEILING(10000000.0 / @current_row_count * 100) < @i_long_running_sample_pct
                                THEN CONVERT(int, CEILING(10000000.0 / @current_row_count * 100))
                                ELSE @i_long_running_sample_pct
                            END
                    END;

                /* Ensure minimum 1% (SQL Server requirement for SAMPLE PERCENT) */
                IF @effective_sample_percent < 1
                    SELECT @effective_sample_percent = 1;

                /* Track sample source for ExtendedInfo logging */
                SELECT @sample_source =
                    CASE
                        WHEN @effective_sample_percent < @i_long_running_sample_pct
                        THEN N'ADAPTIVE_CAPPED'
                        ELSE N'ADAPTIVE'
                    END;

                DECLARE @lr_hist_msg nvarchar(500);
                SELECT @lr_hist_msg =
                    N'  Adaptive Sampling: ' + @current_stat_name +
                    N' (historically slow, forcing ' + CONVERT(nvarchar(10), @effective_sample_percent) + N'%% sample' +
                    CASE
                        WHEN @effective_sample_percent < @i_long_running_sample_pct
                        THEN N', capped from ' + CONVERT(nvarchar(10), @i_long_running_sample_pct) + N'%%'
                        ELSE N''
                    END + N')';
                RAISERROR(@lr_hist_msg, 10, 1) WITH NOWAIT;
            END;
        END;

        /*
        P1c fix (v2.4): Compute absolute sampled rows for RESAMPLE_PERSIST quality floor check.
        Reset each iteration so stale values from previous stats don't bleed through.
        This is referenced by the ELSE IF RESAMPLE_PERSIST block below.
        */
        SET @absolute_sampled_rows = NULL;
        IF  @current_row_count IS NOT NULL
        AND @current_persisted_sample_percent IS NOT NULL
        AND @current_persisted_sample_percent > 0
            SET @absolute_sampled_rows = CONVERT(bigint, @current_row_count * (@current_persisted_sample_percent / 100.0));

        /*
        #183 (P2): Warn when @i_statistics_sample explicitly overrides a persisted sample percent.
        No behavior change -- the update proceeds with @i_statistics_sample as requested.
        Gives the DBA immediate visibility when their PERSIST_SAMPLE_PERCENT setting is being
        silently overridden. To preserve the persisted rate, leave @i_statistics_sample NULL.
        */
        IF  @i_statistics_sample IS NOT NULL
        AND @current_persisted_sample_percent IS NOT NULL
        AND CONVERT(int, ROUND(@current_persisted_sample_percent, 0)) <> @i_statistics_sample
        BEGIN
            DECLARE @persist_ovr_msg nvarchar(1000);
            SET @persist_ovr_msg =
                N'WARNING: Stat [' + @current_schema_name + N'].[' + @current_table_name
                + N'].[' + @current_stat_name + N'] has persisted sample '
                + CONVERT(nvarchar(10), CONVERT(int, ROUND(@current_persisted_sample_percent, 0)))
                + N'% -- @i_statistics_sample=' + CONVERT(nvarchar(10), @i_statistics_sample)
                + N' will override it. To preserve persisted sampling, leave @i_statistics_sample NULL. (#183)';
            RAISERROR(N'%s', 10, 1, @persist_ovr_msg) WITH NOWAIT; /* Use %s to avoid % in message being treated as format spec */
            SET @warnings = @warnings
                + N'PERSIST_SAMPLE_OVERRIDE: [' + @current_schema_name + N'].[' + @current_table_name + N'].[' + @current_stat_name + N'] '
                + N'persisted=' + CONVERT(nvarchar(10), CONVERT(int, ROUND(@current_persisted_sample_percent, 0)))
                + N'% overridden by @i_statistics_sample=' + CONVERT(nvarchar(10), @i_statistics_sample) + N'%; ';
        END;

        /*
        Memory-optimized tables have special requirements
        - SQL Server 2014: Requires FULLSCAN or RESAMPLE, no sampling
        - SQL Server 2016+: Supports sampling
        Note: Memory-optimized takes precedence over adaptive sampling
        */
        IF  @current_is_memory_optimized = 1
        AND @sql_version < 13
        AND @effective_sample_percent IS NOT NULL
        AND @effective_sample_percent < 100
        BEGIN
            /*
            Force FULLSCAN for memory-optimized on SQL 2014
            */
            SELECT
                @with_clause = N'FULLSCAN',
                @has_with_option = 1,
                @sample_source = N'FULLSCAN_MEMOPT',
                @effective_sample_percent = 100;

            IF @Debug = 1
            BEGIN
                RAISERROR(N'  Note: Memory-optimized table on SQL 2014 requires FULLSCAN', 10, 1) WITH NOWAIT;
            END;
        END;
        ELSE IF @effective_sample_percent = 100
        BEGIN
            SELECT
                @with_clause = N'FULLSCAN',
                @has_with_option = 1;
        END;
        ELSE IF @effective_sample_percent IS NOT NULL
        BEGIN
            SELECT
                @with_clause =
                    N'SAMPLE ' +
                    CONVERT(nvarchar(10), @effective_sample_percent) +
                    N' PERCENT',
                @has_with_option = 1;
        END;
        /*
        RESPECT PERSISTED SAMPLE PERCENT
        When @i_statistics_sample is NULL and the stat has a persisted sample,
        honor the existing setting by using RESAMPLE.
        This preserves the sample rate without overriding DBA-tuned values.
        Exception: Long-running stats use forced sample rate instead of RESAMPLE.
        P1c fix (v2.4): Quality floor -- skip RESAMPLE when persisted sample rate would produce
        too few rows for meaningful histograms (controlled by @i_persist_sample_min_rows).
        */
        ELSE IF @effective_sample_percent IS NULL
        AND     @current_persisted_sample_percent IS NOT NULL
        AND     @is_long_running_stat = 0
        AND     (@i_persist_sample_min_rows IS NULL OR @absolute_sampled_rows IS NULL OR @absolute_sampled_rows >= @i_persist_sample_min_rows)
        BEGIN
            SELECT
                @with_clause = N'RESAMPLE',
                @has_with_option = 1,
                @sample_source = N'RESAMPLE_PERSIST';

            IF @Debug = 1
            BEGIN
                SELECT @persisted_pct_msg = CONVERT(integer, @current_persisted_sample_percent);
                RAISERROR(N'  Note: Respecting persisted sample %d%% via RESAMPLE', 10, 1, @persisted_pct_msg) WITH NOWAIT;
            END;
        END
        /* #246: Warn when persisted sample rate produces too few rows for quality histograms */
        ELSE IF @effective_sample_percent IS NULL
        AND     @current_persisted_sample_percent IS NOT NULL
        AND     @is_long_running_stat = 0
        AND     @i_persist_sample_min_rows IS NOT NULL
        AND     @absolute_sampled_rows IS NOT NULL
        AND     @absolute_sampled_rows < @i_persist_sample_min_rows
        BEGIN
            SET @p246_pct = CONVERT(integer, @current_persisted_sample_percent);
            SET @p246_msg =
                N'  WARNING: Persisted sample ' + CONVERT(nvarchar(10), @p246_pct)
                + N'%% yields ~' + CONVERT(nvarchar(20), @absolute_sampled_rows)
                + N' rows (below @i_persist_sample_min_rows=' + CONVERT(nvarchar(20), @i_persist_sample_min_rows)
                + N'). RESAMPLE skipped -- using SQL Server auto-sample instead. (#246)';
            RAISERROR(@p246_msg, 10, 1) WITH NOWAIT;
            SET @warnings += N'PERSIST_SAMPLE_INADEQUATE: [' + @current_schema_name + N'].[' + @current_table_name + N'].[' + @current_stat_name
                + N'] persisted ' + CONVERT(nvarchar(10), @p246_pct) + N'% yields ' + CONVERT(nvarchar(20), @absolute_sampled_rows) + N' rows; ';
        END;

        /*
        #281: Warn when effective sample rate would produce too few rows for a meaningful histogram.
        Threshold: sampled rows < 1000 (too few for SQL Server's 200-step histogram).
        Only fires for explicit or adaptive sample rates, not auto or FULLSCAN.
        */
        IF  @effective_sample_percent IS NOT NULL
        AND @effective_sample_percent < 100
        AND @current_row_count > 0
        AND (@effective_sample_percent * @current_row_count / 100) < 1000
        BEGIN
            DECLARE @p281_sampled_rows bigint = @effective_sample_percent * @current_row_count / 100;
            DECLARE @p281_msg nvarchar(1000) =
                N'  WARNING: SAMPLE ' + CONVERT(nvarchar(10), @effective_sample_percent)
                + N'%% on ' + QUOTENAME(@current_schema_name) + N'.' + QUOTENAME(@current_table_name)
                + N'.' + QUOTENAME(@current_stat_name)
                + N' yields ~' + CONVERT(nvarchar(20), @p281_sampled_rows)
                + N' rows -- may be too few for meaningful histogram.  Consider higher sample or FULLSCAN. (#281)';
            RAISERROR(@p281_msg, 10, 1) WITH NOWAIT;
        END;

        /*
        Incremental statistics: ON PARTITIONS()
        Only applies to incremental stats on partitioned tables.
        Query sys.dm_db_incremental_stats_properties to find stale partitions.
        */
        DECLARE
            @incremental_partitions nvarchar(max) = NULL,
            @incremental_partition_count int = 0,
            @incremental_total_partitions int = 0,
            @physical_partition_count int = 0,
            @on_partitions_clause nvarchar(max) = N'';

        IF  @i_update_incremental = 1
        AND @current_is_incremental = 1
        BEGIN
            /*
            P1 #26: Cross-reference with sys.partitions
            dm_db_incremental_stats_properties may miss truncated/empty partitions.
            Get physical partition count from sys.partitions as authoritative source.

            gh-467: Cache physical partition count per (database, object_id).
            Consecutive stats on the same table would otherwise recompile + rescan
            sys.partitions once per stat.  Cache lives for the whole process loop.
            */
            IF  @last_partition_db COLLATE DATABASE_DEFAULT = @current_database COLLATE DATABASE_DEFAULT
            AND @last_partition_object_id = @current_object_id
            AND @last_partition_count IS NOT NULL
            BEGIN
                SET @physical_partition_count = @last_partition_count;
            END;
            ELSE
            BEGIN
                DECLARE @physical_sql nvarchar(max) = N'
                    USE ' + QUOTENAME(@current_database) + N';
                    SELECT @count_out = COUNT(DISTINCT partition_number)
                    FROM sys.partitions
                    WHERE object_id = @obj_id
                    AND   index_id IN (0, 1)';

                EXEC sys.sp_executesql
                    @physical_sql,
                    N'@obj_id int, @count_out int OUTPUT',
                    @obj_id = @current_object_id,
                    @count_out = @physical_partition_count OUTPUT;

                SET @last_partition_db = @current_database;
                SET @last_partition_object_id = @current_object_id;
                SET @last_partition_count = @physical_partition_count;
            END;

            /*
            Query dm_db_incremental_stats_properties to find partitions with modifications.
            Only update partitions that exceed the modification threshold.
            This is the correct behavior for incremental statistics - updating all
            partitions defeats the purpose of incremental stats.
            */
            DECLARE @partition_sql nvarchar(max) = N'
                USE ' + QUOTENAME(@current_database) + N';
                SELECT @partitions_out = STRING_AGG(CONVERT(nvarchar(10), isp.partition_number), N'', '')
                                         WITHIN GROUP (ORDER BY isp.partition_number),
                       @count_out = COUNT(*),
                       @total_out = (SELECT COUNT(*) FROM sys.dm_db_incremental_stats_properties(@obj_id, @stat_id))
                FROM sys.dm_db_incremental_stats_properties(@obj_id, @stat_id) AS isp
                WHERE isp.modification_counter > 0
                AND   ISNULL(isp.rows, 0) > 0'; /* #171: Skip empty partitions (post-SWITCH OUT) */

            /*
            Use STRING_AGG on SQL 2017+ (faster, cleaner)
            Fall back to FOR XML PATH on SQL 2016
            */
            IF @sql_major_version >= 14
            BEGIN
                /* SQL 2017+: Use STRING_AGG */
                EXEC sys.sp_executesql
                    @partition_sql,
                    N'@obj_id int, @stat_id int, @partitions_out nvarchar(max) OUTPUT, @count_out int OUTPUT, @total_out int OUTPUT',
                    @obj_id = @current_object_id,
                    @stat_id = @current_stats_id,
                    @partitions_out = @incremental_partitions OUTPUT,
                    @count_out = @incremental_partition_count OUTPUT,
                    @total_out = @incremental_total_partitions OUTPUT;
            END
            ELSE
            BEGIN
                /* SQL 2016: Use FOR XML PATH */
                SELECT @partition_sql = N'
                    USE ' + QUOTENAME(@current_database) + N';
                    SELECT @partitions_out = STUFF((
                               SELECT N'', '' + CONVERT(nvarchar(10), isp.partition_number)
                               FROM sys.dm_db_incremental_stats_properties(@obj_id, @stat_id) AS isp
                               WHERE isp.modification_counter > 0
                               AND   ISNULL(isp.rows, 0) > 0 /* #171: Skip empty partitions */
                               ORDER BY isp.partition_number
                               FOR XML PATH(''''), TYPE
                           ).value(''.'', ''nvarchar(max)''), 1, 2, N''''),
                           @count_out = (SELECT COUNT(*) FROM sys.dm_db_incremental_stats_properties(@obj_id, @stat_id)
                                         WHERE modification_counter > 0 AND ISNULL(rows, 0) > 0),
                           @total_out = (SELECT COUNT(*) FROM sys.dm_db_incremental_stats_properties(@obj_id, @stat_id))';

                EXEC sys.sp_executesql
                    @partition_sql,
                    N'@obj_id int, @stat_id int, @partitions_out nvarchar(max) OUTPUT, @count_out int OUTPUT, @total_out int OUTPUT',
                    @obj_id = @current_object_id,
                    @stat_id = @current_stats_id,
                    @partitions_out = @incremental_partitions OUTPUT,
                    @count_out = @incremental_partition_count OUTPUT,
                    @total_out = @incremental_total_partitions OUTPUT;
            END;

            /*
            Add ON PARTITIONS clause if we found specific stale partitions.
            If ALL physical partitions are stale, skip ON PARTITIONS (full RESAMPLE is more efficient).
            If NO partitions are stale, we shouldn't be here (discovery should have filtered).

            P1 #26 / v2.0 #4/26: Truncated partition handling.
            When DMV reports fewer partitions than sys.partitions, some are missing
            (truncated/empty). These have 0 rows so skip them -- only update the
            stale partitions that actually have data. Previously this forced a full
            RESAMPLE which was wasteful (e.g., 24-partition table with 1 truncated
            would update all 24 instead of just the 5 stale ones).
            */
            DECLARE @partitions_missing int = 0;
            IF @physical_partition_count > @incremental_total_partitions
            BEGIN
                SELECT @partitions_missing = @physical_partition_count - @incremental_total_partitions;
                IF @Debug = 1
                BEGIN
                    RAISERROR(N'  Note: %d of %d partitions missing from DMV (truncated/empty, skipping)', 10, 1,
                        @partitions_missing, @physical_partition_count) WITH NOWAIT;
                END;
            END;

            IF  @incremental_partitions IS NOT NULL
            AND @incremental_partition_count > 0
            AND @incremental_partition_count < @physical_partition_count
            BEGIN
                /*
                Partial update: only stale partitions with data.
                Truncated partitions are naturally excluded (not in DMV).
                */
                SELECT
                    @on_partitions_clause = N' ON PARTITIONS(' + @incremental_partitions + N')';

                IF @Debug = 1
                BEGIN
                    IF @partitions_missing > 0
                        RAISERROR(N'  Note: Incremental statistics - updating %d of %d partitions (skipping %d truncated)', 10, 1,
                            @incremental_partition_count, @physical_partition_count, @partitions_missing) WITH NOWAIT;
                    ELSE
                        RAISERROR(N'  Note: Incremental statistics - updating %d of %d partitions', 10, 1,
                            @incremental_partition_count, @physical_partition_count) WITH NOWAIT;
                END;
            END
            ELSE IF @Debug = 1
            BEGIN
                IF @incremental_partition_count >= @physical_partition_count
                BEGIN
                    RAISERROR(N'  Note: Incremental statistics - all %d partitions stale, full RESAMPLE', 10, 1,
                        @physical_partition_count) WITH NOWAIT;
                END;
                ELSE
                BEGIN
                    RAISERROR(N'  Note: Incremental statistics - full update (partition info unavailable)', 10, 1) WITH NOWAIT;
                END;
            END;

            /*
            Incremental stats require RESAMPLE
            */
            IF @i_statistics_sample IS NULL
            BEGIN
                IF @has_with_option = 0
                BEGIN
                    SELECT
                        @with_clause = N'RESAMPLE',
                        @has_with_option = 1,
                        @sample_source = N'RESAMPLE_INCR';
                END;
            END;
        END;

        /*
        MAXDOP (SQL 2016 SP2+, SQL 2017 CU3+)
        Only add if the server supports it - gracefully degrade on older builds
        */
        IF  @MaxDOP IS NOT NULL
        AND @supports_maxdop_stats = 1
        BEGIN
            IF @has_with_option = 0
            BEGIN
                SELECT
                    @with_clause = N'MAXDOP = ' + CONVERT(nvarchar(10), @MaxDOP),
                    @has_with_option = 1;
            END;
            ELSE
            BEGIN
                SELECT
                    @with_clause += N', MAXDOP = ' + CONVERT(nvarchar(10), @MaxDOP);
            END;
        END;
        ELSE IF @MaxDOP IS NOT NULL AND @supports_maxdop_stats = 0 AND @Debug = 1
        BEGIN
            RAISERROR(N'  Note: @MaxDOP ignored - requires SQL 2016 SP2+ or SQL 2017 CU3+', 10, 1) WITH NOWAIT;
        END;

        /*
        PERSIST_SAMPLE_PERCENT (SQL 2016 SP1 CU4+)
        Only add if:
          1. Server supports it (build check passed)
          2. A sample option was specified (FULLSCAN or SAMPLE n%)
          3. NOT using RESAMPLE (RESAMPLE and PERSIST_SAMPLE_PERCENT are mutually exclusive - Error 1052)
        PERSIST_SAMPLE_PERCENT cannot be used alone - it must accompany FULLSCAN or SAMPLE.
        */
        IF  @i_persist_sample_percent = N'Y'
        AND @supports_persist_sample = 1
        AND @has_with_option = 1  /* Only add if FULLSCAN/SAMPLE already specified */
        AND @with_clause NOT LIKE N'%RESAMPLE%'  /* RESAMPLE and PERSIST_SAMPLE_PERCENT conflict (Error 1052) */
        BEGIN
            SELECT
                @with_clause += N', PERSIST_SAMPLE_PERCENT = ON';
        END;
        ELSE IF @i_persist_sample_percent = N'Y' AND @supports_persist_sample = 0 AND @Debug = 1
        BEGIN
            RAISERROR(N'  Note: @i_persist_sample_percent ignored - requires SQL 2016 SP1 CU4+', 10, 1) WITH NOWAIT;
        END;
        ELSE IF @i_persist_sample_percent = N'Y' AND @with_clause LIKE N'%RESAMPLE%' AND @Debug = 1
        BEGIN
            RAISERROR(N'  Note: @i_persist_sample_percent ignored - conflicts with RESAMPLE (Error 1052)', 10, 1) WITH NOWAIT;
        END;
        /* Note: @has_with_option=0 case now handled once at startup to reduce debug noise */

        /*
        ON PARTITIONS must come immediately after RESAMPLE in the WITH clause,
        before any other options like NORECOMPUTE.
        Valid:   WITH RESAMPLE ON PARTITIONS(2, 3), NORECOMPUTE
        Invalid: WITH RESAMPLE, NORECOMPUTE ON PARTITIONS(2, 3)
        (#215 F-1 fix)
        */
        IF @on_partitions_clause <> N''
        BEGIN
            SELECT
                @with_clause += @on_partitions_clause;
        END;

        /*
        NORECOMPUTE: Preserve the flag on stats that have it set
        Without this, UPDATE STATISTICS clears the no_recompute flag
        */
        IF @current_no_recompute = 1
        BEGIN
            IF @has_with_option = 0
            BEGIN
                SELECT
                    @with_clause = N'NORECOMPUTE',
                    @has_with_option = 1;
            END;
            ELSE
            BEGIN
                SELECT
                    @with_clause += N', NORECOMPUTE';
            END;
        END;

        /*
        Add WITH clause if we have options
        */
        IF @has_with_option = 1
        BEGIN
            SELECT
                @current_command += N' WITH ' + @with_clause;
        END;

        SELECT
            @current_command += N';';

        /*
        RESET LOCK_TIMEOUT (P1 #23, v1.9 enhancement)
        SET LOCK_TIMEOUT persists at session level after sp_executesql returns.
        v1.9: Restore to original session value instead of hardcoding -1.
        This respects caller's session state rather than assuming infinite wait.
        */
        IF @i_lock_timeout IS NOT NULL
        BEGIN
            SELECT
                @current_command += N' SET LOCK_TIMEOUT ' +
                    CONVERT(nvarchar(20), @original_lock_timeout) + N';';
        END;

        /*#endregion 09E-COMMAND-BUILD */
        /*#region 09F-EXECUTE: Progress msg, TOCTOU check, CommandLog, deadlock retry, error handling, dry-run */
        /*
        ========================================================================
        OUTPUT / EXECUTE
        ========================================================================
        */

        /*
        Progress message
        */
        SELECT
            @norecompute_display =
                CASE
                    WHEN @current_no_recompute = 1
                    THEN N'YES (preserved)'
                    ELSE N'no'
                END,
            @progress_msg =
                N'[' + CONVERT(nvarchar(10), @stats_processed) + N'/' + CONVERT(nvarchar(10), @total_stats) + N'] ' +
                @current_schema_name + N'.' + @current_table_name + N'.' + @current_stat_name +
                N' (mods: ' + CONVERT(nvarchar(20), @current_modification_counter) +
                CASE
                    WHEN @current_page_count >= 1280 /* >= ~10 MB = 0.01 GB */
                    THEN N', ' + CONVERT(nvarchar(20), CONVERT(decimal(10, 2), @current_page_count * 8.0 / 1024.0 / 1024.0)) + N' GB'
                    ELSE N''
                END +
                N', stale: ' + CONVERT(nvarchar(10), @current_days_stale) + N' days' +
                CASE
                    WHEN @current_is_incremental = 1
                    THEN N', INCREMENTAL'
                    ELSE N''
                END +
                CASE
                    WHEN @current_is_heap = 1
                    AND  @current_forwarded_records > 0
                    THEN N', HEAP (fwd: ' + CONVERT(nvarchar(20), @current_forwarded_records) + N')'
                    WHEN @current_is_heap = 1
                    THEN N', HEAP'
                    ELSE N''
                END +
                CASE
                    WHEN @current_is_memory_optimized = 1
                    THEN N', MEMORY'
                    ELSE N''
                END +
                CASE
                    WHEN @current_is_published = 1
                    THEN N', REPLICATED'
                    ELSE N''
                END +
                CASE
                    WHEN @current_is_tracked_by_cdc = 1
                    THEN N', CDC'
                    ELSE N''
                END +
                CASE
                    WHEN @current_temporal_type = 2
                    THEN N', TEMPORAL'
                    WHEN @current_temporal_type = 1
                    THEN N', TEMPORAL-HIST'
                    ELSE N''
                END +
                N', NORECOMPUTE: ' + @norecompute_display + N')';

        RAISERROR(@progress_msg, 10, 1) WITH NOWAIT;

        /* SQL 2022+ plan feedback warning (#369) */
        IF @Debug = 1 AND ISNULL(@current_qs_active_feedback_count, 0) > 0
        BEGIN
            SET @progress_msg = N'  Note: ' + CONVERT(nvarchar(10), @current_qs_active_feedback_count)
                + N' active plan feedback entries on '
                + @current_schema_name + N'.' + @current_table_name
                + N' -- stats update may reset CE/grant/DOP feedback';
            RAISERROR(@progress_msg, 10, 1) WITH NOWAIT;
        END;

        /* CDC FULLSCAN log volume warning (#mzn) */
        IF @Debug = 1
        AND @current_is_tracked_by_cdc = 1
        AND @with_clause LIKE N'%FULLSCAN%'
        BEGIN
            RAISERROR(N'  Note: CDC-tracked table -- FULLSCAN may increase log volume', 10, 1) WITH NOWAIT;
        END;

        IF @Debug = 1
        BEGIN
            RAISERROR(N'  Command: %s', 10, 1, @current_command) WITH NOWAIT;
        END;

        IF @Execute = N'Y'
        BEGIN
            /*
            TOCTOU check (sp_HeapDoctor pattern): verify stat still exists.
            Between discovery and execution, the statistic may have been dropped
            (e.g., table dropped, stat auto-dropped on schema change, manual DROP).
            bd -h4s: gated on @Debug.  The CATCH block already has a TOCTOU carve-out
            (errors 208, 15009, 2767) that handles drops gracefully without counting
            them as failures.  Skipping the pre-check in non-debug mode saves ~2500
            sp_executesql calls per run (~2.5-5 seconds).
            */
            IF @Debug = 1
            BEGIN
            SET @toctou_sql = N'SELECT @exists = COUNT(*) FROM '
                + QUOTENAME(@current_database) + N'.sys.stats AS s '
                + N'WHERE s.object_id = @obj_id AND s.stats_id = @stat_id';
            SET @toctou_exists = 0;

            BEGIN TRY
                EXEC sys.sp_executesql @toctou_sql,
                    N'@obj_id int, @stat_id int, @exists int OUTPUT',
                    @obj_id = @current_object_id,
                    @stat_id = @current_stats_id,
                    @exists = @toctou_exists OUTPUT;
            END TRY
            BEGIN CATCH
                SET @toctou_exists = 1; /* On error, proceed anyway (e.g., cross-DB permission issue) */
            END CATCH;

            IF @toctou_exists = 0
            BEGIN
                IF @Debug = 1
                BEGIN
                    DECLARE @toctou_msg nvarchar(2000) =
                        N'  Skipping ' + QUOTENAME(@current_schema_name) + N'.' + QUOTENAME(@current_table_name) + N'.' + QUOTENAME(@current_stat_name)
                        + N': object no longer exists (dropped or renamed between discovery and execution)';
                    RAISERROR(@toctou_msg, 10, 1) WITH NOWAIT;
                END
                ELSE
                BEGIN
                    RAISERROR(N'  SKIPPED: statistic no longer exists (dropped between discovery and execution)', 10, 1) WITH NOWAIT;
                END;
                SET @stats_skipped += 1;
                SET @stats_processed -= 1; /* Undo increment - TOCTOU skip is not a real attempt */

                /* #58: Log skipped stats to CommandLog for audit compliance */
                IF  @i_log_skipped = N'Y'
                AND @LogToTable = N'Y'
                AND @commandlog_exists = 1
                AND @Execute = N'Y'
                BEGIN
                    BEGIN TRY
                        INSERT INTO dbo.CommandLog
                            (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ExtendedInfo)
                        VALUES
                        (
                            @current_database,
                            @current_schema_name,
                            @current_table_name,
                            N'U',
                            N'SKIPPED: ' + @current_stat_name + N' (object no longer exists)',
                            N'UPDATE_STATISTICS',
                            SYSDATETIME(),
                            SYSDATETIME(),
                            (SELECT N'TOCTOU_SKIP' AS SkipReason, @run_label AS RunLabel, @procedure_version AS [Version]
                             FOR XML RAW(N'ExtendedInfo'), ELEMENTS)
                        );
                    END TRY
                    BEGIN CATCH /* Non-critical - ignore */ END CATCH;
                END;

                UPDATE stp
                SET stp.processed = 1
                FROM #stats_to_process AS stp
                WHERE stp.database_name = @current_database
                AND   stp.schema_name = @current_schema_name
                AND   stp.table_name = @current_table_name
                AND   stp.stat_name = @current_stat_name;

                CONTINUE;
            END;
            END; /* IF @Debug = 1 -- bd -h4s TOCTOU pre-check */

            /*
            Pre-execution INSERT to CommandLog (Ola Hallengren two-phase pattern):
            INSERT with NULL EndTime before execution, UPDATE EndTime after completion.
            This lets you monitor in-progress stats via: SELECT * FROM dbo.CommandLog WHERE EndTime IS NULL;
            */
            SET @current_commandlog_id = NULL;
            IF  @LogToTable = N'Y'
            AND @commandlog_exists = 1
            BEGIN
                BEGIN TRY
                    INSERT INTO
                        dbo.CommandLog
                    (
                        DatabaseName,
                        SchemaName,
                        ObjectName,
                        ObjectType,
                        StatisticsName,
                        Command,
                        CommandType,
                        StartTime
                    )
                    VALUES
                    (
                        @current_database,
                        @current_schema_name,
                        @current_table_name,
                        N'U',
                        @current_stat_name,
                        @current_command,
                        @current_command_type,
                        @current_start_time
                    );
                    SET @current_commandlog_id = SCOPE_IDENTITY();
                END TRY
                BEGIN CATCH
                    SET @log_error_msg = LEFT(ERROR_MESSAGE(), 3900);
                    RAISERROR(N'  WARNING: Failed to log start to CommandLog (%s). Continuing execution.', 10, 1, @log_error_msg) WITH NOWAIT;
                END CATCH;
            END;

            /* #163 (P2): Deadlock retry -- up to 3 total attempts with exponential backoff on error 1205.
               ROWLOCK+READPAST skips locked rows but cannot prevent Sch-M conflicts when multiple
               workers update the same stat simultaneously. Retry loop handles that residual race. */
            DECLARE
                @exec_retry      int    = 0,
                @exec_retry_delay char(8),
                @exec_done       bit    = 0;

            BEGIN TRY
                WHILE @exec_done = 0
                BEGIN
                    BEGIN TRY
                        EXECUTE sys.sp_executesql
                            @current_command;
                        SET @exec_done = 1; /* Success -- exit retry loop */
                    END TRY
                    BEGIN CATCH
                        IF ERROR_NUMBER() = 1205 AND @exec_retry < 2
                        BEGIN
                            /* Deadlock -- back off and retry */
                            SET @exec_retry_delay =
                                CASE @exec_retry WHEN 0 THEN '00:00:01' WHEN 1 THEN '00:00:02' ELSE '00:00:04' END;
                            SET @exec_retry += 1;
                            RAISERROR(N'  ~ Deadlock on stat update (retry %d/2) -- waiting %s before retry (#163)',
                                10, 1, @exec_retry, @exec_retry_delay) WITH NOWAIT;
                            WAITFOR DELAY @exec_retry_delay;
                        END
                        ELSE
                        BEGIN
                            /* Non-deadlock error, or all retries exhausted -- re-raise to outer CATCH */
                            IF @exec_retry > 0
                                SET @warnings = @warnings
                                    + N'DEADLOCK_RETRY_FAIL: [' + @current_schema_name + N'].[' + @current_table_name + N'].[' + @current_stat_name + N'] '
                                    + N'deadlocked, failed after ' + CONVERT(nvarchar(5), @exec_retry + 1) + N' attempt(s); ';
                            SET @exec_done = 1; /* Force loop exit before re-raise */
                            ;THROW; /* Propagates to outer BEGIN CATCH */
                        END;
                    END CATCH;
                END; /* WHILE @exec_done = 0 */

                /* Emit note if stat update succeeded after one or more deadlock retries */
                IF @exec_retry > 0
                BEGIN
                    RAISERROR(N'  ~ Stat update succeeded after %d deadlock retry(s) (#163)', 10, 1, @exec_retry) WITH NOWAIT;
                    SET @warnings = @warnings
                        + N'DEADLOCK_RETRY_OK: [' + @current_schema_name + N'].[' + @current_table_name + N'].[' + @current_stat_name + N'] '
                        + N'succeeded after ' + CONVERT(nvarchar(5), @exec_retry) + N' retry(s); ';
                END;

                SELECT
                    @current_end_time = SYSDATETIME(),
                    @stats_succeeded += 1,
                    @total_pages_processed += ISNULL(@current_page_count, 0),
                    @consecutive_failures = 0, /* Reset on success */
                    @claimed_table_stats_updated += CASE WHEN @StatsInParallel = N'Y' THEN 1 ELSE 0 END,
                    @duration_ms = DATEDIFF(MILLISECOND, @current_start_time, @current_end_time);

                /* P3e (v2.4): Update ETR running totals */
                SET @etr_total_ms += @duration_ms;
                SET @etr_completed += 1;
                SET @etr_suffix = N'';

                /* P3e (v2.4): Compute ETR after at least 3 stats completed (avoids wild early estimates) */
                IF @etr_completed >= 3
                BEGIN
                    SET @etr_avg_ms = @etr_total_ms / @etr_completed;
                    SET @etr_remaining_count = @total_stats - @stats_processed;
                    IF @etr_remaining_count > 0
                    BEGIN
                        SET @etr_ms = CONVERT(bigint, @etr_remaining_count) * @etr_avg_ms;
                        IF @etr_ms >= 3600000
                            SET @etr_display = CONVERT(nvarchar(10), @etr_ms / 3600000) + N'h ' + CONVERT(nvarchar(10), (@etr_ms % 3600000) / 60000) + N'm';
                        ELSE IF @etr_ms >= 60000
                            SET @etr_display = CONVERT(nvarchar(10), @etr_ms / 60000) + N'm';
                        ELSE
                            SET @etr_display = CONVERT(nvarchar(10), @etr_ms / 1000) + N's';
                        SET @etr_suffix = N' | ETR: ~' + @etr_display;
                    END;
                END;

                SET @progress_msg =
                    N'  Complete (' +
                    CONVERT(nvarchar(10), @duration_ms / 1000) + N'.' +
                    CONVERT(nvarchar(10), (@duration_ms % 1000) / 100) + N's)' +
                    @etr_suffix;

                /* v2.3: Update LastStatCompletedAt to track per-stat heartbeat for dead worker detection */
                /* Uses dynamic SQL to avoid compile-time column validation when column doesn't exist yet */
                /* P2d fix (v2.4): Nested TRY/CATCH -- heartbeat write failure is non-fatal.
                   Without this, a transient table lock or column error would fall into the outer CATCH,
                   mark a successfully-completed stat as FAILED, and potentially trip @i_max_consecutive_failures. */
                IF  @StatsInParallel = N'Y'
                AND @claimed_table_database IS NOT NULL
                AND OBJECT_ID(N'dbo.QueueStatistic', N'U') IS NOT NULL
                AND @has_heartbeat_col = 1
                BEGIN
                    BEGIN TRY
                        EXEC sp_executesql
                            N'UPDATE dbo.QueueStatistic SET LastStatCompletedAt = SYSDATETIME() WHERE QueueID = @qid AND DatabaseName = @db AND SchemaName = @sch AND ObjectName = @obj;',
                            N'@qid int, @db sysname, @sch sysname, @obj sysname',
                            @qid = @queue_id,
                            @db = @claimed_table_database,
                            @sch = @claimed_table_schema,
                            @obj = @claimed_table_name;
                    END TRY
                    BEGIN CATCH
                        /* Heartbeat failure is non-fatal -- the stat update succeeded. Log warning only. */
                        DECLARE @heartbeat_err nvarchar(500) = LEFT(ERROR_MESSAGE(), 500);
                        RAISERROR(N'[sp_StatUpdate] WARNING: Heartbeat write failed (%s). Stat update was successful.', 0, 1, @heartbeat_err) WITH NOWAIT;
                    END CATCH;
                END;

                RAISERROR(@progress_msg, 10, 1) WITH NOWAIT;

                /*
                Update CommandLog with success (two-phase pattern: pre-exec INSERT already done above).
                Rows with NULL EndTime are visible to other sessions during execution -- query with:
                  SELECT * FROM dbo.CommandLog WHERE EndTime IS NULL AND CommandType = 'UPDATE_STATISTICS';
                */
                BEGIN TRY
                IF  @LogToTable = N'Y'
                AND @commandlog_exists = 1
                AND @current_commandlog_id IS NOT NULL
                BEGIN
                    /*
                    Build ExtendedInfo XML for CommandLog.
                    NOTE: RunLabel is intentionally denormalized into each stat's ExtendedInfo.
                    This enables simple correlation queries without complex joins:
                      SELECT * FROM CommandLog WHERE ExtendedInfo.value('...RunLabel...') = 'server_20260128_123456'
                    Trade-off: ~50 bytes per row vs. join complexity. Acceptable for maintenance logs.
                    */
                    SELECT
                        @current_extended_info =
                        (
                            SELECT
                                @current_object_id AS ObjectId,
                                @current_stats_id AS StatsId,
                                @current_modification_counter AS ModificationCounter,
                                CASE
                                    WHEN @current_row_count > 0
                                    THEN CONVERT(decimal(18, 2), (@current_modification_counter * 100.0 / @current_row_count))
                                    ELSE 0
                                END AS ModificationPct,
                                @current_days_stale AS DaysStale,
                                @current_page_count AS PageCount,
                                @current_page_count / 128 AS SizeMB,
                                @current_row_count AS [RowCount],
                                @current_no_recompute AS HasNorecompute,
                                @current_is_incremental AS IsIncremental,
                                @current_is_heap AS IsHeap,
                                ISNULL(@current_forwarded_records, 0) AS ForwardedRecords,
                                @current_is_memory_optimized AS IsMemoryOptimized,
                                @current_auto_created AS AutoCreated,
                                @current_histogram_steps AS HistogramSteps,
                                /* Persisted sample metadata (P1c, v2.4) */
                                @current_persisted_sample_percent AS PersistedSamplePercent,
                                @absolute_sampled_rows AS PersistedSampledRows,
                                /* Filtered statistics metadata */
                                @current_has_filter AS HasFilter,
                                LEFT(@current_filter_definition, 500) AS FilterDefinition, /*truncate for XML*/
                                @current_unfiltered_rows AS UnfilteredRows,
                                @current_filtered_drift_ratio AS FilteredDriftRatio,
                                /* Query Store priority metadata */
                                @current_qs_plan_count AS QSPlanCount,
                                @current_qs_total_executions AS QSTotalExecutions,
                                @current_qs_total_cpu_ms AS QSTotalCpuMs,
                                @current_qs_total_duration_ms AS QSTotalDurationMs,
                                @current_qs_total_logical_reads AS QSTotalLogicalReads,
                                @current_qs_total_memory_grant_kb AS QSTotalMemoryGrantKB,
                                @current_qs_total_tempdb_pages AS QSTotalTempdbPages,
                                @current_qs_total_physical_reads AS QSTotalPhysicalReads,
                                @current_qs_total_logical_writes AS QSTotalLogicalWrites,
                                @current_qs_total_wait_time_ms AS QSTotalWaitTimeMs,
                                @current_qs_max_dop AS QSMaxDOP,
                                @current_qs_active_feedback_count AS QSActiveFeedbackCount,
                                @current_qs_last_execution AS QSLastExecution,
                                @current_qs_priority_boost AS QSPriorityBoost,
                                @i_qs_metric AS QSMetric,
                                CASE
                                    WHEN @in_mop_up = 1
                                    THEN N'MOP_UP'
                                    WHEN @mode = N'DIRECT_STRING'
                                    THEN N'DIRECT_MODE'
                                    WHEN @current_qs_priority_boost > 0
                                    THEN N'QUERY_STORE_PRIORITY'
                                    WHEN @current_has_filter = 1
                                    AND  @i_filtered_stats_mode = N'PRIORITY'
                                    AND  @current_filtered_drift_ratio >= @i_filtered_stats_stale_factor
                                    THEN N'FILTERED_DRIFT'
                                    WHEN @current_no_recompute = 1
                                    AND  @TargetNorecompute IN (N'Y', N'BOTH')
                                    THEN N'NORECOMPUTE_TARGET'
                                    WHEN (@current_days_stale * 24) >= ISNULL(@StaleHours, 999999)
                                    THEN N'DAYS_STALE'
                                    WHEN @i_tiered_thresholds = 1
                                    THEN N'TIERED_THRESHOLD'
                                    WHEN @i_modification_percent IS NOT NULL
                                    AND  (@current_modification_counter * 100.0 / NULLIF(@current_row_count, 0)) >= @i_modification_percent
                                    THEN N'MOD_PERCENT'
                                    WHEN @current_modification_counter >= ISNULL(@ModificationThreshold, 0)
                                    THEN N'MOD_COUNTER'
                                    ELSE N'THRESHOLD_MATCH'
                                END AS QualifyReason,
                                /* Sample rate traceability - answers "why this sample rate?" */
                                @i_statistics_sample AS RequestedSamplePct,
                                @effective_sample_percent AS EffectiveSamplePct,
                                @sample_source AS SampleSource,
                                @mode AS Mode,
                                @run_label AS RunLabel,
                                @stats_processed AS ProcessingPosition,
                                @procedure_version AS Version
                            FOR
                                XML RAW(N'ExtendedInfo'),
                                ELEMENTS
                        );

                    /* #153: Warn if command exceeds 4000 chars (older CommandLog schemas may have nvarchar(4000)) */
                    IF LEN(@current_command) > 4000
                        RAISERROR(N'  Note: UPDATE STATISTICS command is %d chars -- may truncate on older CommandLog schemas with nvarchar(4000).', 10, 1, @current_command) WITH NOWAIT;

                    UPDATE dbo.CommandLog
                    SET EndTime = @current_end_time,
                        ErrorNumber = 0,
                        ExtendedInfo = @current_extended_info
                    WHERE ID = @current_commandlog_id;

                    /*
                    Progress logging at interval (for Agent job monitoring).
                    Logs SP_STATUPDATE_PROGRESS entry every N stats processed.
                    */
                    IF  @i_progress_log_interval IS NOT NULL
                    AND @stats_processed % @i_progress_log_interval = 0
                    BEGIN
                        INSERT INTO
                            dbo.CommandLog
                        (
                            DatabaseName,
                            SchemaName,
                            ObjectName,
                            ObjectType,
                            Command,
                            CommandType,
                            StartTime,
                            EndTime,
                            ExtendedInfo
                        )
                        VALUES
                        (
                            ISNULL(@Databases, DB_NAME()),
                            N'dbo',
                            N'sp_StatUpdate',
                            N'P',
                            N'Progress: ' + CONVERT(nvarchar(10), @stats_processed) + N'/' + CONVERT(nvarchar(10), @total_stats) + N' stats processed',
                            N'SP_STATUPDATE_PROGRESS',
                            @start_time,
                            SYSDATETIME(),
                            (
                                SELECT
                                    @stats_processed AS StatsProcessed,
                                    @stats_succeeded AS StatsSucceeded,
                                    @stats_failed AS StatsFailed,
                                    @total_stats AS StatsTotal,
                                    DATEDIFF(SECOND, @start_time, SYSDATETIME()) AS ElapsedSeconds
                                FOR XML RAW(N'Progress'), ELEMENTS
                            )
                        );
                    END;
                END;
                END TRY
                BEGIN CATCH
                    /* CommandLog UPDATE failure is non-fatal: stat update succeeded. Log warning only. */
                    SET @log_error_msg = LEFT(ERROR_MESSAGE(), 3900);
                    RAISERROR(N'  WARNING: Failed to update CommandLog (%s). Stat update succeeded.', 10, 1, @log_error_msg) WITH NOWAIT;
                END CATCH;

                /*
                #168 (P2): After stat update, warn if this table has ANY forced plans in Query Store.
                Statistics changes can shift cardinality estimates causing forced plans to become
                suboptimal. This gives the DBA immediate per-stat visibility -- the end-of-run
                summary (#32) still provides the aggregate count for monitoring automation.
                Wrapped in TRY/CATCH: silently skipped when QS is disabled on the database.
                #288: Gated on @i_qs_enabled -- per-stat QS check is expensive (dynamic SQL per stat).
                bd -pf7: cached per table -- query fires once per table, not once per stat.
                A 31-stat table like TBL_2CF193 now does 1 QS query instead of 31.
                */
                IF @i_qs_enabled = 1
                BEGIN
                BEGIN TRY
                    DECLARE @qs168_count int = 0, @qs168_sql nvarchar(max), @qs168_msg nvarchar(1000);
                    DECLARE @fp_cached_table sysname, @fp_cached_db sysname, @fp_cached_count int;

                    /* Cache hit: same table+db as last stat — reuse cached count */
                    IF  @current_table_name = @fp_cached_table
                    AND @current_database = @fp_cached_db
                    BEGIN
                        SET @qs168_count = @fp_cached_count;
                    END
                    ELSE
                    BEGIN
                        /* Cache miss: new table — query QS and cache */
                        SET @qs168_sql =
                            N'SELECT @cnt = COUNT(DISTINCT qsp.plan_id)
                              FROM ' + QUOTENAME(@current_database) + N'.sys.query_store_plan AS qsp
                              INNER JOIN ' + QUOTENAME(@current_database) + N'.sys.query_store_query AS qsq
                                  ON qsq.query_id = qsp.query_id
                              INNER JOIN ' + QUOTENAME(@current_database) + N'.sys.query_store_query_text AS qsqt
                                  ON qsqt.query_text_id = qsq.query_text_id
                              WHERE qsp.is_forced_plan = 1
                              AND qsp.force_failure_count = 0
                              AND CHARINDEX(@tbl COLLATE DATABASE_DEFAULT,
                                            qsqt.query_sql_text COLLATE DATABASE_DEFAULT) > 0';
                        EXEC sp_executesql @qs168_sql,
                            N'@tbl sysname, @cnt int OUTPUT',
                            @tbl = @current_table_name,
                            @cnt = @qs168_count OUTPUT;
                        SET @fp_cached_table = @current_table_name;
                        SET @fp_cached_db = @current_database;
                        SET @fp_cached_count = ISNULL(@qs168_count, 0);
                    END;

                    IF ISNULL(@qs168_count, 0) > 0
                    BEGIN
                        SET @qs168_msg =
                            N'WARNING: Stat update on [' + @current_schema_name + N'].[' + @current_table_name
                            + N'] -- ' + CONVERT(nvarchar(10), @qs168_count)
                            + N' forced plan(s) exist in Query Store. Verify query plans remain optimal after statistics change. (#168)';
                        RAISERROR(N'%s', 10, 1, @qs168_msg) WITH NOWAIT;
                        SET @warnings = @warnings
                            + N'QS_FORCED_PLANS_STAT: [' + @current_schema_name + N'].[' + @current_table_name + N'].[' + @current_stat_name + N'] '
                            + CONVERT(nvarchar(10), @qs168_count) + N' forced plan(s); ';
                    END;
                END TRY
                BEGIN CATCH /* QS not enabled or not accessible on this database -- skip */ END CATCH;
                END; /* IF @i_qs_enabled = 1 (#288) */

            END TRY
            BEGIN CATCH
                SELECT
                    @current_end_time = SYSDATETIME(),
                    @current_error_number = ERROR_NUMBER(),
                    @current_error_message = ERROR_MESSAGE();

                /*
                TOCTOU carve-out: errors 208 (invalid object name), 15009 (object not found),
                and 2767 (statistics not found) indicate the object/stat was dropped between
                queue-load and execution -- a race condition, not a real infrastructure failure.
                Count separately from real failures to avoid misleading summary output (#222).
                */
                IF @current_error_number IN (208, 15009, 2767)
                BEGIN
                    SELECT @stats_toctou += 1;
                    RAISERROR(N'  ~ TOCTOU skip (error %d): object/statistic no longer exists -- not counted as failure.', 10, 1, @current_error_number) WITH NOWAIT;
                END;
                ELSE
                BEGIN
                    SELECT
                        @stats_failed += 1,
                        @consecutive_failures += 1,
                        @claimed_table_stats_failed += CASE WHEN @StatsInParallel = N'Y' THEN 1 ELSE 0 END;
                    RAISERROR(N'  X Error %d: %s', 16, 1, @current_error_number, @current_error_message) WITH NOWAIT;
                END;

                /*
                Update CommandLog with error (two-phase pattern: pre-exec INSERT already done above)
                */
                IF  @LogToTable = N'Y'
                AND @commandlog_exists = 1
                AND @current_commandlog_id IS NOT NULL
                BEGIN
                    BEGIN TRY
                        UPDATE dbo.CommandLog
                        SET EndTime = @current_end_time,
                            ErrorNumber = @current_error_number,
                            ErrorMessage = @current_error_message
                        WHERE ID = @current_commandlog_id;
                    END TRY
                    BEGIN CATCH
                        SELECT @log_error_msg = LEFT(ERROR_MESSAGE(), 3900);
                        RAISERROR(N'  WARNING: Failed to log error to CommandLog (%s)', 10, 1, @log_error_msg) WITH NOWAIT;
                    END CATCH;
                END;

                SELECT
                    @return_code = @current_error_number;

                /*
                I/O corruption (823/824/825): Abort immediately and advise CHECKDB (#222).
                Continuing stats maintenance on a database with I/O corruption risks further damage.
                */
                IF @current_error_number IN (823, 824, 825)
                BEGIN
                    RAISERROR(N'', 10, 1) WITH NOWAIT;
                    RAISERROR(N'CRITICAL: I/O corruption detected (error %d). Aborting stats maintenance.', 16, 1, @current_error_number) WITH NOWAIT;
                    RAISERROR(N'  Run DBCC CHECKDB on database [%s] to assess corruption extent.', 10, 1, @current_database) WITH NOWAIT;
                    SELECT @stop_reason = N'IO_CORRUPTION';
                    SET @warnings = @warnings + N'IO_CORRUPTION: Error ' + CONVERT(nvarchar(10), @current_error_number) + N' on [' + @current_database + N']; ';
                    BREAK;
                END;

                /*
                FailFast: Abort on first error if enabled
                */
                IF @FailFast = 1
                BEGIN
                    RAISERROR(N'', 10, 1) WITH NOWAIT;
                    RAISERROR(N'FailFast enabled. Aborting due to error.', 16, 1) WITH NOWAIT;
                    SELECT @stop_reason = N'FAIL_FAST';
                    BREAK;
                END;

                /*
                MaxConsecutiveFailures: Abort after N consecutive failures
                Prevents cascading issues from shared resource problems (disk, memory, locks).
                */
                IF @i_max_consecutive_failures IS NOT NULL AND @consecutive_failures >= @i_max_consecutive_failures
                BEGIN
                    RAISERROR(N'', 10, 1) WITH NOWAIT;
                    RAISERROR(N'Aborting: %d consecutive failures reached. Last error %d: %s', 16, 1, @consecutive_failures, @current_error_number, @current_error_message) WITH NOWAIT;
                    SELECT @stop_reason = N'CONSECUTIVE_FAILURES';
                    BREAK;
                END;
            END CATCH;
        END;
        ELSE
        BEGIN
            /*
            Dry run - just show command
            */
            RAISERROR(N'  [DRY RUN] %s', 10, 1, @current_command) WITH NOWAIT;

            /*
            If @WhatIfOutputTable is specified, insert the command into that table
            */
            IF @WhatIfOutputTable IS NOT NULL
            BEGIN
                /* P1b fix: use @safe_wio_fqn (PARSENAME+QUOTENAME validated) instead of raw @WhatIfOutputTable */
                DECLARE @whatif_insert_sql nvarchar(max) = N'
                    INSERT INTO ' + @safe_wio_fqn + N'
                    (DatabaseName, SchemaName, TableName, StatName, Command, ModificationCounter, DaysStale, PageCount)
                    VALUES (@db, @schema, @table, @stat, @cmd, @mods, @days, @pages)';

                EXEC sys.sp_executesql
                    @whatif_insert_sql,
                    N'@db sysname, @schema sysname, @table sysname, @stat sysname, @cmd nvarchar(max), @mods bigint, @days int, @pages bigint',
                    @db = @current_database,
                    @schema = @current_schema_name,
                    @table = @current_table_name,
                    @stat = @current_stat_name,
                    @cmd = @current_command,
                    @mods = @current_modification_counter,
                    @days = @current_days_stale,
                    @pages = @current_page_count;
            END;

            SELECT
                @stats_skipped += 1,
                @claimed_table_stats_skipped += CASE WHEN @StatsInParallel = N'Y' THEN 1 ELSE 0 END;
        END;

        /*
        Delay between stats if specified.
        P2 fix (#210): Check @i_time_limit BEFORE sleeping. Previously the WAITFOR executed
        unconditionally, causing overshoot equal to the full delay duration even after
        the time budget was exhausted. Now we skip the sleep (and break) when time is up.
        */
        IF  @i_delay_between_stats IS NOT NULL
        AND @i_delay_between_stats > 0
        AND @Execute = N'Y'
        BEGIN
            /* Skip the sleep and exit the loop if we are already at or past the time limit */
            IF @i_time_limit IS NOT NULL AND DATEDIFF(SECOND, @start_time, SYSDATETIME()) >= @i_time_limit
            BEGIN
                RAISERROR(N'Time limit (%d seconds) reached (pre-delay check). Stopping gracefully.', 10, 1, @i_time_limit) WITH NOWAIT;
                SELECT @stop_reason = N'TIME_LIMIT';
                BREAK;
            END;

            DECLARE
                @delay_time datetime = DATEADD(MILLISECOND, CONVERT(int, @i_delay_between_stats * 1000), '00:00:00');

            WAITFOR DELAY @delay_time;
        END;

        /*
        Clear for next iteration
        */
        SELECT
            @current_command = NULL,
            @current_error_number = NULL,
            @current_error_message = NULL,
            @current_extended_info = NULL;

        /*#endregion 09F-EXECUTE */
        /*#region 09G-PARALLEL-COMPLETE: Check claimed table finished, UPDATE QueueStatistic TableEndTime */
        /*
        ====================================================================
        PARALLEL MODE: Check if we're done with the current table
        ====================================================================
        If no more unprocessed stats for the claimed table, mark it complete
        and release the claim so we can claim the next table.
        */
        IF  @StatsInParallel = N'Y'
        AND @claimed_table_database IS NOT NULL
        BEGIN
            DECLARE
                @remaining_table_stats integer =
                (
                    SELECT
                        COUNT_BIG(*)
                    FROM #stats_to_process AS stp
                    WHERE stp.processed = 0
                    AND   stp.database_name = @claimed_table_database
                    AND   stp.schema_name = @claimed_table_schema
                    AND   stp.table_name = @claimed_table_name
                );

            IF @remaining_table_stats = 0
            BEGIN
                /*
                Mark table as complete in QueueStatistic
                */
                UPDATE
                    qs
                SET
                    qs.TableEndTime = SYSDATETIME(),
                    qs.StatsUpdated = @claimed_table_stats_updated,
                    qs.StatsFailed = @claimed_table_stats_failed,
                    qs.StatsSkipped = @claimed_table_stats_skipped
                FROM dbo.QueueStatistic AS qs
                WHERE qs.QueueID = @queue_id
                AND   qs.DatabaseName = @claimed_table_database
                AND   qs.SchemaName = @claimed_table_schema
                AND   qs.ObjectName = @claimed_table_name;

                IF @Debug = 1
                BEGIN
                    RAISERROR(N'  Completed table: %s.%s.%s (updated=%d, failed=%d, skipped=%d)', 10, 1,
                        @claimed_table_database,
                        @claimed_table_schema,
                        @claimed_table_name,
                        @claimed_table_stats_updated,
                        @claimed_table_stats_failed,
                        @claimed_table_stats_skipped) WITH NOWAIT;
                END;

                /*
                Release claim so next iteration will claim a new table
                */
                SELECT
                    @claimed_table_database = NULL,
                    @claimed_table_schema = NULL,
                    @claimed_table_name = NULL,
                    @claimed_table_object_id = NULL;
            END;
        END;
    END;
    /*#endregion 09G-PARALLEL-COMPLETE */
    /*#endregion 09-PROCESS-LOOP */
    /* gh-462: Shared WHERE filter for mop-up discovery (parallel-leader and serial).
       Both paths concatenate @mop_up_where_sql then append @commandlog_3part + tail.
       Any filter change needs updating only here -- one source of truth. */
    DECLARE @mop_up_where_sql nvarchar(max) = CAST(N'
                WHERE ISNULL(sp.modification_counter, 0) > 0
                AND   (
                          OBJECTPROPERTY(s.object_id, N''''IsUserTable'''') = 1
                       OR @i_include_system_objects_param = N''''Y''''
                       OR (o.type = N''''V'''' AND @i_include_indexed_views_param = N''''Y''''
                           AND EXISTS (SELECT 1 FROM sys.indexes AS vi WHERE vi.object_id = s.object_id AND vi.index_id = 1))
                      )
                AND   (o.is_ms_shipped = 0 OR @i_include_system_objects_param = N''''Y'''')
                AND   o.type NOT IN (N''''ET'''', N''''S'''')
                /* v2.27: Stretch Database auto-skip (#55) */
                AND   ISNULL(OBJECTPROPERTY(s.object_id, N''''TableHasRemoteDataArchive''''), 0) = 0
                /* v2.27: Skip tables on READ_ONLY filegroups (#65) */
                AND   NOT EXISTS
                      (
                          SELECT 1
                          FROM sys.indexes AS ri
                          JOIN sys.data_spaces AS rds ON rds.data_space_id = ri.data_space_id
                          JOIN sys.filegroups AS rfg ON rfg.data_space_id = rds.data_space_id
                          WHERE ri.object_id = s.object_id
                          AND   ri.index_id IN (0, 1)
                          AND   rfg.is_read_only = 1
                      )
                AND   (
                          (@TargetNorecompute_param = N''''N'''' AND s.no_recompute = 0)
                       OR (@TargetNorecompute_param = N''''Y'''' AND s.no_recompute = 1)
                       OR @TargetNorecompute_param = N''''BOTH''''
                      )
                /* v2.27: Table inclusion filter */
                AND   (
                          @Tables_param IS NULL
                       OR OBJECT_SCHEMA_NAME(s.object_id) + N''''.'''' + OBJECT_NAME(s.object_id) COLLATE DATABASE_DEFAULT IN
                          (SELECT LTRIM(RTRIM(ss.value)) COLLATE DATABASE_DEFAULT FROM STRING_SPLIT(@Tables_param, N'''','''') AS ss)
                       OR OBJECT_NAME(s.object_id) COLLATE DATABASE_DEFAULT IN
                          (SELECT LTRIM(RTRIM(ss.value)) COLLATE DATABASE_DEFAULT FROM STRING_SPLIT(@Tables_param, N'''','''') AS ss)
                      )
                /* v2.26: Table exclusion filter (was missing from mop-up) */
                AND   (
                          @ExcludeTables_param IS NULL
                       OR NOT EXISTS
                          (
                              SELECT 1
                              FROM STRING_SPLIT(@ExcludeTables_param, N'''','''') AS ex
                              WHERE OBJECT_SCHEMA_NAME(s.object_id) + N''''.'''' + OBJECT_NAME(s.object_id) COLLATE DATABASE_DEFAULT LIKE LTRIM(RTRIM(ex.value)) COLLATE DATABASE_DEFAULT
                          )
                      )
                /* v2.26: Statistics exclusion filter (was missing from mop-up) */
                AND   (
                          @ExcludeStatistics_param IS NULL
                       OR NOT EXISTS
                          (
                              SELECT 1
                              FROM STRING_SPLIT(@ExcludeStatistics_param, N'''','''') AS ex
                              WHERE s.name COLLATE DATABASE_DEFAULT LIKE LTRIM(RTRIM(ex.value)) COLLATE DATABASE_DEFAULT
                                 OR s.name COLLATE DATABASE_DEFAULT = LTRIM(RTRIM(ex.value)) COLLATE DATABASE_DEFAULT
                          )
                      )
                /* v2.27: Filtered stats mode filter */
                AND   (
                          @i_filtered_stats_mode_param = N''''INCLUDE''''
                       OR @i_filtered_stats_mode_param = N''''PRIORITY''''
                       OR (@i_filtered_stats_mode_param = N''''EXCLUDE'''' AND s.has_filter = 0)
                       OR (@i_filtered_stats_mode_param = N''''ONLY'''' AND s.has_filter = 1)
                      )
                /* v2.27: Skip tables with columnstore indexes */
                AND   (
                          @i_skip_tables_with_columnstore_param = N''''N''''
                       OR NOT EXISTS
                          (
                              SELECT 1
                              FROM sys.indexes AS ci
                              WHERE ci.object_id = s.object_id
                              AND   ci.type IN (5, 6)
                          )
                      )
                /* v2.27: Minimum page count filter */
                AND   (@i_min_page_count_param IS NULL OR ISNULL(pgs.total_pages, 0) >= @i_min_page_count_param)
                AND   NOT EXISTS (
                    /* Only exclude stats still pending in priority queue (processed = 0).
                       Failed/completed stats (processed = 1) are eligible for mop-up --
                       the CommandLog NOT EXISTS below handles deduplication. */
                    SELECT 1 FROM #stats_to_process AS stp
                    WHERE stp.database_name = DB_NAME() COLLATE DATABASE_DEFAULT
                    AND   stp.object_id = s.object_id
                    AND   stp.stats_id = s.stats_id
                    AND   stp.processed = 0
                )
                AND   NOT EXISTS (
                    SELECT 1 FROM ' + @commandlog_3part + N' AS cl
' AS nvarchar(max));

    /*#region 10-MOP-UP: Broad sweep with remaining time budget */
    /*
    ============================================================================
    MOP-UP PASS (v2.24)
    ============================================================================
    After the priority pass completes normally, use remaining TimeLimit for a
    broad sweep: any stat with modification_counter > 0 that was not updated
    in this run (checked via CommandLog).  No tiered thresholds, no QS
    enrichment -- just raw modification count ordering.

    Triggers when:
      - @i_mop_up_pass = 'Y'
      - Priority pass completed (COMPLETED, NATURAL_END, or PARALLEL_COMPLETE)
      - Remaining time >= @i_mop_up_min_remaining
      - @LogToTable = 'Y' and CommandLog exists (needed to skip recent updates)

    In parallel mode, the first worker to finish uses sp_getapplock to serialize
    mop-up discovery.  It populates QueueStatistic with mop-up tables so all
    workers can claim them via the normal parallel queue.  Workers that did not
    run discovery use lazy per-table discovery when claiming mop-up tables.
    */
    IF  @i_mop_up_pass = N'Y'
    AND @mop_up_done = 0
    AND @stop_reason IN (N'COMPLETED', N'NATURAL_END', N'PARALLEL_COMPLETE')
    AND @i_time_limit IS NOT NULL
    AND (DATEDIFF(SECOND, @start_time, SYSDATETIME()) + @i_mop_up_min_remaining) <= @i_time_limit
    AND @LogToTable = N'Y'
    AND @commandlog_exists = 1
    AND @Execute = N'Y'
    BEGIN
        SET @mop_up_done = 1;

        DECLARE
            @mop_up_remaining_seconds int = @i_time_limit - DATEDIFF(SECOND, @start_time, SYSDATETIME()),
            @mop_up_found int = 0,
            @mop_up_sql nvarchar(max),
            @mop_up_params nvarchar(max),
            @mop_up_db_idx int,
            @mop_up_db sysname;

        /*
        #340: Pre-flight safety checks before mop-up discovery.
        If conditions degraded during the priority pass, skip mop-up.
        */
        IF @i_min_tempdb_free_mb IS NOT NULL
        BEGIN
            DECLARE @mop_tempdb_free_mb decimal(10, 2);
            SELECT @mop_tempdb_free_mb = SUM(unallocated_extent_page_count) * 8 / 1024.0
            FROM tempdb.sys.dm_db_file_space_usage;

            IF @mop_tempdb_free_mb < @i_min_tempdb_free_mb
            BEGIN
                DECLARE @mop_tempdb_msg nvarchar(200) =
                    N'Mop-up skipped: tempdb free space ' + CONVERT(nvarchar(20), CONVERT(int, @mop_tempdb_free_mb)) +
                    N' MB below threshold ' + CONVERT(nvarchar(20), @i_min_tempdb_free_mb) + N' MB.';
                RAISERROR(@mop_tempdb_msg, 10, 1) WITH NOWAIT;
                GOTO SkipMopUp;
            END;
        END;

        IF  @i_max_ag_redo_queue_mb IS NOT NULL
        AND @ag_is_primary = 1
        BEGIN
            DECLARE @mop_ag_redo_mb decimal(10, 2);
            SELECT @mop_ag_redo_mb = MAX(drs.redo_queue_size) / 1024.0
            FROM sys.dm_hadr_database_replica_states AS drs
            INNER JOIN sys.availability_replicas AS ar
                ON ar.replica_id = drs.replica_id
            WHERE drs.is_local = 0
            AND   drs.is_primary_replica = 0
            AND   drs.redo_queue_size IS NOT NULL
            AND   ar.availability_mode = 1; /* SYNCHRONOUS_COMMIT */

            IF @mop_ag_redo_mb IS NOT NULL AND @mop_ag_redo_mb > @i_max_ag_redo_queue_mb
            BEGIN
                DECLARE @mop_ag_msg nvarchar(200) =
                    N'Mop-up skipped: AG sync redo queue ' + CONVERT(nvarchar(20), CONVERT(int, @mop_ag_redo_mb)) +
                    N' MB exceeds threshold ' + CONVERT(nvarchar(20), @i_max_ag_redo_queue_mb) + N' MB.';
                RAISERROR(@mop_ag_msg, 10, 1) WITH NOWAIT;
                GOTO SkipMopUp;
            END;
        END;

        /*
        ====================================================================
        PARALLEL MOP-UP: Serialize discovery, populate shared queue
        ====================================================================
        */
        IF @StatsInParallel = N'Y'
        BEGIN
            SET @mop_lock_resource = N'sp_StatUpdate_MopUp_' + CONVERT(nvarchar(10), @queue_id);

            /* Non-blocking try -- check if we can be the mop-up leader */
            EXEC @mop_lock_result = sp_getapplock
                @Resource = @mop_lock_resource,
                @LockMode = N'Exclusive',
                @LockOwner = N'Session',
                @LockTimeout = 0;

            IF @mop_lock_result >= 0
            BEGIN
                /* WE ARE THE MOP-UP LEADER -- run full discovery */
                RAISERROR(N'', 10, 1) WITH NOWAIT;
                RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
                RAISERROR(N' Mop-Up Pass (parallel leader): %d seconds remaining', 10, 1, @mop_up_remaining_seconds) WITH NOWAIT;
                RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
                RAISERROR(N'', 10, 1) WITH NOWAIT;

                DECLARE @mop_up_db_list TABLE (idx int IDENTITY(1,1) PRIMARY KEY, database_name sysname NOT NULL);
                INSERT INTO @mop_up_db_list (database_name) SELECT DISTINCT database_name FROM #stats_to_process;

                INSERT INTO @mop_up_db_list (database_name)
                SELECT td.DatabaseName
                FROM @tmpDatabases AS td
                WHERE td.Selected = 1
                AND NOT EXISTS (SELECT 1 FROM @mop_up_db_list AS ml WHERE ml.database_name = td.DatabaseName);

                SELECT @mop_up_db_idx = MIN(idx) FROM @mop_up_db_list;

                WHILE @mop_up_db_idx IS NOT NULL
                BEGIN
                    SELECT @mop_up_db = database_name FROM @mop_up_db_list WHERE idx = @mop_up_db_idx;

                    /* v2.26: CAST first element to nvarchar(max) to prevent 4000 char truncation during concatenation */
                    SET @mop_up_sql = CAST(N'
                    USE ' AS nvarchar(max)) + QUOTENAME(@mop_up_db) + N';

                    SELECT
                        database_name = DB_NAME(),
                        schema_name = OBJECT_SCHEMA_NAME(s.object_id),
                        table_name = OBJECT_NAME(s.object_id),
                        stat_name = s.name,
                        object_id = s.object_id,
                        stats_id = s.stats_id,
                        no_recompute = s.no_recompute,
                        is_incremental = s.is_incremental,
                        is_memory_optimized = ISNULL(t.is_memory_optimized, 0),
                        is_heap = CASE WHEN EXISTS (SELECT 1 FROM sys.indexes AS ix WHERE ix.object_id = s.object_id AND ix.index_id = 0) THEN 1 ELSE 0 END,
                        auto_created = s.auto_created,
                        modification_counter = ISNULL(sp.modification_counter, 0),
                        row_count = ISNULL(sp.rows, 0),
                        days_stale = ISNULL(DATEDIFF(DAY, sp.last_updated, SYSDATETIME()), 9999),
                        page_count = ISNULL(pgs.total_pages, 0),
                        persisted_sample_percent = sp.persisted_sample_percent,
                        histogram_steps = sp.steps,
                        has_filter = s.has_filter,
                        filter_definition = s.filter_definition,
                        unfiltered_rows = sp.unfiltered_rows,
                        qs_plan_count = CONVERT(int, NULL),
                        qs_total_executions = CONVERT(bigint, NULL),
                        qs_total_cpu_ms = CONVERT(bigint, NULL),
                        qs_total_duration_ms = CONVERT(bigint, NULL),
                        qs_total_logical_reads = CONVERT(bigint, NULL),
                        qs_total_memory_grant_kb = CONVERT(bigint, NULL),
                        qs_total_tempdb_pages = CONVERT(bigint, NULL),
                        qs_total_physical_reads = CONVERT(bigint, NULL),
                        qs_total_logical_writes = CONVERT(bigint, NULL),
                        qs_total_wait_time_ms = CONVERT(bigint, NULL),
                        qs_max_dop = CONVERT(smallint, NULL),
                        qs_active_feedback_count = CONVERT(int, NULL),
                        qs_last_execution = CONVERT(datetime2, NULL),
                        qs_priority_boost = CONVERT(bigint, 0),
                        is_published = ISNULL(t.is_published, 0),
                        is_tracked_by_cdc = ISNULL(t.is_tracked_by_cdc, 0),
                        temporal_type = ISNULL(t.temporal_type, 0),
                        priority = ROW_NUMBER() OVER (ORDER BY ISNULL(sp.modification_counter, 0) DESC, s.object_id ASC, s.stats_id ASC)
                    FROM sys.stats AS s
                    JOIN sys.objects AS o ON o.object_id = s.object_id
                    LEFT JOIN sys.tables AS t ON t.object_id = s.object_id
                    CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
                    OUTER APPLY (
                        SELECT total_pages = SUM(p.used_page_count)
                        FROM sys.dm_db_partition_stats AS p
                        WHERE p.object_id = s.object_id AND p.index_id IN (0, 1)
                    ) AS pgs
                    ' + @mop_up_where_sql + N'
                        WHERE cl.CommandType = N''UPDATE_STATISTICS''
                        AND   cl.DatabaseName = DB_NAME() COLLATE DATABASE_DEFAULT
                        AND   cl.SchemaName = OBJECT_SCHEMA_NAME(s.object_id) COLLATE DATABASE_DEFAULT
                        AND   cl.ObjectName = OBJECT_NAME(s.object_id) COLLATE DATABASE_DEFAULT
                        AND   cl.StatisticsName = s.name COLLATE DATABASE_DEFAULT
                        AND   cl.StartTime >= @start_time_param
                        AND   cl.ErrorNumber = 0
                    )
                    ORDER BY priority
                    OPTION (RECOMPILE);';

                    SET @mop_up_params = N'
                        @i_include_system_objects_param nvarchar(1),
                        @i_include_indexed_views_param nvarchar(1),
                        @TargetNorecompute_param nvarchar(10),
                        @Tables_param nvarchar(max),
                        @ExcludeTables_param nvarchar(max),
                        @ExcludeStatistics_param nvarchar(max),
                        @i_filtered_stats_mode_param nvarchar(10),
                        @i_skip_tables_with_columnstore_param nchar(1),
                        @i_min_page_count_param bigint,
                        @start_time_param datetime2(7)';

                    IF @Debug = 1
                    BEGIN
                        DECLARE @mop_up_sql_len_p int = LEN(@mop_up_sql);
                        RAISERROR(N'  Mop-up SQL for %s (%d chars)', 10, 1, @mop_up_db, @mop_up_sql_len_p) WITH NOWAIT;
                    END;

                    BEGIN TRY
                        INSERT INTO #stats_to_process
                        (
                            database_name, schema_name, table_name, stat_name, object_id, stats_id,
                            no_recompute, is_incremental, is_memory_optimized, is_heap, auto_created,
                            modification_counter, row_count, days_stale, page_count, persisted_sample_percent, histogram_steps,
                            has_filter, filter_definition, unfiltered_rows,
                            qs_plan_count, qs_total_executions, qs_total_cpu_ms, qs_total_duration_ms,
                            qs_total_logical_reads, qs_total_memory_grant_kb, qs_total_tempdb_pages, qs_total_physical_reads, qs_total_logical_writes, qs_total_wait_time_ms, qs_max_dop, qs_active_feedback_count,
                            qs_last_execution, qs_priority_boost,
                            is_published, is_tracked_by_cdc, temporal_type, priority
                        )
                        EXECUTE sys.sp_executesql
                            @mop_up_sql,
                            @mop_up_params,
                            @i_include_system_objects_param = @i_include_system_objects,
                            @i_include_indexed_views_param = @i_include_indexed_views,
                            @TargetNorecompute_param = @TargetNorecompute,
                            @Tables_param = @Tables,
                            @ExcludeTables_param = @ExcludeTables,
                            @ExcludeStatistics_param = @ExcludeStatistics,
                            @i_filtered_stats_mode_param = @i_filtered_stats_mode,
                            @i_skip_tables_with_columnstore_param = @i_skip_tables_with_columnstore,
                            @i_min_page_count_param = @i_min_page_count,
                            @start_time_param = @start_time;
                    END TRY
                    BEGIN CATCH
                        IF @Debug = 1
                        BEGIN
                            DECLARE @mop_up_err nvarchar(500) = ERROR_MESSAGE();
                            RAISERROR(N'  Mop-up discovery warning (%s): %s', 10, 1, @mop_up_db, @mop_up_err) WITH NOWAIT;
                        END;
                    END CATCH;

                    SELECT @mop_up_db_idx = MIN(idx) FROM @mop_up_db_list WHERE idx > @mop_up_db_idx;
                END;

                SET @mop_up_found = (SELECT COUNT(*) FROM #stats_to_process WHERE processed = 0);

                IF @mop_up_found > 0
                BEGIN
                    /* #436: Wrap DELETE+INSERT in TRY/CATCH to release mop_lock_resource on error.
                       Without this, an unhandled exception orphans the session-scoped applock. */
                    DECLARE @mop_tables_queued int = 0;
                    BEGIN TRY
                        /* Populate QueueStatistic with mop-up tables */

                        /* v2.27 (#357): Remove completed QueueStatistic entries for tables that
                           mop-up found new stats on.  Without this, the NOT EXISTS on the INSERT
                           blocks re-queuing -- tables from the priority pass (now marked complete)
                           can never transition to the mop-up queue. */
                        DELETE qs
                        FROM dbo.QueueStatistic AS qs
                        WHERE qs.QueueID = @queue_id
                        AND   qs.TableEndTime IS NOT NULL
                        AND   EXISTS (
                                  SELECT 1
                                  FROM #stats_to_process AS stp
                                  WHERE stp.processed = 0
                                  AND   stp.database_name = qs.DatabaseName COLLATE DATABASE_DEFAULT
                                  AND   stp.schema_name = qs.SchemaName COLLATE DATABASE_DEFAULT
                                  AND   stp.table_name = qs.ObjectName COLLATE DATABASE_DEFAULT
                              );

                        DECLARE @phase_a_max_priority int = ISNULL(
                            (SELECT MAX(qs.TablePriority) FROM dbo.QueueStatistic AS qs WHERE qs.QueueID = @queue_id),
                            0
                        );

                        INSERT INTO dbo.QueueStatistic
                        (
                            QueueID, DatabaseName, SchemaName, ObjectName, ObjectID,
                            TablePriority, StatsCount, MaxModificationCounter
                        )
                        SELECT
                            QueueID = @queue_id,
                            DatabaseName = stp.database_name,
                            SchemaName = stp.schema_name,
                            ObjectName = stp.table_name,
                            ObjectID = stp.object_id,
                            TablePriority = @phase_a_max_priority + ROW_NUMBER() OVER (
                                ORDER BY MAX(stp.modification_counter) DESC,
                                         stp.object_id ASC
                            ),
                            StatsCount = COUNT_BIG(*),
                            MaxModificationCounter = MAX(stp.modification_counter)
                        FROM #stats_to_process AS stp
                        WHERE stp.processed = 0
                        AND   NOT EXISTS (
                                  SELECT 1
                                  FROM dbo.QueueStatistic AS qs
                                  WHERE qs.QueueID = @queue_id
                                  AND   qs.DatabaseName = stp.database_name COLLATE DATABASE_DEFAULT
                                  AND   qs.SchemaName = stp.schema_name COLLATE DATABASE_DEFAULT
                                  AND   qs.ObjectName = stp.table_name COLLATE DATABASE_DEFAULT
                              )
                        GROUP BY
                            stp.database_name,
                            stp.schema_name,
                            stp.table_name,
                            stp.object_id;

                        SET @mop_tables_queued = ROWCOUNT_BIG();
                    END TRY
                    BEGIN CATCH
                        /* Release lock before propagating error */
                        EXEC sp_releaseapplock @Resource = @mop_lock_resource, @LockOwner = N'Session';
                        DECLARE @mop_queue_err nvarchar(4000) = ERROR_MESSAGE();
                        RAISERROR(N'Mop-up QueueStatistic error: %s', 10, 1, @mop_queue_err) WITH NOWAIT;
                        /* Fall through -- mop-up skipped but priority pass results preserved */
                        SET @mop_up_found = 0;
                    END CATCH;

                    IF @mop_up_found > 0
                    BEGIN
                        /* Release lock -- other workers can now see mop-up rows */
                        EXEC sp_releaseapplock @Resource = @mop_lock_resource, @LockOwner = N'Session';

                        RAISERROR(N'Mop-up: found %d stats across %d tables -- queued for parallel processing', 10, 1, @mop_up_found, @mop_tables_queued) WITH NOWAIT;
                        RAISERROR(N'', 10, 1) WITH NOWAIT;
                        RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
                        RAISERROR(N' Processing Mop-Up Statistics (parallel)', 10, 1) WITH NOWAIT;
                        RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
                        RAISERROR(N'', 10, 1) WITH NOWAIT;

                        SET @total_stats = @total_stats + @mop_up_found;
                        SET @stop_reason = NULL;
                        SET @in_mop_up = 1;
                        SET @mop_up_stats_found = @mop_up_found;
                        SET @mop_up_stats_processed = @stats_processed;

                        SELECT
                            @claimed_table_database = NULL,
                            @claimed_table_schema = NULL,
                            @claimed_table_name = NULL,
                            @claimed_table_object_id = NULL;

                        GOTO ProcessLoop;
                    END;
                END /* outer IF @mop_up_found > 0 */
                ELSE
                BEGIN
                    EXEC sp_releaseapplock @Resource = @mop_lock_resource, @LockOwner = N'Session';
                    RAISERROR(N'Mop-up: no additional stats found with modifications.  All stats are current.', 10, 1) WITH NOWAIT;
                END;
            END
            ELSE
            BEGIN
                /* ANOTHER WORKER IS THE MOP-UP LEADER -- wait for them to finish discovery */
                RAISERROR(N'Mop-up: waiting for leader to finish discovery...', 10, 1) WITH NOWAIT;

                EXEC @mop_lock_result = sp_getapplock
                    @Resource = @mop_lock_resource,
                    @LockMode = N'Exclusive',
                    @LockOwner = N'Session',
                    @LockTimeout = 60000;

                IF @mop_lock_result >= 0
                    EXEC sp_releaseapplock @Resource = @mop_lock_resource, @LockOwner = N'Session';

                /* Check if mop-up tables were added to QueueStatistic */
                DECLARE @mop_up_queued int = 0;
                SELECT @mop_up_queued = COUNT(*)
                FROM dbo.QueueStatistic AS qs
                WHERE qs.QueueID = @queue_id
                AND   qs.TableStartTime IS NULL
                AND   qs.TableEndTime IS NULL;

                IF @mop_up_queued > 0
                BEGIN
                    RAISERROR(N'Mop-up: joining mop-up pass (%d tables unclaimed)', 10, 1, @mop_up_queued) WITH NOWAIT;
                    RAISERROR(N'', 10, 1) WITH NOWAIT;
                    RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
                    RAISERROR(N' Processing Mop-Up Statistics (parallel follower)', 10, 1) WITH NOWAIT;
                    RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
                    RAISERROR(N'', 10, 1) WITH NOWAIT;

                    SET @stop_reason = NULL;
                    SET @in_mop_up = 1;
                    /* gh-459: follower skipped discovery, so #stats_to_process
                       has 0 unprocessed rows.  Use the queue row count that the
                       leader populated instead so MopUpFound is accurate in the
                       CommandLog END XML. */
                    SET @mop_up_stats_found = @mop_up_queued;
                    SET @mop_up_stats_processed = @stats_processed; /* v2.27 (#357): baseline for MopUpProcessed count */

                    SELECT
                        @claimed_table_database = NULL,
                        @claimed_table_schema = NULL,
                        @claimed_table_name = NULL,
                        @claimed_table_object_id = NULL;

                    GOTO ProcessLoop;
                END
                ELSE
                BEGIN
                    RAISERROR(N'Mop-up: leader found no additional stats.', 10, 1) WITH NOWAIT;
                END;
            END;
        END
        ELSE
        BEGIN
            /*
            ====================================================================
            SERIAL MOP-UP: Original single-worker path
            ====================================================================
            */
            RAISERROR(N'', 10, 1) WITH NOWAIT;
            RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
            RAISERROR(N' Mop-Up Pass: %d seconds remaining -- broad sweep of modified stats', 10, 1, @mop_up_remaining_seconds) WITH NOWAIT;
            RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
            RAISERROR(N'', 10, 1) WITH NOWAIT;

            DECLARE @mop_up_db_list_s TABLE (idx int IDENTITY(1,1) PRIMARY KEY, database_name sysname NOT NULL);
            INSERT INTO @mop_up_db_list_s (database_name) SELECT DISTINCT database_name FROM #stats_to_process;

            INSERT INTO @mop_up_db_list_s (database_name)
            SELECT td.DatabaseName
            FROM @tmpDatabases AS td
            WHERE td.Selected = 1
            AND NOT EXISTS (SELECT 1 FROM @mop_up_db_list_s AS ml WHERE ml.database_name = td.DatabaseName);

            SELECT @mop_up_db_idx = MIN(idx) FROM @mop_up_db_list_s;

            WHILE @mop_up_db_idx IS NOT NULL
            BEGIN
                SELECT @mop_up_db = database_name FROM @mop_up_db_list_s WHERE idx = @mop_up_db_idx;

                /* v2.26: CAST first element to nvarchar(max) to prevent 4000 char truncation during concatenation */
                SET @mop_up_sql = CAST(N'
                USE ' AS nvarchar(max)) + QUOTENAME(@mop_up_db) + N';

                SELECT
                    database_name = DB_NAME(),
                    schema_name = OBJECT_SCHEMA_NAME(s.object_id),
                    table_name = OBJECT_NAME(s.object_id),
                    stat_name = s.name,
                    object_id = s.object_id,
                    stats_id = s.stats_id,
                    no_recompute = s.no_recompute,
                    is_incremental = s.is_incremental,
                    is_memory_optimized = ISNULL(t.is_memory_optimized, 0),
                    is_heap = CASE WHEN EXISTS (SELECT 1 FROM sys.indexes AS ix WHERE ix.object_id = s.object_id AND ix.index_id = 0) THEN 1 ELSE 0 END,
                    auto_created = s.auto_created,
                    modification_counter = ISNULL(sp.modification_counter, 0),
                    row_count = ISNULL(sp.rows, 0),
                    days_stale = ISNULL(DATEDIFF(DAY, sp.last_updated, SYSDATETIME()), 9999),
                    page_count = ISNULL(pgs.total_pages, 0),
                    persisted_sample_percent = sp.persisted_sample_percent,
                    histogram_steps = sp.steps,
                    has_filter = s.has_filter,
                    filter_definition = s.filter_definition,
                    unfiltered_rows = sp.unfiltered_rows,
                    qs_plan_count = CONVERT(int, NULL),
                    qs_total_executions = CONVERT(bigint, NULL),
                    qs_total_cpu_ms = CONVERT(bigint, NULL),
                    qs_total_duration_ms = CONVERT(bigint, NULL),
                    qs_total_logical_reads = CONVERT(bigint, NULL),
                    qs_total_memory_grant_kb = CONVERT(bigint, NULL),
                    qs_total_tempdb_pages = CONVERT(bigint, NULL),
                    qs_total_physical_reads = CONVERT(bigint, NULL),
                    qs_total_logical_writes = CONVERT(bigint, NULL),
                    qs_total_wait_time_ms = CONVERT(bigint, NULL),
                    qs_max_dop = CONVERT(smallint, NULL),
                    qs_active_feedback_count = CONVERT(int, NULL),
                    qs_last_execution = CONVERT(datetime2, NULL),
                    qs_priority_boost = CONVERT(bigint, 0),
                    is_published = ISNULL(t.is_published, 0),
                    is_tracked_by_cdc = ISNULL(t.is_tracked_by_cdc, 0),
                    temporal_type = ISNULL(t.temporal_type, 0),
                    priority = ROW_NUMBER() OVER (ORDER BY ISNULL(sp.modification_counter, 0) DESC, s.object_id ASC, s.stats_id ASC)
                FROM sys.stats AS s
                JOIN sys.objects AS o ON o.object_id = s.object_id
                LEFT JOIN sys.tables AS t ON t.object_id = s.object_id
                CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
                OUTER APPLY (
                    SELECT total_pages = SUM(p.used_page_count)
                    FROM sys.dm_db_partition_stats AS p
                    WHERE p.object_id = s.object_id AND p.index_id IN (0, 1)
                ) AS pgs
                ' + @mop_up_where_sql + N'
                    WHERE cl.CommandType = N''UPDATE_STATISTICS''
                    AND   cl.DatabaseName = DB_NAME() COLLATE DATABASE_DEFAULT
                    AND   cl.SchemaName = OBJECT_SCHEMA_NAME(s.object_id) COLLATE DATABASE_DEFAULT
                    AND   cl.ObjectName = OBJECT_NAME(s.object_id) COLLATE DATABASE_DEFAULT
                    AND   cl.StatisticsName = s.name COLLATE DATABASE_DEFAULT
                    AND   cl.StartTime >= @start_time_param
                    AND   cl.ErrorNumber = 0
                )
                ORDER BY priority
                OPTION (RECOMPILE);';

                SET @mop_up_params = N'
                    @i_include_system_objects_param nvarchar(1),
                    @i_include_indexed_views_param nvarchar(1),
                    @TargetNorecompute_param nvarchar(10),
                    @Tables_param nvarchar(max),
                    @ExcludeTables_param nvarchar(max),
                    @ExcludeStatistics_param nvarchar(max),
                    @i_filtered_stats_mode_param nvarchar(10),
                    @i_skip_tables_with_columnstore_param nchar(1),
                    @i_min_page_count_param bigint,
                    @start_time_param datetime2(7)';

                IF @Debug = 1
                BEGIN
                    DECLARE @mop_up_sql_len int = LEN(@mop_up_sql);
                    RAISERROR(N'  Mop-up SQL for %s (%d chars)', 10, 1, @mop_up_db, @mop_up_sql_len) WITH NOWAIT;
                END;

                BEGIN TRY
                    INSERT INTO #stats_to_process
                    (
                        database_name, schema_name, table_name, stat_name, object_id, stats_id,
                        no_recompute, is_incremental, is_memory_optimized, is_heap, auto_created,
                        modification_counter, row_count, days_stale, page_count, persisted_sample_percent, histogram_steps,
                        has_filter, filter_definition, unfiltered_rows,
                        qs_plan_count, qs_total_executions, qs_total_cpu_ms, qs_total_duration_ms,
                        qs_total_logical_reads, qs_total_memory_grant_kb, qs_total_tempdb_pages, qs_total_physical_reads, qs_total_logical_writes, qs_total_wait_time_ms, qs_max_dop, qs_active_feedback_count,
                            qs_last_execution, qs_priority_boost,
                        is_published, is_tracked_by_cdc, temporal_type, priority
                    )
                    EXECUTE sys.sp_executesql
                        @mop_up_sql,
                        @mop_up_params,
                        @i_include_system_objects_param = @i_include_system_objects,
                        @i_include_indexed_views_param = @i_include_indexed_views,
                        @TargetNorecompute_param = @TargetNorecompute,
                        @Tables_param = @Tables,
                        @ExcludeTables_param = @ExcludeTables,
                        @ExcludeStatistics_param = @ExcludeStatistics,
                        @i_filtered_stats_mode_param = @i_filtered_stats_mode,
                        @i_skip_tables_with_columnstore_param = @i_skip_tables_with_columnstore,
                        @i_min_page_count_param = @i_min_page_count,
                        @start_time_param = @start_time;
                END TRY
                BEGIN CATCH
                    IF @Debug = 1
                    BEGIN
                        DECLARE @mop_up_err_s nvarchar(500) = ERROR_MESSAGE();
                        RAISERROR(N'  Mop-up discovery warning (%s): %s', 10, 1, @mop_up_db, @mop_up_err_s) WITH NOWAIT;
                    END;
                END CATCH;

                SELECT @mop_up_db_idx = MIN(idx) FROM @mop_up_db_list_s WHERE idx > @mop_up_db_idx;
            END;

            SET @mop_up_found = (SELECT COUNT(*) FROM #stats_to_process WHERE processed = 0);

            IF @mop_up_found > 0
            BEGIN
                SET @total_stats = @total_stats + @mop_up_found;
                SET @stop_reason = NULL;
                SET @in_mop_up = 1;
                SET @mop_up_stats_found = @mop_up_found;
                SET @mop_up_stats_processed = @stats_processed;

                RAISERROR(N'Mop-up: found %d additional stats with modifications (ordered by modification_counter DESC)', 10, 1, @mop_up_found) WITH NOWAIT;
                RAISERROR(N'', 10, 1) WITH NOWAIT;
                RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
                RAISERROR(N' Processing Mop-Up Statistics', 10, 1) WITH NOWAIT;
                RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
                RAISERROR(N'', 10, 1) WITH NOWAIT;

                GOTO ProcessLoop;
            END
            ELSE
            BEGIN
                RAISERROR(N'Mop-up: no additional stats found with modifications.  All stats are current.', 10, 1) WITH NOWAIT;
            END;
        END;
    END;
    SkipMopUp: /* #340: GOTO target for pre-flight safety check failures */
    /*#endregion 10-MOP-UP */
    /*#region 11-FINALIZE: Summary, CommandLog footer, output params */
    /* #434: restore caller's session LOCK_TIMEOUT before finalization */
    DECLARE @lock_restore_sql nvarchar(100) = N'SET LOCK_TIMEOUT ' + CONVERT(nvarchar(20), @original_lock_timeout);
    EXECUTE (@lock_restore_sql);

    /*
    ============================================================================
    SUMMARY
    ============================================================================
    */
    DECLARE
        @end_time datetime2(7) = SYSDATETIME(),
        @duration_seconds integer = DATEDIFF(SECOND, @start_time, SYSDATETIME()),
        @duration_ms_total bigint = DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
        @remaining_stats integer =
        (
            SELECT
                COUNT_BIG(*)
            FROM #stats_to_process AS stp
            WHERE stp.processed = 0
        ),
        @end_time_display nvarchar(30) = CONVERT(nvarchar(30), SYSDATETIME(), 121);

    /*
    Determine stop reason if not already set
    */
    IF @stop_reason IS NULL
    BEGIN
        SELECT
            @stop_reason = N'NATURAL_END';
    END;
    RAISERROR(N'', 10, 1) WITH NOWAIT;
    RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
    RAISERROR(N' Summary', 10, 1) WITH NOWAIT;
    RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
    RAISERROR(N'', 10, 1) WITH NOWAIT;
    RAISERROR(N'End time:        %s', 10, 1, @end_time_display) WITH NOWAIT;
    /* Sub-second duration display: show decimal seconds (e.g., 0.9s not 0s) */
    DECLARE @duration_display nvarchar(30) =
        CONVERT(nvarchar(20), @duration_ms_total / 1000) + N'.' +
        CONVERT(nvarchar(3), (@duration_ms_total % 1000) / 100) + N's';
    RAISERROR(N'Duration:        %s', 10, 1, @duration_display) WITH NOWAIT;
    RAISERROR(N'', 10, 1) WITH NOWAIT;
    RAISERROR(N'Stats processed: %d / %d', 10, 1, @stats_processed, @total_stats) WITH NOWAIT;
    RAISERROR(N'  Succeeded:     %d', 10, 1, @stats_succeeded) WITH NOWAIT;
    RAISERROR(N'  Failed:        %d', 10, 1, @stats_failed) WITH NOWAIT;
    IF @stats_toctou > 0
        RAISERROR(N'  TOCTOU skips:  %d (dropped objects)', 10, 1, @stats_toctou) WITH NOWAIT;
    RAISERROR(N'  Skipped:       %d (dry run)', 10, 1, @stats_skipped) WITH NOWAIT;
    RAISERROR(N'  Remaining:     %d', 10, 1, @remaining_stats) WITH NOWAIT;
    IF @total_pages_processed > 0
    BEGIN
        DECLARE @volume_gb nvarchar(20) = CONVERT(nvarchar(20), CONVERT(decimal(10, 2), @total_pages_processed * 8.0 / 1024.0 / 1024.0));
        DECLARE @volume_msg nvarchar(200) = N'  Data volume:   ' + @volume_gb + N' GB';
        IF @duration_ms_total > 0
        BEGIN
            DECLARE @gb_per_min nvarchar(20) = CONVERT(nvarchar(20), CONVERT(decimal(10, 2),
                (@total_pages_processed * 8.0 / 1024.0 / 1024.0) / (@duration_ms_total / 60000.0)));
            SET @volume_msg += N' (' + @gb_per_min + N' GB/min)';
        END;
        RAISERROR(@volume_msg, 10, 1) WITH NOWAIT;
    END;
    RAISERROR(N'', 10, 1) WITH NOWAIT;

    IF @remaining_stats > 0
    BEGIN
        RAISERROR(N'Note: %d stats remain. Re-run sp_StatUpdate to continue.', 10, 1, @remaining_stats) WITH NOWAIT;
    END;

    /* #333: Post-run overshoot warning when @StopByTime was specified */
    IF  @StopByTime IS NOT NULL
    AND @Execute = N'Y'
    BEGIN
        DECLARE
            @p333_stop_time time(0) = CONVERT(time(0), @StopByTime),
            @p333_now time(0) = CONVERT(time(0), SYSDATETIME()),
            @p333_overshoot_sec int;

        SET @p333_overshoot_sec = DATEDIFF(SECOND, @p333_stop_time, @p333_now);

        /* If stop time wrapped to tomorrow (e.g., start 20:00, stop 05:00),
           the DATEDIFF is large positive but the run hasn't actually overshot.
           Real overshoot cannot exceed total run duration. */
        IF @p333_overshoot_sec > DATEDIFF(SECOND, @start_time, SYSDATETIME())
            SET @p333_overshoot_sec = @p333_overshoot_sec - 86400;

        /* Only warn if overshoot > 30 seconds (avoid noise from sub-second overruns) */
        IF @p333_overshoot_sec > 30
        BEGIN
            DECLARE @p333_msg nvarchar(500) =
                N'WARNING: Run exceeded @StopByTime (' + @StopByTime + N') by '
                + CONVERT(nvarchar(10), @p333_overshoot_sec) + N' seconds. '
                + N'Consider setting @i_max_seconds_per_stat to guard against large-table overshoot. (#333)';
            RAISERROR(@p333_msg, 10, 1) WITH NOWAIT;
            SET @warnings += N'STOPBYTIME_OVERSHOOT: ' + CONVERT(nvarchar(10), @p333_overshoot_sec) + N's past ' + @StopByTime + N'; ';
        END;
    END;

    RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;

    /*
    ============================================================================
    QUERY STORE FORCED PLAN CHECK (#32)
    After stats updates, warn if forced plans exist on updated tables.
    Statistics changes may invalidate forced plan choices.
    ============================================================================
    */
    IF  @Execute = N'Y'
    AND @stats_succeeded > 0
    AND (@i_time_limit IS NULL OR DATEDIFF(SECOND, @start_time, SYSDATETIME()) < @i_time_limit)
    BEGIN
        DECLARE
            @qs_check_db sysname = NULL,
            @qs_check_sql nvarchar(max),
            @qs_forced_count int = 0,
            @qs_forced_total int = 0,
            @qs_forced_details nvarchar(max) = N'',
            @qs_db_idx int = 0;

        DECLARE @qs_databases TABLE
        (
            idx int IDENTITY(1,1) PRIMARY KEY,
            database_name sysname NOT NULL
        );

        INSERT INTO @qs_databases (database_name)
        SELECT DISTINCT stp.database_name
        FROM #stats_to_process AS stp
        WHERE stp.processed = 1;

        SELECT @qs_db_idx = MIN(idx) FROM @qs_databases;

        WHILE @qs_db_idx IS NOT NULL
        BEGIN
            /* Time guard: skip remaining databases if past @i_time_limit */
            IF @i_time_limit IS NOT NULL AND DATEDIFF(SECOND, @start_time, SYSDATETIME()) >= @i_time_limit
            BEGIN
                IF @Debug = 1
                    RAISERROR(N'  Forced plan check: skipping remaining databases (time limit reached).', 10, 1) WITH NOWAIT;
                SET @qs_db_idx = NULL;
                CONTINUE;
            END;

            SELECT @qs_check_db = database_name
            FROM @qs_databases
            WHERE idx = @qs_db_idx;

            BEGIN TRY
                /*
                v2.26: Early-out when database has zero forced plans.
                Avoids the expensive CHARINDEX scan entirely.
                */
                DECLARE @qs_has_forced int = 0;
                SET @qs_check_sql = N'
                    SET LOCK_TIMEOUT 10000;
                    SELECT @cnt = COUNT(*)
                    FROM ' + QUOTENAME(@qs_check_db) + N'.sys.query_store_plan
                    WHERE is_forced_plan = 1;';

                EXEC sp_executesql @qs_check_sql,
                    N'@cnt int OUTPUT',
                    @cnt = @qs_has_forced OUTPUT;

                IF ISNULL(@qs_has_forced, 0) > 0
                BEGIN
                /*
                P1 fix (#187): Filter forced plans first (typically very few), then check
                if any reference updated tables via CHARINDEX on query text.
                Uses 30-second LOCK_TIMEOUT to prevent indefinite blocking on QS DMVs.
                */
                SET @qs_check_sql = N'
                    SET LOCK_TIMEOUT 30000;
                    SELECT @cnt = COUNT(DISTINCT qsp.plan_id)
                    FROM ' + QUOTENAME(@qs_check_db) + N'.sys.query_store_plan AS qsp
                    INNER JOIN ' + QUOTENAME(@qs_check_db) + N'.sys.query_store_query AS qsq
                        ON qsq.query_id = qsp.query_id
                    INNER JOIN ' + QUOTENAME(@qs_check_db) + N'.sys.query_store_query_text AS qsqt
                        ON qsqt.query_text_id = qsq.query_text_id
                    WHERE qsp.is_forced_plan = 1
                    AND EXISTS (
                        SELECT 1 FROM #stats_to_process AS stp
                        WHERE stp.database_name = @dbname COLLATE DATABASE_DEFAULT
                        AND stp.processed = 1
                        AND CHARINDEX(stp.table_name COLLATE DATABASE_DEFAULT,
                                      qsqt.query_sql_text COLLATE DATABASE_DEFAULT) > 0
                    )';

                EXEC sp_executesql @qs_check_sql,
                    N'@dbname sysname, @cnt int OUTPUT',
                    @dbname = @qs_check_db,
                    @cnt = @qs_forced_count OUTPUT;

                IF ISNULL(@qs_forced_count, 0) > 0
                BEGIN
                    SET @qs_forced_total = @qs_forced_total + @qs_forced_count;

                    /* #301: Check AUTOMATIC_PLAN_CORRECTION (SQL 2017+) to soften the warning */
                    DECLARE @apc_enabled bit = 0;
                    BEGIN TRY
                        DECLARE @apc_sql nvarchar(max) = N'
                            SELECT @apc_out = CASE WHEN desired_state = 1 THEN 1 ELSE 0 END
                            FROM ' + QUOTENAME(@qs_check_db) + N'.sys.database_automatic_tuning_options
                            WHERE name = N''FORCE_LAST_GOOD_PLAN'';';
                        EXEC sp_executesql @apc_sql,
                            N'@apc_out bit OUTPUT',
                            @apc_out = @apc_enabled OUTPUT;
                    END TRY
                    BEGIN CATCH
                        SET @apc_enabled = 0; /* DMV not available (pre-2017) or not accessible */
                    END CATCH;

                    SET @qs_forced_details = @qs_forced_details
                        + @qs_check_db + N'(' + CONVERT(nvarchar(10), @qs_forced_count)
                        + CASE WHEN @apc_enabled = 1 THEN N',APC' ELSE N'' END
                        + N') ';
                END;
                END; /* END IF @qs_has_forced > 0 */
            END TRY
            BEGIN CATCH
                /* Query Store not enabled or not accessible on this database -- skip */
            END CATCH;

            SELECT @qs_db_idx = MIN(idx) FROM @qs_databases WHERE idx > @qs_db_idx;
        END;

        IF @qs_forced_total > 0
        BEGIN
            SET @warnings = @warnings + N'QS_FORCED_PLANS: '
                + CONVERT(nvarchar(10), @qs_forced_total)
                + N' forced plan(s) on updated tables -- verify plan quality: '
                + RTRIM(@qs_forced_details) + N'; ';
            RAISERROR(N'', 10, 1) WITH NOWAIT;
            RAISERROR(N'WARNING: %d Query Store forced plan(s) on tables with updated statistics: %s', 10, 1, @qs_forced_total, @qs_forced_details) WITH NOWAIT;
            RAISERROR(N'         Stats changes may affect forced plan quality. Review with sys.query_store_plan.', 10, 1) WITH NOWAIT;
        END;

        /* gh-452: the forced-plan check issued SET LOCK_TIMEOUT 10000/30000 via
           sp_executesql, which persists at session level after the EXECUTE
           returns -- overwriting the restore done at region-top.  Re-restore now. */
        EXECUTE (@lock_restore_sql);
    END;

    /*
    ============================================================================
    LOG RUN_FOOTER TO COMMANDLOG
    ============================================================================
    */
    IF  @LogToTable = N'Y'
    AND @Execute = N'Y'
    AND @commandlog_exists = 1
    BEGIN
        /* Build JSON summary for monitoring integration (v2.3) */
        DECLARE @json_status nvarchar(10) =
            CASE
                WHEN @stats_failed > 0 THEN N'ERROR'
                WHEN @remaining_stats > 0 THEN N'WARNING'
                ELSE N'OK'
            END;
        DECLARE @json_summary_str nvarchar(500) =
            N'{"succeeded":' + CONVERT(nvarchar(20), @stats_succeeded)
            + N',"failed":' + CONVERT(nvarchar(20), @stats_failed)
            + N',"duration_seconds":' + CONVERT(nvarchar(20), @duration_seconds)
            + N',"status":"' + @json_status + N'"}';

        DECLARE
            @summary_xml xml =
            (
                SELECT
                    @procedure_version AS [Version],
                    @run_label AS RunLabel,
                    @@SPID AS SessionID,
                    @total_stats AS StatsFound,
                    @stats_processed AS StatsProcessed,
                    @stats_succeeded AS StatsSucceeded,
                    @stats_failed AS StatsFailed,
                    @stats_toctou AS StatsToctou,
                    @remaining_stats AS StatsRemaining,
                    @duration_seconds AS DurationSeconds,
                    @stop_reason AS StopReason,
                    @in_mop_up AS MopUpTriggered,
                    @mop_up_stats_found AS MopUpFound,
                    CASE WHEN @in_mop_up = 1 THEN @stats_processed - @mop_up_stats_processed ELSE 0 END AS MopUpProcessed,
                    @total_pages_processed AS TotalPagesProcessed,
                    CASE
                        WHEN @i_qs_enabled = 1
                        AND  NOT EXISTS (SELECT 1 FROM #stats_to_process WHERE qs_priority_boost > 0)
                        THEN 1 ELSE 0
                    END AS QSEnrichmentSkipped,
                    @json_summary_str AS JsonSummary
                FOR
                    XML RAW(N'Summary'),
                    ELEMENTS
            );

        BEGIN TRY
            INSERT INTO
                dbo.CommandLog
            (
                DatabaseName,
                SchemaName,
                ObjectName,
                ObjectType,
                Command,
                CommandType,
                StartTime,
                EndTime,
                ExtendedInfo
            )
            VALUES
            (
                ISNULL(@Databases, DB_NAME()),
                N'dbo',
                N'sp_StatUpdate',
                N'P',
                N'sp_StatUpdate completed: ' + @stop_reason,
                N'SP_STATUPDATE_END',
                @start_time,
                @end_time,
                @summary_xml
            );

            RAISERROR(N'', 10, 1) WITH NOWAIT;
            RAISERROR(N'Run logged: %s (StopReason: %s)', 10, 1, @run_label, @stop_reason) WITH NOWAIT;
        END TRY
        BEGIN CATCH
            SELECT @log_error_msg = LEFT(ERROR_MESSAGE(), 3900);
            RAISERROR(N'WARNING: Failed to log run end to CommandLog (%s)', 10, 1, @log_error_msg) WITH NOWAIT;
        END CATCH;
    END;

    /*
    ============================================================================
    POPULATE OUTPUT PARAMETERS (for automation)
    ============================================================================
    */
    IF @stats_toctou > 0
        SET @warnings = @warnings + N'TOCTOU_SKIPS: ' + CONVERT(nvarchar(10), @stats_toctou) + N' stat(s) skipped (objects dropped during run); ';

    SELECT
        @StatsFoundOut = @total_stats,
        @StatsProcessedOut = @stats_processed,
        @StatsSucceededOut = @stats_succeeded,
        @StatsFailedOut = @stats_failed,
        @StatsRemainingOut = @remaining_stats,
        @DurationSecondsOut = @duration_seconds,
        @WarningsOut = NULLIF(@warnings, N''),
        /* #179: If databases were skipped, append to stop reason so automation sees partial completion */
        @StopReasonOut = CASE
            WHEN @warnings LIKE N'%DB_SKIPPED:%' AND @stop_reason = N'COMPLETED'
            THEN N'COMPLETED_WITH_SKIPPED_DBS'
            ELSE @stop_reason
        END;

    /*
    ============================================================================
    RETURN SUMMARY RESULT SET
    ============================================================================
    Provides programmatic access to run statistics.
    Enables automation scripts to capture and react to results.
    */
    SELECT
        /*
        Status: Enables easy Agent job alerting
          ERROR   = At least one statistic failed to update
          WARNING = Stats skipped or remaining (time/batch limit)
          SUCCESS = All discovered stats updated without issues
        */
        Status = CASE
            WHEN @stats_failed > 0 THEN N'ERROR'
            WHEN @remaining_stats > 0 THEN N'WARNING'
            WHEN @stats_skipped > 0 AND @Execute = N'Y' THEN N'WARNING'
            ELSE N'SUCCESS'
        END,
        StatusMessage = CASE
            WHEN @stats_failed > 0 AND @remaining_stats > 0
                THEN N'Failed: ' + CONVERT(nvarchar(10), @stats_failed) + N' stat(s), '
                    + CONVERT(nvarchar(10), @remaining_stats) + N' remaining (' + ISNULL(@stop_reason, N'unknown') + N')'
            WHEN @stats_failed > 0
                THEN N'Failed: ' + CONVERT(nvarchar(10), @stats_failed) + N' stat(s)'
            WHEN @remaining_stats > 0
                THEN N'Incomplete: ' + CONVERT(nvarchar(10), @remaining_stats) + N' stat(s) remaining (' + ISNULL(@stop_reason, N'unknown') + N')'
            WHEN @stats_skipped > 0
                THEN N'Skipped: ' + CONVERT(nvarchar(10), @stats_skipped) + N' stat(s)'
            ELSE N'All ' + CONVERT(nvarchar(10), @stats_succeeded) + N' stat(s) updated successfully'
        END,
        StatsFound = @total_stats,
        StatsProcessed = @stats_processed,
        StatsSucceeded = @stats_succeeded,
        StatsFailed = @stats_failed,
        StatsToctou = @stats_toctou,
        StatsSkipped = @stats_skipped,
        StatsRemaining = @remaining_stats,
        DatabasesProcessed = @database_count,
        DurationSeconds = @duration_seconds,
        StopReason = @stop_reason,
        RunLabel = @run_label,
        Version = @procedure_version;

    /*
    ============================================================================
    DETAILED RESULTS (#25)
    Per-statistic result set for PowerShell/automation integration.
    ============================================================================
    */
    /*
    Ensure non-zero return code when failures occurred (Agent job detection)
    */
    IF @stats_failed > 0 AND @return_code = 0
    BEGIN
        SELECT @return_code = 1; /*Generic failure code for Agent jobs*/
    END;

    /*
    Release re-entrancy guard (bd -j9d: queue-table pattern)
    */
    IF @StatsInParallel = N'N'
    BEGIN
        DELETE FROM dbo.StatUpdateLock
        WHERE Resource = N'sp_StatUpdate' AND SessionID = @@SPID;
    END;

    RETURN @return_code;
    /*#endregion 11-FINALIZE */

END;
GO
