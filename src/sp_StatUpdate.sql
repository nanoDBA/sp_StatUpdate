SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/*

sp_StatUpdate - Priority-Based Statistics Maintenance

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

Version:    1.5.2026.0120 (Major.Minor.Year.MMDD)
            - Version logged to CommandLog ExtendedInfo on each run
            - Query: ExtendedInfo.value('(/Parameters/Version)[1]', 'nvarchar(20)')

History:    1.5.2026.0120 - CRITICAL: Fixed @ExcludeStatistics filter not working. The OR between
                            threshold logic blocks had incorrect operator precedence, causing
                            table/exclusion filters to only apply when @ThresholdLogic='AND'.
                            Wrapped both OR and AND threshold blocks in parentheses.
                          - Fix: @TargetNorecompute='N' now correctly filters to regular stats
                            only (no_recompute=0). Previously included all stats.
                          - Fix: Time/batch limit messages now use severity 10 (informational)
                            instead of severity 16 (error). Prevents SQL Agent jobs from
                            reporting failure when procedure stops gracefully at configured
                            limits. Return code remains 0 for successful execution; non-zero
                            only when actual stat update failures occur.
            1.5.2026.0119 - Core logic fixes and new parameters based on code review analysis:
                            (1) Incremental stats partition targeting: Now queries
                                sys.dm_db_incremental_stats_properties to find which partitions
                                have modifications and only updates those (ON PARTITIONS clause).
                                Previously updated ALL partitions, defeating the purpose of
                                incremental statistics. SQL 2016 compatible (FOR XML PATH fallback).
                            (2) Query Store join reorder: Filter by object_id FIRST through
                                sys.query_store_query, then join to plans. Reduces intermediate
                                result set size on large Query Store catalogs.
                            (3) New @ExcludeStatistics parameter: Exclude stats by name pattern
                                (supports % wildcard), e.g., '_WA_Sys%' to skip auto-created stats.
                            (4) New @ProgressLogInterval parameter: Log SP_STATUPDATE_PROGRESS
                                to CommandLog every N stats for Agent job monitoring visibility.
            1.4.2026.0119d - Code review fixes:
                            (1) Tiered threshold cliff effect at 500/501 rows - added SQRT
                                alternative to first tier to smooth the transition.
                            (2) FilteredStatsStaleFactor now uses selectivity-adjusted threshold
                                instead of ratio check (ratio measures selectivity, not staleness).
                            (3) @parameters_string TRIM to prevent duplicate queues from whitespace.
                            (4) Query Store state check - only query if actual_state IN (1,2)
                                for READ_ONLY/READ_WRITE modes (avoids errors on disabled QS).
                            (5) Style: Changed 1/0 to 1 in EXISTS checks for clarity.
            1.4.2026.0119c - Bug fix: Arithmetic overflow (8115) in FILTERED_DRIFT sort order
                            and Query Store metric calculations. Changed int to bigint for
                            large value handling; added IIF() cap for ratio calculations.
            1.4.2026.0119b - Bug fix: @StatsInParallel=Y table claiming bug. @claimed_tables
                            table variable was not cleared between loop iterations, causing
                            SELECT TOP 1 to return stale data from previous claim attempts.
                            Workers would exit early with COMPLETED after processing 1 stat.
            1.4.2026.0119 - Query Store prioritization: @QueryStorePriority cross-references
                            sys.query_store_plan to prioritize statistics used by active queries.
                            @QueryStoreMinExecutions and @QueryStoreRecentHours control thresholds.
                            New QUERY_STORE sort order for query-driven maintenance.
                            Filtered statistics handling: @FilteredStatsMode (INCLUDE/EXCLUDE/
                            ONLY/PRIORITY) and @FilteredStatsStaleFactor detect filtered stats
                            with selectivity drift (unfiltered_rows/rows ratio) on partitioned data.
                            New FILTERED_DRIFT sort order. ExtendedInfo now logs HasFilter,
                            FilterDefinition, UnfilteredRows, FilteredDriftRatio, QSPlanCount,
                            QSTotalExecutions, QSLastExecution. QualifyReason now includes
                            QUERY_STORE_PRIORITY and FILTERED_DRIFT.
            1.3.2026.0119 - Multi-database support: Ola Hallengren-style keywords
                            (SYSTEM_DATABASES, USER_DATABASES, ALL_DATABASES,
                            AVAILABILITY_GROUP_DATABASES), wildcards (%), exclusions (-).
                            @IncludeSystemObjects parameter for system object statistics.
                            AG secondaries excluded automatically (warning, not error).
                            Bug fixes: RESAMPLE + PERSIST_SAMPLE_PERCENT conflict (1052),
                            RAISERROR bit parameter (2748), PERSIST invalid usage (153).
                            Code review hardening: STRING_SPLIT replaced with recursive CTE
                            for deterministic ordering, persisted_sample_percent respected,
                            OUTPUT parameters for automation, TRY/CATCH around CommandLog,
                            OPTION (RECOMPILE) on discovery queries, @commandlog_exists
                            caching, non-zero return code on failures for Agent jobs,
                            severity 16 on time/batch limits, MAXRECURSION 500.
            1.2.2026.0117 - Tiger Toolbox 5-tier adaptive thresholds (@TieredThresholds),
                            AND/OR threshold logic (@ThresholdLogic),
                            version-aware PERSIST_SAMPLE_PERCENT and MAXDOP.
                            Erik Darling style refactor, @Help parameter,
                            incremental stats (@UpdateIncremental), @StatisticsResample,
                            AG awareness, heap handling (index_id=0, PageCount),
                            memory-optimized tables, saner defaults.
            1.0.2026.0113 - Initial release with versioning, RUN_HEADER/FOOTER logging

Key Features:
    - RUN_HEADER/RUN_FOOTER logging to detect killed runs
    - Queue-based parallelism for large databases
    - Priority ordering (worst stats first)
    - Incremental statistics support (ON PARTITIONS)
    - RESAMPLE option for preserving sample rates
    - Availability Group awareness (prevents write attempts on secondaries)
    - Memory-optimized table handling
    - Heap forwarding pointer tracking

DROP-IN COMPATIBILITY with Ola Hallengren's SQL Server Maintenance Solution:
    https://ola.hallengren.com

    REQUIRED:
      - dbo.CommandLog table (https://ola.hallengren.com/scripts/CommandLog.sql)
        Used for all logging. Set @LogToTable = 'N' if you don't have it.

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
    MODE 1: DIRECT - Pass specific statistics (skips DMV discovery)

    Three input methods (checked in order):
      1. @StatisticsFromTable - Name of temp/permanent table with stats list
      2. @Statistics            - Comma-separated 'Schema.Table.Stat' values
      3. NULL for both          - Use discovery mode
    ============================================================================
    */
    @Statistics sysname = NULL, /*'Schema.Table.Stat' or 'Table.Stat', comma-separated for multiple*/
    @StatisticsFromTable sysname = NULL, /*table name: '#MyStats', '##GlobalStats', 'dbo.StatsQueue'*/

    /*
    ============================================================================
    MODE 2: DISCOVERY - DMV-based candidate selection (when @Statistics is NULL)
    ============================================================================
    */
    @Databases nvarchar(max) = NULL, /*NULL = current database, SYSTEM_DATABASES, USER_DATABASES, ALL_DATABASES (excludes system), AVAILABILITY_GROUP_DATABASES, comma-separated, wildcards (%), exclusions (-)*/
    @Tables nvarchar(max) = NULL, /*NULL = all tables, or comma-separated 'Schema.Table'*/
    @ExcludeStatistics nvarchar(max) = NULL, /*comma-separated patterns to exclude (supports % wildcard), e.g., '_WA_Sys%' to skip auto-created stats*/
    @TargetNorecompute nvarchar(10) = N'BOTH', /*Y = NORECOMPUTE only, N = regular only, BOTH = all (default)*/
    @ModificationThreshold bigint = 1000, /*minimum modification_counter to qualify*/
    @ModificationPercent float = NULL, /*alternative: min mod % of rows (SQRT-based)*/
    @TieredThresholds bit = 1, /*1 = use Tiger Toolbox 5-tier adaptive thresholds (default), 0 = use fixed @ModificationThreshold*/
    @ThresholdLogic nvarchar(3) = N'OR', /*OR = any threshold qualifies (default), AND = all thresholds must be met*/
    @DaysStaleThreshold integer = NULL, /*minimum days since last update*/
    @MinPageCount bigint = 0, /*minimum used_page_count (125000 = ~1GB)*/
    @IncludeSystemObjects nvarchar(1) = N'N', /*Y = include system object statistics (sys.* tables/views)*/

    /*
    ============================================================================
    FILTERED STATISTICS (edge case handling for partitioned data)
    ============================================================================
    */
    @FilteredStatsMode nvarchar(10) = N'INCLUDE', /*INCLUDE = normal, EXCLUDE = skip filtered, ONLY = filtered only, PRIORITY = boost filtered priority*/
    @FilteredStatsStaleFactor float = 2.0, /*PRIORITY mode threshold multiplier - filtered stats use threshold * selectivity * factor*/

    /*
    ============================================================================
    QUERY STORE PRIORITIZATION (query-driven stat maintenance)
    ============================================================================
    */
    @QueryStorePriority nvarchar(1) = N'N', /*Y = prioritize stats used by Query Store plans, N = ignore*/
    @QueryStoreMetric nvarchar(20) = N'CPU', /*CPU = total CPU time (default), EXECUTIONS = count, DURATION = elapsed time, READS = logical I/O*/
    @QueryStoreMinExecutions bigint = 100, /*minimum plan executions to boost priority*/
    @QueryStoreRecentHours integer = 168, /*only consider plans executed in last N hours (default: 7 days)*/

    /*
    ============================================================================
    UPDATE BEHAVIOR (both modes)
    ============================================================================
    */
    @StatisticsSample integer = NULL, /*NULL = SQL Server decides, 100 = FULLSCAN*/
    @StatisticsResample bit = 0, /*1 = use RESAMPLE (preserves previous sample rate)*/
    @PersistSamplePercent nvarchar(1) = N'Y', /*Y = add PERSIST_SAMPLE_PERCENT = ON*/
    @MaxDOP integer = NULL, /*MAXDOP for FULLSCAN (SQL 2016 SP2+)*/

    /*
    ============================================================================
    INCREMENTAL STATISTICS (partitioned tables only)
    ============================================================================
    */
    @UpdateIncremental bit = 1, /*1 = use ON PARTITIONS() for incremental stats*/

    /*
    ============================================================================
    EXECUTION CONTROL (both modes)
    ============================================================================
    */
    @TimeLimit integer = 3600, /*seconds (default: 1 hour, NULL = unlimited)*/
    @BatchLimit integer = NULL, /*max stats to update per run*/
    @SortOrder nvarchar(50) = N'MODIFICATION_COUNTER', /*priority: MODIFICATION_COUNTER, DAYS_STALE, RANDOM, PAGE_COUNT, QUERY_STORE, FILTERED_DRIFT*/
    @DelayBetweenStats integer = NULL, /*seconds between stats updates*/

    /*
    ============================================================================
    LOGGING & OUTPUT (both modes)
    ============================================================================
    */
    @LockTimeout integer = NULL, /*seconds to wait for locks (NULL = no limit)*/
    @LogToTable nvarchar(1) = N'Y', /*Y = log to dbo.CommandLog (requires table), N = no logging*/
    @ProgressLogInterval int = NULL, /*log progress to CommandLog every N stats (for Agent job monitoring)*/
    @Execute nvarchar(1) = N'Y', /*Y = execute, N = print only (dry run)*/
    @FailFast bit = 0, /*1 = abort on first error*/
    @Debug bit = 0, /*1 = verbose output*/

    /*
    ============================================================================
    PARALLEL EXECUTION (Ola Hallengren Queue pattern)

    When @StatsInParallel = 'Y':
      - First worker populates dbo.QueueStatistic with qualifying stats
      - All workers claim work via UPDATE ... WHERE StatStartTime IS NULL
      - Dead workers detected via sys.dm_exec_requests
      - Run same EXECUTE from multiple sessions/jobs for parallelism

    Prerequisites:
      - dbo.Queue table (https://ola.hallengren.com/scripts/Queue.sql)
      - dbo.QueueStatistic table (from QueueStatistic.sql)
    ============================================================================
    */
    @StatsInParallel nvarchar(1) = N'N', /*Y = use queue-based parallel processing*/

    /*
    ============================================================================
    HELP & VERSION OUTPUT
    ============================================================================
    */
    @Help bit = 0, /*1 = show help in SSMS result set*/
    @Version varchar(20) = NULL OUTPUT, /*returns procedure version*/
    @VersionDate datetime = NULL OUTPUT, /*returns procedure version date*/

    /*
    ============================================================================
    SUMMARY OUTPUT (for automation - avoids parsing result sets)
    ============================================================================
    */
    @StatsFoundOut integer = NULL OUTPUT, /*total qualifying stats discovered*/
    @StatsProcessedOut integer = NULL OUTPUT, /*stats attempted (succeeded + failed)*/
    @StatsSucceededOut integer = NULL OUTPUT, /*stats updated successfully*/
    @StatsFailedOut integer = NULL OUTPUT, /*stats that failed to update*/
    @StatsRemainingOut integer = NULL OUTPUT, /*stats not processed (time/batch limit)*/
    @DurationSecondsOut integer = NULL OUTPUT /*total run duration in seconds*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    SET ARITHABORT ON;
    SET NUMERIC_ROUNDABORT OFF;

    /*
    ============================================================================
    VERSION AND CONSTANTS
    ============================================================================
    */
    DECLARE
        @procedure_version varchar(20) = '1.5.2026.0120',
        @procedure_version_date datetime = '20260120',
        @procedure_name sysname = OBJECT_NAME(@@PROCID),
        @procedure_schema sysname = OBJECT_SCHEMA_NAME(@@PROCID);

    /*
    Set output parameters
    */
    SELECT
        @Version = @procedure_version,
        @VersionDate = @procedure_version_date;

    /*
    ============================================================================
    HELP SECTION
    ============================================================================
    */
    IF @Help = 1
    BEGIN
        /*
        Introduction
        */
        SELECT
            introduction =
                N'Hi, I''m sp_StatUpdate!' UNION ALL
        SELECT
            N'I help you update statistics with priority ordering and time limits.' UNION ALL
        SELECT
            N'I can handle NORECOMPUTE stats, incremental stats, and memory-optimized tables.' UNION ALL
        SELECT
            N'for more info, visit: https://github.com/nanoDBA/sp_StatUpdate' UNION ALL
        SELECT
            N'' UNION ALL
        SELECT
            N'version: ' + @procedure_version + N' (' + CONVERT(nvarchar(10), @procedure_version_date, 120) + N')';

        /*
        Parameter documentation
        */
        SELECT
            parameter_name =
                ap.name,
            data_type =
                t.name +
                CASE
                    WHEN t.name IN (N'varchar', N'nvarchar', N'char', N'nchar')
                    THEN N'(' +
                        CASE
                            WHEN ap.max_length = -1
                            THEN N'max'
                            WHEN t.name IN (N'nvarchar', N'nchar')
                            THEN CONVERT(nvarchar(10), ap.max_length / 2)
                            ELSE CONVERT(nvarchar(10), ap.max_length)
                        END + N')'
                    ELSE N''
                END,
            description =
                CASE ap.name
                    WHEN N'@Statistics'
                    THEN N'DIRECT MODE: comma-separated stat names (Schema.Table.Stat or Table.Stat)'
                    WHEN N'@StatisticsFromTable'
                    THEN N'DIRECT MODE: table name containing stats list (#temp, ##global, or dbo.Table)'
                    WHEN N'@Databases'
                    THEN N'DISCOVERY: database(s) - NULL=current, SYSTEM_DATABASES, USER_DATABASES, ALL_DATABASES (excludes system), AVAILABILITY_GROUP_DATABASES, wildcards (%), exclusions (-)'
                    WHEN N'@Tables'
                    THEN N'DISCOVERY MODE: table filter (NULL = all, comma-separated Schema.Table)'
                    WHEN N'@ExcludeStatistics'
                    THEN N'DISCOVERY MODE: exclude stats by name pattern (supports % wildcard)'
                    WHEN N'@IncludeSystemObjects'
                    THEN N'Y = include system object statistics (sys.* tables/views), N = user objects only'
                    WHEN N'@TargetNorecompute'
                    THEN N'Y = only NORECOMPUTE stats, N = only regular stats, BOTH = all stats'
                    WHEN N'@ModificationThreshold'
                    THEN N'minimum modification_counter to qualify for update'
                    WHEN N'@ModificationPercent'
                    THEN N'SQRT-based formula (mod_counter >= percent * SQRT(row_count))'
                    WHEN N'@TieredThresholds'
                    THEN N'1 = use Tiger Toolbox 5-tier adaptive thresholds (ignores @ModificationPercent)'
                    WHEN N'@ThresholdLogic'
                    THEN N'OR = any threshold qualifies, AND = all must be met'
                    WHEN N'@DaysStaleThreshold'
                    THEN N'minimum days since last update to qualify'
                    WHEN N'@MinPageCount'
                    THEN N'minimum used_page_count (125000 = ~1GB tables only)'
                    WHEN N'@StatisticsSample'
                    THEN N'sample percent: NULL = SQL Server decides, 100 = FULLSCAN'
                    WHEN N'@StatisticsResample'
                    THEN N'1 = use RESAMPLE (preserves previous sample rate, required for incremental)'
                    WHEN N'@PersistSamplePercent'
                    THEN N'Y = add PERSIST_SAMPLE_PERCENT = ON to preserve sample rate'
                    WHEN N'@MaxDOP'
                    THEN N'MAXDOP for FULLSCAN operations (SQL Server 2016 SP2+)'
                    WHEN N'@UpdateIncremental'
                    THEN N'1 = use ON PARTITIONS() for incremental stats on partitioned tables'
                    WHEN N'@TimeLimit'
                    THEN N'maximum seconds to run (NULL = unlimited, default: 18000 = 5 hours)'
                    WHEN N'@BatchLimit'
                    THEN N'maximum number of stats to update per run'
                    WHEN N'@SortOrder'
                    THEN N'priority ordering: MODIFICATION_COUNTER, DAYS_STALE, PAGE_COUNT, RANDOM, QUERY_STORE, FILTERED_DRIFT'
                    WHEN N'@FilteredStatsMode'
                    THEN N'INCLUDE = all stats, EXCLUDE = skip filtered, ONLY = filtered only, PRIORITY = boost filtered with drift'
                    WHEN N'@FilteredStatsStaleFactor'
                    THEN N'PRIORITY mode threshold multiplier for selectivity-adjusted threshold (default 2.0)'
                    WHEN N'@QueryStorePriority'
                    THEN N'Y = prioritize stats used by Query Store plans, N = ignore'
                    WHEN N'@QueryStoreMetric'
                    THEN N'CPU = total CPU time (default), DURATION = elapsed time, READS = logical I/O, EXECUTIONS = count'
                    WHEN N'@QueryStoreMinExecutions'
                    THEN N'minimum plan executions to boost priority (default 100)'
                    WHEN N'@QueryStoreRecentHours'
                    THEN N'only consider plans executed in last N hours (default 168 = 7 days)'
                    WHEN N'@DelayBetweenStats'
                    THEN N'seconds to wait between stats updates (pacing)'
                    WHEN N'@LockTimeout'
                    THEN N'seconds to wait for locks (NULL = no limit)'
                    WHEN N'@LogToTable'
                    THEN N'Y = log to dbo.CommandLog table'
                    WHEN N'@ProgressLogInterval'
                    THEN N'log SP_STATUPDATE_PROGRESS to CommandLog every N stats (Agent job monitoring)'
                    WHEN N'@Execute'
                    THEN N'Y = execute commands, N = print only (dry run)'
                    WHEN N'@FailFast'
                    THEN N'1 = abort on first error, 0 = continue processing'
                    WHEN N'@Debug'
                    THEN N'1 = verbose diagnostic output'
                    WHEN N'@StatsInParallel'
                    THEN N'Y = use queue-based parallel processing (requires Queue tables)'
                    WHEN N'@Help'
                    THEN N'1 = show this help information'
                    WHEN N'@Version'
                    THEN N'OUTPUT: returns procedure version string'
                    WHEN N'@VersionDate'
                    THEN N'OUTPUT: returns procedure version date'
                    WHEN N'@StatsFoundOut'
                    THEN N'OUTPUT: total qualifying stats discovered'
                    WHEN N'@StatsProcessedOut'
                    THEN N'OUTPUT: stats attempted (succeeded + failed)'
                    WHEN N'@StatsSucceededOut'
                    THEN N'OUTPUT: stats updated successfully'
                    WHEN N'@StatsFailedOut'
                    THEN N'OUTPUT: stats that failed to update'
                    WHEN N'@StatsRemainingOut'
                    THEN N'OUTPUT: stats not processed (time/batch limit)'
                    WHEN N'@DurationSecondsOut'
                    THEN N'OUTPUT: total run duration in seconds'
                    ELSE N'undocumented parameter'
                END,
            valid_inputs =
                CASE ap.name
                    WHEN N'@TargetNorecompute'
                    THEN N'Y, N, BOTH'
                    WHEN N'@SortOrder'
                    THEN N'MODIFICATION_COUNTER, DAYS_STALE, PAGE_COUNT, RANDOM, QUERY_STORE, FILTERED_DRIFT'
                    WHEN N'@FilteredStatsMode'
                    THEN N'INCLUDE, EXCLUDE, ONLY, PRIORITY'
                    WHEN N'@QueryStorePriority'
                    THEN N'Y, N'
                    WHEN N'@QueryStoreMetric'
                    THEN N'CPU, DURATION, READS, EXECUTIONS'
                    WHEN N'@StatisticsSample'
                    THEN N'NULL, 1-100 (100 = FULLSCAN)'
                    WHEN N'@LogToTable'
                    THEN N'Y, N'
                    WHEN N'@ProgressLogInterval'
                    THEN N'NULL, 1-N (e.g., 10, 50, 100)'
                    WHEN N'@Execute'
                    THEN N'Y, N'
                    WHEN N'@PersistSamplePercent'
                    THEN N'Y, N'
                    WHEN N'@StatsInParallel'
                    THEN N'Y, N'
                    ELSE N''
                END,
            defaults =
                CASE ap.name
                    WHEN N'@Statistics'
                    THEN N'NULL (use discovery mode)'
                    WHEN N'@StatisticsFromTable'
                    THEN N'NULL'
                    WHEN N'@Databases'
                    THEN N'NULL (current database)'
                    WHEN N'@Tables'
                    THEN N'NULL (all tables)'
                    WHEN N'@ExcludeStatistics'
                    THEN N'NULL (no exclusions)'
                    WHEN N'@IncludeSystemObjects'
                    THEN N'N'
                    WHEN N'@TargetNorecompute'
                    THEN N'BOTH'
                    WHEN N'@ModificationThreshold'
                    THEN N'1000'
                    WHEN N'@ModificationPercent'
                    THEN N'NULL'
                    WHEN N'@TieredThresholds'
                    THEN N'1'
                    WHEN N'@ThresholdLogic'
                    THEN N'OR'
                    WHEN N'@DaysStaleThreshold'
                    THEN N'NULL'
                    WHEN N'@MinPageCount'
                    THEN N'0'
                    WHEN N'@StatisticsSample'
                    THEN N'NULL (SQL Server decides)'
                    WHEN N'@StatisticsResample'
                    THEN N'0'
                    WHEN N'@PersistSamplePercent'
                    THEN N'Y'
                    WHEN N'@MaxDOP'
                    THEN N'NULL'
                    WHEN N'@UpdateIncremental'
                    THEN N'1'
                    WHEN N'@TimeLimit'
                    THEN N'3600 (1 hour)'
                    WHEN N'@BatchLimit'
                    THEN N'NULL (no limit)'
                    WHEN N'@SortOrder'
                    THEN N'MODIFICATION_COUNTER'
                    WHEN N'@FilteredStatsMode'
                    THEN N'INCLUDE'
                    WHEN N'@FilteredStatsStaleFactor'
                    THEN N'2.0'
                    WHEN N'@QueryStorePriority'
                    THEN N'N'
                    WHEN N'@QueryStoreMetric'
                    THEN N'CPU'
                    WHEN N'@QueryStoreMinExecutions'
                    THEN N'100'
                    WHEN N'@QueryStoreRecentHours'
                    THEN N'168 (7 days)'
                    WHEN N'@DelayBetweenStats'
                    THEN N'NULL'
                    WHEN N'@LockTimeout'
                    THEN N'NULL'
                    WHEN N'@LogToTable'
                    THEN N'Y'
                    WHEN N'@ProgressLogInterval'
                    THEN N'NULL (disabled)'
                    WHEN N'@Execute'
                    THEN N'Y'
                    WHEN N'@FailFast'
                    THEN N'0'
                    WHEN N'@Debug'
                    THEN N'0'
                    WHEN N'@StatsInParallel'
                    THEN N'N'
                    WHEN N'@Help'
                    THEN N'0'
                    ELSE N''
                END
        FROM sys.parameters AS ap
        JOIN sys.types AS t
          ON t.user_type_id = ap.user_type_id
        WHERE ap.object_id = @@PROCID
        ORDER BY
            ap.parameter_id;

        /*
        Example usage
        */
        SELECT
            example_name =
                example_data.example_name,
            example_description =
                example_data.example_description,
            example_code =
                example_data.example_code
        FROM
        (
            VALUES
                (
                    N'Direct Mode - Single Stat',
                    N'Update a specific statistic by name',
                    N'EXECUTE dbo.sp_StatUpdate @Statistics = N''dbo.Customers._WA_Sys_00000001_ABC123'';'
                ),
                (
                    N'Direct Mode - From Table',
                    N'Update stats from a priority queue table',
                    N'EXECUTE dbo.sp_StatUpdate @StatisticsFromTable = N''dbo.StatsPriorityQueue'', @StatisticsSample = 100;'
                ),
                (
                    N'Discovery - NORECOMPUTE Only',
                    N'Find and update all NORECOMPUTE stats',
                    N'EXECUTE dbo.sp_StatUpdate @TargetNorecompute = N''Y'', @ModificationThreshold = 10000;'
                ),
                (
                    N'Discovery - All Stale Stats',
                    N'Update all stats stale > 30 days',
                    N'EXECUTE dbo.sp_StatUpdate @TargetNorecompute = N''BOTH'', @DaysStaleThreshold = 30;'
                ),
                (
                    N'Discovery - Large Tables Only',
                    N'Only tables with 1GB+ data',
                    N'EXECUTE dbo.sp_StatUpdate @MinPageCount = 125000, @TimeLimit = 7200;'
                ),
                (
                    N'With RESAMPLE',
                    N'Preserve previous sample rate (required for incremental stats)',
                    N'EXECUTE dbo.sp_StatUpdate @StatisticsResample = 1;'
                ),
                (
                    N'Parallel Mode',
                    N'Run from multiple SSMS windows for parallel processing',
                    N'EXECUTE dbo.sp_StatUpdate @StatsInParallel = N''Y'', @TimeLimit = 3600;'
                ),
                (
                    N'Dry Run',
                    N'Show what would be updated without executing',
                    N'EXECUTE dbo.sp_StatUpdate @Execute = N''N'', @Debug = 1;'
                )
        ) AS example_data
        (
            example_name,
            example_description,
            example_code
        );

        /*
        Modes explanation
        */
        SELECT
            mode_name =
                mode_data.mode_name,
            mode_description =
                mode_data.mode_description,
            mode_when_to_use =
                mode_data.mode_when_to_use
        FROM
        (
            VALUES
                (
                    N'DIRECT_TABLE',
                    N'Reads stats from @StatisticsFromTable parameter',
                    N'When you have a pre-populated queue (e.g., QueryStore priority queue)'
                ),
                (
                    N'DIRECT_STRING',
                    N'Parses stats from @Statistics comma-separated string',
                    N'When you know exactly which stats need updating'
                ),
                (
                    N'DISCOVERY',
                    N'Queries DMVs to find qualifying stats based on thresholds',
                    N'Nightly maintenance, catch-up runs, finding orphaned stats'
                )
        ) AS mode_data
        (
            mode_name,
            mode_description,
            mode_when_to_use
        );

        RETURN;
    END;

    /*
    ============================================================================
    VARIABLE DECLARATIONS
    ============================================================================
    */
    DECLARE
        @start_time datetime2(7) = SYSDATETIME(),
        @empty_line nvarchar(max) = N'',
        @error_number integer = 0,
        @return_code integer = 0;

    /*
    SQL Server version detection
    Major version: 13 = 2016, 14 = 2017, 15 = 2019, 16 = 2022
    Build number used for feature detection (e.g., PERSIST_SAMPLE_PERCENT, MAXDOP in UPDATE STATISTICS)
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
        @supports_maxdop_stats bit = 0;

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
        @current_modification_counter bigint = NULL,
        @current_row_count bigint = NULL,
        @current_days_stale integer = NULL,
        @current_page_count bigint = NULL,
        @current_persisted_sample_percent float = NULL,
        @current_partition_number integer = NULL,
        @current_forwarded_records bigint = NULL,
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
        @current_qs_last_execution datetime2(3) = NULL,
        @current_qs_priority_boost bigint = NULL;

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
        @current_extended_info xml = NULL;

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

    /*
    Run identification for tracking completion
    */
    DECLARE
        @run_label nvarchar(100) = N'',
        @stop_reason nvarchar(50) = NULL,
        @commandlog_exists bit = CASE WHEN OBJECT_ID(N'dbo.CommandLog', N'U') IS NOT NULL THEN 1 ELSE 0 END;

    /*
    Counters
    */
    DECLARE
        @stats_processed integer = 0,
        @stats_succeeded integer = 0,
        @stats_failed integer = 0,
        @stats_skipped integer = 0;

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
        modification_counter bigint NOT NULL DEFAULT 0,
        row_count bigint NOT NULL DEFAULT 0,
        days_stale integer NOT NULL DEFAULT 0,
        page_count bigint NOT NULL DEFAULT 0,
        partition_number integer NULL,
        persisted_sample_percent float NULL, /*existing persisted sample (warn if overriding)*/
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
                WHEN @sql_major_version = 14 AND @sql_build_number >= 3015 THEN 1 /* SQL 2017 CU3+ */
                WHEN @sql_major_version = 13 AND @sql_build_number >= 5026 THEN 1 /* SQL 2016 SP2+ */
                ELSE 0
            END;

    /*
    Check transaction count
    */
    IF @@TRANCOUNT <> 0
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The transaction count is not 0. sp_StatUpdate should not be called within an open transaction.',
            error_severity = 16;
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

        RAISERROR(N'Created dbo.QueueStatistic table and indexes.', 10, 1) WITH NOWAIT;
    END;

    /*
    ============================================================================
    PARAMETER VALIDATION
    ============================================================================
    */

    IF @TargetNorecompute NOT IN (N'Y', N'N', N'BOTH')
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The value for @TargetNorecompute is not supported. Use Y, N, or BOTH.',
            error_severity = 16;
    END;

    IF @Execute NOT IN (N'Y', N'N')
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The value for @Execute is not supported. Use Y or N.',
            error_severity = 16;
    END;

    IF @LogToTable NOT IN (N'Y', N'N')
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The value for @LogToTable is not supported. Use Y or N.',
            error_severity = 16;
    END;

    IF @PersistSamplePercent NOT IN (N'Y', N'N')
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The value for @PersistSamplePercent is not supported. Use Y or N.',
            error_severity = 16;
    END;

    IF @StatsInParallel NOT IN (N'Y', N'N')
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The value for @StatsInParallel is not supported. Use Y or N.',
            error_severity = 16;
    END;

    IF @IncludeSystemObjects NOT IN (N'Y', N'N')
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The value for @IncludeSystemObjects is not supported. Use Y or N.',
            error_severity = 16;
    END;

    IF @ThresholdLogic NOT IN (N'OR', N'AND')
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The value for @ThresholdLogic is not supported. Use OR or AND.',
            error_severity = 16;
    END;

    /*
    Validate @StatisticsFromTable doesn't contain SQL injection characters
    */
    IF  @StatisticsFromTable IS NOT NULL
    AND (
            @StatisticsFromTable LIKE N'%;%'
         OR @StatisticsFromTable LIKE N'%--%'
         OR @StatisticsFromTable LIKE N'%/*%'
         OR @StatisticsFromTable LIKE N'%*/%'
         OR @StatisticsFromTable LIKE N'%''%'
         OR @StatisticsFromTable LIKE N'%xp_%'
         OR @StatisticsFromTable LIKE N'%sp_execute%'
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
                N'The value for @StatisticsFromTable contains invalid characters.',
            error_severity = 16;
    END;

    /*
    Validate @FilteredStatsMode
    */
    IF @FilteredStatsMode NOT IN (N'INCLUDE', N'EXCLUDE', N'ONLY', N'PRIORITY')
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The value for @FilteredStatsMode is not supported. Use INCLUDE, EXCLUDE, ONLY, or PRIORITY.',
            error_severity = 16;
    END;

    /*
    Validate @QueryStorePriority
    */
    IF @QueryStorePriority NOT IN (N'Y', N'N')
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The value for @QueryStorePriority is not supported. Use Y or N.',
            error_severity = 16;
    END;

    /*
    Validate @QueryStoreMetric
    */
    IF @QueryStoreMetric NOT IN (N'CPU', N'DURATION', N'READS', N'EXECUTIONS')
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The value for @QueryStoreMetric is not supported. Use CPU, DURATION, READS, or EXECUTIONS.',
            error_severity = 16;
    END;

    /*
    Validate @SortOrder (add new options)
    */
    IF @SortOrder NOT IN (N'MODIFICATION_COUNTER', N'DAYS_STALE', N'PAGE_COUNT', N'RANDOM', N'QUERY_STORE', N'FILTERED_DRIFT')
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The value for @SortOrder is not supported. Use MODIFICATION_COUNTER, DAYS_STALE, PAGE_COUNT, RANDOM, QUERY_STORE, or FILTERED_DRIFT.',
            error_severity = 16;
    END;

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
        OPTION (MAXRECURSION 500); /* Support up to 500 comma-separated databases */
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
    AND   d.state = 0;                  /* ONLINE only */

    /*
    Apply selections (inclusion pass first)
    */
    UPDATE td
    SET td.Selected = sd.Selected
    FROM @tmpDatabases AS td
    INNER JOIN @SelectedDatabases AS sd
        ON td.DatabaseName LIKE REPLACE(sd.DatabaseItem, N'_', N'[_]')
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
        ON td.DatabaseName LIKE REPLACE(sd.DatabaseItem, N'_', N'[_]')
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
    SELECT @database_count = COUNT_BIG(*)
    FROM @tmpDatabases
    WHERE Selected = 1;

    /*
    Validate at least one database matched
    */
    IF @database_count = 0
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'No databases matched the @Databases pattern: ' + ISNULL(@Databases, N'(NULL)'),
            error_severity = 16;
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
    SELECT @database_count = COUNT_BIG(*)
    FROM @tmpDatabases
    WHERE Selected = 1;

    IF @database_count = 0
    AND @ag_secondary_count > 0
    BEGIN
        RAISERROR(N'All selected databases are on AG secondary replicas. Statistics cannot be updated on readable secondaries.', 16, 1) WITH NOWAIT;
        RETURN 1;
    END;

    /*
    ============================================================================
    PARAMETER VALIDATION
    ============================================================================
    */
    IF  @StatisticsSample IS NOT NULL
    AND (
            @StatisticsSample < 1
         OR @StatisticsSample > 100
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
                N'The value for @StatisticsSample must be NULL or between 1 and 100.',
            error_severity = 16;
    END;

    /*
    RESAMPLE and sample percent are mutually exclusive
    */
    IF  @StatisticsResample = 1
    AND @StatisticsSample IS NOT NULL
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'Cannot specify both @StatisticsResample = 1 and @StatisticsSample. Choose one.',
            error_severity = 16;
    END;

    IF  @MaxDOP IS NOT NULL
    AND (
            @MaxDOP < 0
         OR @MaxDOP > 64
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
                N'The value for @MaxDOP must be between 0 and 64.',
            error_severity = 16;
    END;

    IF  @TimeLimit IS NOT NULL
    AND @TimeLimit < 0
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The value for @TimeLimit must be >= 0.',
            error_severity = 16;
    END;

    IF  @BatchLimit IS NOT NULL
    AND @BatchLimit < 1
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The value for @BatchLimit must be >= 1.',
            error_severity = 16;
    END;

    IF  @DelayBetweenStats IS NOT NULL
    AND @DelayBetweenStats < 0
    BEGIN
        INSERT INTO
            @errors
        (
            error_message,
            error_severity
        )
        SELECT
            error_message =
                N'The value for @DelayBetweenStats must be >= 0.',
            error_severity = 16;
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
        DECLARE
            @error_cursor cursor,
            @error_msg nvarchar(max) = N'',
            @error_sev integer = 0;

        SET @error_cursor = CURSOR LOCAL FAST_FORWARD FOR
            SELECT
                e.error_message,
                e.error_severity
            FROM @errors AS e
            ORDER BY
                e.id;

        OPEN @error_cursor;

        FETCH NEXT
        FROM @error_cursor
        INTO
            @error_msg,
            @error_sev;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            RAISERROR(@error_msg, @error_sev, 1) WITH NOWAIT;

            FETCH NEXT
            FROM @error_cursor
            INTO
                @error_msg,
                @error_sev;
        END;

        RAISERROR(N'', 10, 1) WITH NOWAIT;
        RAISERROR(N'Documentation: https://ola.hallengren.com/sql-server-index-and-statistics-maintenance.html', 10, 1) WITH NOWAIT;

        RETURN 50000;
    END;

    /*
    Build human-readable run label
    */
    SELECT
        @run_label =
            CONVERT(nvarchar(50), SERVERPROPERTY(N'ServerName')) +
            N'_' +
            FORMAT(@start_time, N'yyyyMMdd_HHmmss');

    /*
    ============================================================================
    HEADER OUTPUT
    ============================================================================
    */
    DECLARE
        @server_name nvarchar(128) = CONVERT(nvarchar(128), SERVERPROPERTY(N'ServerName')),
        @product_version nvarchar(128) = CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')),
        @edition nvarchar(128) = CONVERT(nvarchar(128), SERVERPROPERTY(N'Edition')),
        @Tables_display nvarchar(max) = ISNULL(@Tables, N'ALL'),
        @TimeLimit_display nvarchar(20) = ISNULL(CONVERT(nvarchar(20), @TimeLimit), N'None'),
        @BatchLimit_display nvarchar(20) = ISNULL(CONVERT(nvarchar(20), @BatchLimit), N'None'),
        @LockTimeout_display nvarchar(20) = ISNULL(CONVERT(nvarchar(20), @LockTimeout), N'None'),
        @start_time_display nvarchar(30) = CONVERT(nvarchar(30), @start_time, 121);

    RAISERROR(N'', 10, 1) WITH NOWAIT;
    RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
    RAISERROR(N' sp_StatUpdate - Priority-Based Statistics Maintenance', 10, 1) WITH NOWAIT;
    RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;
    RAISERROR(N'', 10, 1) WITH NOWAIT;
    RAISERROR(N'Server:      %s', 10, 1, @server_name) WITH NOWAIT;
    RAISERROR(N'Version:     %s', 10, 1, @product_version) WITH NOWAIT;
    RAISERROR(N'Edition:     %s', 10, 1, @edition) WITH NOWAIT;
    RAISERROR(N'Procedure:   %s', 10, 1, @procedure_version) WITH NOWAIT;
    RAISERROR(N'Start time:  %s', 10, 1, @start_time_display) WITH NOWAIT;

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
    RAISERROR(N'  @ModificationThreshold   = %I64d', 10, 1, @ModificationThreshold) WITH NOWAIT;
    IF @ModificationPercent IS NOT NULL
    BEGIN
        DECLARE @ModPctDisplay nvarchar(20) = CONVERT(nvarchar(20), @ModificationPercent);
        RAISERROR(N'  @ModificationPercent     = %s', 10, 1, @ModPctDisplay) WITH NOWAIT;
    END;

    /*
    Bit parameters must be cast to integer for RAISERROR display
    */
    DECLARE
        @TieredThresholds_int integer = @TieredThresholds,
        @StatisticsResample_int integer = @StatisticsResample,
        @UpdateIncremental_int integer = @UpdateIncremental,
        @FailFast_int integer = @FailFast;

    RAISERROR(N'  @TieredThresholds        = %d', 10, 1, @TieredThresholds_int) WITH NOWAIT;
    RAISERROR(N'  @ThresholdLogic          = %s', 10, 1, @ThresholdLogic) WITH NOWAIT;
    IF @DaysStaleThreshold IS NOT NULL
    BEGIN
        RAISERROR(N'  @DaysStaleThreshold      = %d', 10, 1, @DaysStaleThreshold) WITH NOWAIT;
    END;
    RAISERROR(N'  @MinPageCount            = %I64d', 10, 1, @MinPageCount) WITH NOWAIT;
    RAISERROR(N'  @IncludeSystemObjects    = %s', 10, 1, @IncludeSystemObjects) WITH NOWAIT;
    IF @StatisticsSample IS NOT NULL
    BEGIN
        RAISERROR(N'  @StatisticsSample        = %d%%', 10, 1, @StatisticsSample) WITH NOWAIT;
    END;
    RAISERROR(N'  @StatisticsResample      = %d', 10, 1, @StatisticsResample_int) WITH NOWAIT;
    RAISERROR(N'  @UpdateIncremental       = %d', 10, 1, @UpdateIncremental_int) WITH NOWAIT;
    RAISERROR(N'  @TimeLimit               = %s seconds', 10, 1, @TimeLimit_display) WITH NOWAIT;
    RAISERROR(N'  @BatchLimit              = %s stats', 10, 1, @BatchLimit_display) WITH NOWAIT;
    RAISERROR(N'  @LockTimeout             = %s seconds', 10, 1, @LockTimeout_display) WITH NOWAIT;
    RAISERROR(N'  @SortOrder               = %s', 10, 1, @SortOrder) WITH NOWAIT;
    RAISERROR(N'  @StatsInParallel         = %s', 10, 1, @StatsInParallel) WITH NOWAIT;
    RAISERROR(N'  @Execute                 = %s', 10, 1, @Execute) WITH NOWAIT;
    RAISERROR(N'  @FailFast                = %d', 10, 1, @FailFast_int) WITH NOWAIT;
    IF @ExcludeStatistics IS NOT NULL
        RAISERROR(N'  @ExcludeStatistics       = %s', 10, 1, @ExcludeStatistics) WITH NOWAIT;
    RAISERROR(N'', 10, 1) WITH NOWAIT;

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
                    @procedure_version AS [Version],
                    @run_label AS RunLabel,
                    @@SPID AS SessionID,
                    @Databases AS [Databases],
                    @database_count AS DatabaseCount,
                    @Tables AS [Tables],
                    @Statistics AS [Statistics],
                    @StatisticsFromTable AS StatisticsFromTable,
                    @TargetNorecompute AS TargetNorecompute,
                    @ModificationThreshold AS ModificationThreshold,
                    @ModificationPercent AS ModificationPercent,
                    @TieredThresholds AS TieredThresholds,
                    @ThresholdLogic AS ThresholdLogic,
                    @DaysStaleThreshold AS DaysStaleThreshold,
                    @MinPageCount AS MinPageCount,
                    @IncludeSystemObjects AS IncludeSystemObjects,
                    @StatisticsSample AS StatisticsSample,
                    @StatisticsResample AS StatisticsResample,
                    @UpdateIncremental AS UpdateIncremental,
                    @TimeLimit AS TimeLimit,
                    @BatchLimit AS BatchLimit,
                    @SortOrder AS SortOrder,
                    @StatsInParallel AS StatsInParallel,
                    @FailFast AS FailFast
                FOR
                    XML RAW(N'Parameters'),
                    ELEMENTS
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
            N'EXECUTE dbo.sp_StatUpdate @Databases = N''' + ISNULL(@Databases, DB_NAME()) + N'''...',
            N'SP_STATUPDATE_START',
            @start_time,
            @parameters_xml
        );

        RAISERROR(N'Run: %s (logged to CommandLog)', 10, 1, @run_label) WITH NOWAIT;
        RAISERROR(N'', 10, 1) WITH NOWAIT;
    END;

    /*
    ============================================================================
    DETERMINE MODE: DIRECT (table), DIRECT (string), or DISCOVERY
    ============================================================================
    */
    DECLARE
        @mode nvarchar(20) =
            CASE
                WHEN @StatisticsFromTable IS NOT NULL
                THEN N'DIRECT_TABLE'
                WHEN @Statistics IS NOT NULL
                THEN N'DIRECT_STRING'
                ELSE N'DISCOVERY'
            END;

    RAISERROR(N'Mode: %s', 10, 1, @mode) WITH NOWAIT;
    RAISERROR(N'', 10, 1) WITH NOWAIT;

    /*
    ============================================================================
    MODE 1A: DIRECT_TABLE - Read from temp/permanent table
    ============================================================================
    */
    IF @mode = N'DIRECT_TABLE'
    BEGIN
        RAISERROR(N'Reading statistics from table: %s', 10, 1, @StatisticsFromTable) WITH NOWAIT;

        DECLARE
            @table_sql nvarchar(max) = N'';

        /*
        Check if table exists
        */
        IF @StatisticsFromTable LIKE N'#%'
        BEGIN
            IF OBJECT_ID(N'tempdb..' + @StatisticsFromTable) IS NULL
            BEGIN
                RAISERROR(N'Temp table %s does not exist', 16, 1, @StatisticsFromTable);
                RETURN 1;
            END;
        END;
        ELSE
        BEGIN
            IF OBJECT_ID(@StatisticsFromTable) IS NULL
            BEGIN
                RAISERROR(N'Table %s does not exist', 16, 1, @StatisticsFromTable);
                RETURN 1;
            END;
        END;

        /*
        Create intermediate temp table
        */
        CREATE TABLE
            #input_stats
        (
            schema_name sysname NULL,
            table_name sysname NULL,
            stat_name sysname NOT NULL,
            priority integer NULL
        );

        /*
        Copy from source table - expects at minimum: StatName column
        */
        SELECT
            @table_sql = N'
        INSERT INTO
            #input_stats
        (
            stat_name
        )
        SELECT
            StatName
        FROM ' + @StatisticsFromTable + N';';

        BEGIN TRY
            EXECUTE sys.sp_executesql
                @table_sql;
        END TRY
        BEGIN CATCH
            RAISERROR(N'Table %s must have at least a StatName column', 16, 1, @StatisticsFromTable);
            RETURN 1;
        END CATCH;

        /*
        Try to update optional columns if they exist
        */
        SELECT
            @table_sql = N'
        UPDATE
            ist
        SET
            ist.schema_name = src.SchemaName
        FROM #input_stats AS ist
        JOIN ' + @StatisticsFromTable + N' AS src
          ON src.StatName = ist.stat_name;';
        BEGIN TRY
            EXECUTE sys.sp_executesql
                @table_sql;
        END TRY
        BEGIN CATCH
        END CATCH;

        SELECT
            @table_sql = N'
        UPDATE
            ist
        SET
            ist.table_name = src.TableName
        FROM #input_stats AS ist
        JOIN ' + @StatisticsFromTable + N' AS src
          ON src.StatName = ist.stat_name;';
        BEGIN TRY
            EXECUTE sys.sp_executesql
                @table_sql;
        END TRY
        BEGIN CATCH
        END CATCH;

        SELECT
            @table_sql = N'
        UPDATE
            ist
        SET
            ist.priority = src.Priority
        FROM #input_stats AS ist
        JOIN ' + @StatisticsFromTable + N' AS src
          ON src.StatName = ist.stat_name;';
        BEGIN TRY
            EXECUTE sys.sp_executesql
                @table_sql;
        END TRY
        BEGIN CATCH
        END CATCH;

        /*
        Join with sys.stats to get full metadata
        */
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
            is_heap,
            modification_counter,
            row_count,
            days_stale,
            page_count,
            persisted_sample_percent,
            priority
        )
        SELECT
            database_name = DB_NAME(),
            schema_name = ISNULL(src.schema_name, OBJECT_SCHEMA_NAME(s.object_id)),
            table_name = ISNULL(src.table_name, OBJECT_NAME(s.object_id)),
            stat_name = s.name,
            object_id = s.object_id,
            stats_id = s.stats_id,
            no_recompute = s.no_recompute,
            is_incremental = s.is_incremental,
            is_memory_optimized = ISNULL(t.is_memory_optimized, 0),
            /*
            HEAP DETECTION: Check for index_id = 0 (heap) directly.
            Heaps have index_id = 0; clustered indexes have index_id = 1.
            Every user table has exactly one of these, never both.
            */
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
            modification_counter = ISNULL(sp.modification_counter, 0),
            row_count = ISNULL(sp.rows, 0),
            days_stale = ISNULL(DATEDIFF(DAY, sp.last_updated, GETDATE()), 9999),
            page_count = ISNULL(pgs.total_pages, 0),
            persisted_sample_percent = sp.persisted_sample_percent, /*track existing persisted sample*/
            priority = ISNULL(src.priority, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))
        FROM #input_stats AS src
        JOIN sys.stats AS s
          ON s.name = src.stat_name
         AND (
                 src.table_name IS NULL
              OR OBJECT_NAME(s.object_id) = src.table_name
             )
         AND (
                 src.schema_name IS NULL
              OR OBJECT_SCHEMA_NAME(s.object_id) = src.schema_name
             )
        LEFT JOIN sys.tables AS t
          ON t.object_id = s.object_id
        CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
        OUTER APPLY
        (
            /*
            PAGE COUNT: Get total table pages from base table structure.
            - index_id = 0 for heaps (tables without clustered index)
            - index_id = 1 for clustered indexes
            Note: We don't match stats_id to index_id because column statistics
            have unique stats_id values that don't correspond to any index.
            */
            SELECT
                total_pages = SUM(p.used_page_count)
            FROM sys.dm_db_partition_stats AS p
            WHERE p.object_id = s.object_id
            AND   p.index_id IN (0, 1)
        ) AS pgs
        WHERE (OBJECTPROPERTY(s.object_id, N'IsUserTable') = 1 OR @IncludeSystemObjects = N'Y')
        ORDER BY
            ISNULL(src.priority, 0)
        OPTION (RECOMPILE); /*Prevents plan caching issues with DMV joins*/

        DROP TABLE #input_stats;

        DECLARE
            @table_row_count integer =
            (
                SELECT
                    COUNT_BIG(*)
                FROM #stats_to_process
            );

        RAISERROR(N'Loaded %d statistics from table', 10, 1, @table_row_count) WITH NOWAIT;
    END;

    /*
    ============================================================================
    MODE 1B: DIRECT_STRING - Parse @Statistics parameter
    ============================================================================
    */
    IF @mode = N'DIRECT_STRING'
    BEGIN
        RAISERROR(N'Parsing explicit statistics list...', 10, 1) WITH NOWAIT;

        ;WITH
            parsed_stats
        AS
        (
            SELECT
                raw_value = LTRIM(RTRIM(ss.value)),
                parsed_schema = PARSENAME(LTRIM(RTRIM(ss.value)), 3),
                parsed_table = PARSENAME(LTRIM(RTRIM(ss.value)), 2),
                parsed_stat = PARSENAME(LTRIM(RTRIM(ss.value)), 1)
            FROM STRING_SPLIT(@Statistics, N',') AS ss
            WHERE LTRIM(RTRIM(ss.value)) <> N''
        )
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
            is_heap,
            modification_counter,
            row_count,
            days_stale,
            page_count,
            persisted_sample_percent,
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
            /*
            HEAP DETECTION: Check for index_id = 0 (heap) directly.
            Heaps have index_id = 0; clustered indexes have index_id = 1.
            */
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
            modification_counter = ISNULL(sp.modification_counter, 0),
            row_count = ISNULL(sp.rows, 0),
            days_stale = ISNULL(DATEDIFF(DAY, sp.last_updated, GETDATE()), 9999),
            page_count = ISNULL(pgs.total_pages, 0),
            persisted_sample_percent = sp.persisted_sample_percent,
            priority = ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
        FROM parsed_stats AS ps
        JOIN sys.stats AS s
          ON s.name = ps.parsed_stat
         AND (
                 ps.parsed_table IS NULL
              OR OBJECT_NAME(s.object_id) = ps.parsed_table
             )
         AND (
                 ps.parsed_schema IS NULL
              OR OBJECT_SCHEMA_NAME(s.object_id) = ps.parsed_schema
             )
        LEFT JOIN sys.tables AS t
          ON t.object_id = s.object_id
        CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
        OUTER APPLY
        (
            /*
            PAGE COUNT: Get total table pages from base table structure.
            - index_id = 0 for heaps, index_id = 1 for clustered indexes
            - Column statistics have stats_id values that don't match any index_id
            */
            SELECT
                total_pages = SUM(p.used_page_count)
            FROM sys.dm_db_partition_stats AS p
            WHERE p.object_id = s.object_id
            AND   p.index_id IN (0, 1)
        ) AS pgs
        WHERE (OBJECTPROPERTY(s.object_id, N'IsUserTable') = 1 OR @IncludeSystemObjects = N'Y')
        OPTION (RECOMPILE); /*Prevents plan caching issues with DMV joins*/

        /*
        Warn about any stats not found
        */
        DECLARE
            @requested_count integer =
            (
                SELECT
                    COUNT_BIG(*)
                FROM STRING_SPLIT(@Statistics, N',') AS ss
                WHERE LTRIM(RTRIM(ss.value)) <> N''
            ),
            @found_count integer =
            (
                SELECT
                    COUNT_BIG(*)
                FROM #stats_to_process
            );

        IF @found_count < @requested_count
        BEGIN
            RAISERROR(N'Warning: Only %d of %d requested statistics found', 10, 1, @found_count, @requested_count) WITH NOWAIT;
        END;
    END;

    /*
    ============================================================================
    MODE 2: DISCOVERY - DMV-based candidate selection
    ============================================================================
    */
    IF @mode = N'DISCOVERY'
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
            Build dynamic SQL to run in target database context

            HEAP HANDLING NOTES:
            ====================
            - Heaps have index_id = 0 in sys.indexes (clustered = 1, nonclustered > 1)
            - Correct identification: EXISTS (index_id = 0), NOT "NOT EXISTS (index_id = 1)"
            - PageCount: For all stats, get total table pages from heap (0) or clustered (1)
            - stats_id != index_id for column statistics (only index stats have stats_id = index_id)
            - Auto-created stats on heaps still use _WA_Sys_ prefix
            */
            SELECT
                @discovery_sql = N'
            USE ' + QUOTENAME(@CurrentDatabaseName) + N';

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
            /*
            HEAP DETECTION: Check for index_id = 0 (heap) directly.
            Every table has either a heap (index_id=0) OR a clustered index (index_id=1), never both.
            */
            is_heap =
                CASE
                    WHEN EXISTS
                         (
                             SELECT
                                 1
                             FROM sys.indexes AS ix
                             WHERE ix.object_id = s.object_id
                             AND   ix.index_id = 0
                         )
                    THEN 1
                    ELSE 0
                END,
            modification_counter = ISNULL(sp.modification_counter, 0),
            row_count = ISNULL(sp.rows, 0),
            days_stale = ISNULL(DATEDIFF(DAY, sp.last_updated, GETDATE()), 9999),
            page_count = ISNULL(pgs.total_pages, 0),
            persisted_sample_percent = sp.persisted_sample_percent,
            /* Filtered statistics metadata */
            has_filter = s.has_filter,
            filter_definition = s.filter_definition,
            unfiltered_rows = sp.unfiltered_rows,
            /* Query Store priority (NULL if QS disabled or not requested) */
            qs_plan_count = qs_stats.plan_count,
            qs_total_executions = qs_stats.total_executions,
            qs_total_cpu_ms = qs_stats.total_cpu_ms,
            qs_total_duration_ms = qs_stats.total_duration_ms,
            qs_total_logical_reads = qs_stats.total_logical_reads,
            qs_last_execution = qs_stats.last_execution,
            /*
            Priority boost based on selected metric - uses resource consumption, not just execution count.
            Normalized to provide consistent ordering across metrics.
            */
            qs_priority_boost =
                CASE
                    WHEN @QueryStorePriority_param = N''Y''
                    AND  ISNULL(qs_stats.total_executions, 0) >= @QueryStoreMinExecutions_param
                    THEN CASE @QueryStoreMetric_param
                             WHEN N''CPU'' THEN ISNULL(qs_stats.total_cpu_ms, 0)
                             WHEN N''DURATION'' THEN ISNULL(qs_stats.total_duration_ms, 0)
                             WHEN N''READS'' THEN ISNULL(qs_stats.total_logical_reads, 0) / 1000 /* scale down */
                             WHEN N''EXECUTIONS'' THEN ISNULL(qs_stats.total_executions, 0)
                             ELSE ISNULL(qs_stats.total_cpu_ms, 0) /* default to CPU */
                         END
                    ELSE 0
                END,
            priority =
                ROW_NUMBER() OVER
                (
                    ORDER BY
                        /*
                        Query Store boost based on selected metric.
                        Uses actual resource consumption to prioritize expensive queries.
                        */
                        CASE
                            WHEN @QueryStorePriority_param = N''Y''
                            AND  ISNULL(qs_stats.total_executions, 0) >= @QueryStoreMinExecutions_param
                            THEN CASE @QueryStoreMetric_param
                                     WHEN N''CPU'' THEN ISNULL(qs_stats.total_cpu_ms, 0)
                                     WHEN N''DURATION'' THEN ISNULL(qs_stats.total_duration_ms, 0)
                                     WHEN N''READS'' THEN ISNULL(qs_stats.total_logical_reads, 0) / 1000
                                     WHEN N''EXECUTIONS'' THEN ISNULL(qs_stats.total_executions, 0)
                                     ELSE ISNULL(qs_stats.total_cpu_ms, 0)
                                 END
                            ELSE 0
                        END +
                        CASE @SortOrder_param
                            WHEN N''MODIFICATION_COUNTER''
                            THEN ISNULL(sp.modification_counter, 0)
                            WHEN N''DAYS_STALE''
                            THEN ISNULL(DATEDIFF(DAY, sp.last_updated, GETDATE()), 9999)
                            WHEN N''PAGE_COUNT''
                            THEN ISNULL(pgs.total_pages, 0)
                            WHEN N''RANDOM''
                            THEN CHECKSUM(NEWID())
                            WHEN N''QUERY_STORE''
                            THEN CASE @QueryStoreMetric_param
                                     WHEN N''CPU'' THEN ISNULL(qs_stats.total_cpu_ms, 0)
                                     WHEN N''DURATION'' THEN ISNULL(qs_stats.total_duration_ms, 0)
                                     WHEN N''READS'' THEN ISNULL(qs_stats.total_logical_reads, 0) / 1000
                                     WHEN N''EXECUTIONS'' THEN ISNULL(qs_stats.total_executions, 0)
                                     ELSE ISNULL(qs_stats.total_cpu_ms, 0)
                                 END
                            WHEN N''FILTERED_DRIFT''
                            THEN CASE
                                     WHEN s.has_filter = 1 AND ISNULL(sp.rows, 0) > 0 AND sp.unfiltered_rows IS NOT NULL
                                     THEN CONVERT(bigint, IIF((CONVERT(float, sp.unfiltered_rows) / sp.rows) > 9000000000000.0, 9000000000000.0, (CONVERT(float, sp.unfiltered_rows) / sp.rows)) * 1000000)
                                     ELSE 0
                                 END
                            ELSE ISNULL(sp.modification_counter, 0)
                        END DESC
                )
        FROM sys.stats AS s
        JOIN sys.objects AS o
          ON o.object_id = s.object_id
        LEFT JOIN sys.tables AS t
          ON t.object_id = s.object_id
        CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
        OUTER APPLY
        (
            /*
            PAGE COUNT: Get total table pages from the base table structure.
            - index_id = 0 for heaps
            - index_id = 1 for clustered indexes
            We always want the base table pages, regardless of which statistic we''re updating.
            Note: stats_id does NOT equal index_id for column statistics!
            */
            SELECT
                total_pages = SUM(p.used_page_count)
            FROM sys.dm_db_partition_stats AS p
            WHERE p.object_id = s.object_id
            AND   p.index_id IN (0, 1)
        ) AS pgs
        /*
        QUERY STORE CROSS-REFERENCE: Find plans that reference this object''s statistics.
        This identifies stats that are actively used by the query optimizer.
        Only runs the join if @QueryStorePriority_param = ''Y'' to avoid overhead.

        Query Store must be enabled on the database for this to return data.
        We use sys.query_store_plan to find plans, then aggregate resource consumption
        from sys.query_store_runtime_stats.

        RESOURCE METRICS (following sp_QuickieStore patterns):
        - total_cpu_ms: Total CPU consumption (avg_cpu_time is microseconds, * count / 1000 = ms)
        - total_duration_ms: Total elapsed time
        - total_logical_reads: Total I/O operations

        These help prioritize stats causing the most resource consumption, not just
        the most frequently executed. A single 10-second query matters more than
        1000 1-millisecond queries.

        Note: We match by object_id since Query Store tracks plans by object, not by
        individual statistic. A stat on a hot table will be prioritized even if
        we can''t determine exactly which stat column the plan used.
        */
        OUTER APPLY
        (
            /*
            OPTIMIZED JOIN ORDER: Filter by object_id FIRST through sys.query_store_query,
            then join to plans. This dramatically reduces intermediate result set size
            on databases with large Query Store catalogs (Erin Stellato recommendation).
            */
            SELECT
                plan_count = COUNT_BIG(DISTINCT qsp.plan_id),
                total_executions = SUM(qsrs.count_executions),
                /* CPU time: avg_cpu_time is in microseconds, convert to milliseconds */
                total_cpu_ms = SUM(qsrs.avg_cpu_time * qsrs.count_executions) / 1000,
                /* Duration: avg_duration is in microseconds, convert to milliseconds */
                total_duration_ms = SUM(qsrs.avg_duration * qsrs.count_executions) / 1000,
                /* Logical reads: direct sum of avg * count */
                total_logical_reads = SUM(CONVERT(bigint, qsrs.avg_logical_io_reads * qsrs.count_executions)),
                last_execution = MAX(qsrs.last_execution_time)
            FROM sys.query_store_query AS qsq  /* Filter by object_id FIRST */
            JOIN sys.query_store_plan AS qsp
              ON qsp.query_id = qsq.query_id
            JOIN sys.query_store_runtime_stats AS qsrs
              ON qsrs.plan_id = qsp.plan_id
            JOIN sys.query_store_runtime_stats_interval AS qsrsi
              ON qsrsi.runtime_stats_interval_id = qsrs.runtime_stats_interval_id
            WHERE qsq.object_id = s.object_id  /* Early filter reduces join cardinality */
            AND   @QueryStorePriority_param = N''Y''
            AND   qsrsi.end_time >= DATEADD(HOUR, -@QueryStoreRecentHours_param, GETDATE())
            /*
            Check Query Store is enabled and readable before querying.
            actual_state: 0=OFF, 1=READ_ONLY, 2=READ_WRITE, 3=ERROR
            Only query if state is READ_ONLY (1) or READ_WRITE (2).
            */
            AND   EXISTS
                  (
                      SELECT 1
                      FROM sys.database_query_store_options AS qso
                      WHERE qso.actual_state IN (1, 2)
                  )
        ) AS qs_stats
        WHERE (o.is_ms_shipped = 0 OR @IncludeSystemObjects_param = N''Y'')
        AND   (OBJECTPROPERTY(s.object_id, N''IsUserTable'') = 1 OR @IncludeSystemObjects_param = N''Y'')
        /* NORECOMPUTE filter */
        AND   (
                  (@TargetNorecompute_param = N''N'' AND s.no_recompute = 0)
               OR (@TargetNorecompute_param = N''Y'' AND s.no_recompute = 1)
               OR @TargetNorecompute_param = N''BOTH''
              )
        /* Page count threshold (includes heaps via index_id IN (0, 1)) - always required */
        AND   ISNULL(pgs.total_pages, 0) >= @MinPageCount_param
        /*
        THRESHOLD LOGIC: OR vs AND mode
        ================================
        OR mode (default): stat qualifies if ANY threshold is met
        AND mode: stat qualifies only if ALL specified thresholds are met

        When @TieredThresholds_param = 1, uses Tiger Toolbox 5-tier formula:
          0-500 rows: 500 modifications OR SQRT(rows * 1000) - avoids cliff effect
          501-10K rows: 20% + 500 OR SQRT(rows * 1000)
          10K-100K rows: 15% + 500 OR SQRT(rows * 1000)
          100K-1M rows: 10% + 500 OR SQRT(rows * 1000)
          1M+ rows: 5% + 500 OR SQRT(rows * 1000)
        */
        AND   (
              (
                  /* OR Logic: Any threshold triggers update */
                  @ThresholdLogic_param = N''OR''
                  AND (
                      /* Modification threshold */
                      (
                          @ModificationThreshold_param IS NOT NULL
                          AND ISNULL(sp.modification_counter, 0) >= @ModificationThreshold_param
                      )
                      /* Modification percent (SQRT-based formula) - only if not using tiered */
                      OR (
                          @TieredThresholds_param = 0
                          AND @ModificationPercent_param IS NOT NULL
                          AND sp.modification_counter >= (@ModificationPercent_param * SQRT(ISNULL(sp.rows, 1)))
                      )
                      /* Tiered thresholds (Tiger Toolbox 5-tier adaptive formula) */
                      OR (
                          @TieredThresholds_param = 1
                          AND (
                              /* 0-500 rows: 500 modifications OR SQRT(rows * 1000) - avoids cliff effect at 500/501 boundary */
                              (ISNULL(sp.rows, 0) <= 500 AND (ISNULL(sp.modification_counter, 0) >= 500 OR ISNULL(sp.modification_counter, 0) >= SQRT(CONVERT(float, ISNULL(sp.rows, 1)) * 1000)))
                              /* 501-10K rows: 20% + 500 OR SQRT(rows * 1000) */
                              OR (ISNULL(sp.rows, 0) BETWEEN 501 AND 10000 AND (ISNULL(sp.modification_counter, 0) >= (ISNULL(sp.rows, 0) * 20) / 100 + 500 OR ISNULL(sp.modification_counter, 0) >= SQRT(CONVERT(float, ISNULL(sp.rows, 1)) * 1000)))
                              /* 10K-100K rows: 15% + 500 OR SQRT(rows * 1000) */
                              OR (ISNULL(sp.rows, 0) BETWEEN 10001 AND 100000 AND (ISNULL(sp.modification_counter, 0) >= (ISNULL(sp.rows, 0) * 15) / 100 + 500 OR ISNULL(sp.modification_counter, 0) >= SQRT(CONVERT(float, ISNULL(sp.rows, 1)) * 1000)))
                              /* 100K-1M rows: 10% + 500 OR SQRT(rows * 1000) */
                              OR (ISNULL(sp.rows, 0) BETWEEN 100001 AND 1000000 AND (ISNULL(sp.modification_counter, 0) >= (ISNULL(sp.rows, 0) * 10) / 100 + 500 OR ISNULL(sp.modification_counter, 0) >= SQRT(CONVERT(float, ISNULL(sp.rows, 1)) * 1000)))
                              /* 1M+ rows: 5% + 500 OR SQRT(rows * 1000) */
                              OR (ISNULL(sp.rows, 0) >= 1000001 AND (ISNULL(sp.modification_counter, 0) >= CONVERT(bigint, CONVERT(float, ISNULL(sp.rows, 0)) * 5 / 100) + 500 OR ISNULL(sp.modification_counter, 0) >= SQRT(CONVERT(float, ISNULL(sp.rows, 1)) * 1000)))
                          )
                      )
                      /* Days stale threshold */
                      OR (
                          @DaysStaleThreshold_param IS NOT NULL
                          AND DATEDIFF(DAY, sp.last_updated, GETDATE()) >= @DaysStaleThreshold_param
                      )
                      /* If no thresholds specified, include all */
                      OR (
                          @ModificationThreshold_param IS NULL
                          AND @ModificationPercent_param IS NULL
                          AND @TieredThresholds_param = 0
                          AND @DaysStaleThreshold_param IS NULL
                      )
                  )
              )
        OR    (
                  /* AND Logic: All specified thresholds must be met */
                  @ThresholdLogic_param = N''AND''
                  AND (
                      /* Modification threshold - must be met if specified */
                      @ModificationThreshold_param IS NULL
                      OR ISNULL(sp.modification_counter, 0) >= @ModificationThreshold_param
                  )
                  AND (
                      /* Modification percent OR Tiered - must be met if specified */
                      (@ModificationPercent_param IS NULL AND @TieredThresholds_param = 0)
                      OR (@TieredThresholds_param = 0 AND sp.modification_counter >= (@ModificationPercent_param * SQRT(ISNULL(sp.rows, 1))))
                      OR (@TieredThresholds_param = 1 AND (
                          (ISNULL(sp.rows, 0) <= 500 AND (ISNULL(sp.modification_counter, 0) >= 500 OR ISNULL(sp.modification_counter, 0) >= SQRT(CONVERT(float, ISNULL(sp.rows, 1)) * 1000)))
                          OR (ISNULL(sp.rows, 0) BETWEEN 501 AND 10000 AND (ISNULL(sp.modification_counter, 0) >= (ISNULL(sp.rows, 0) * 20) / 100 + 500 OR ISNULL(sp.modification_counter, 0) >= SQRT(CONVERT(float, ISNULL(sp.rows, 1)) * 1000)))
                          OR (ISNULL(sp.rows, 0) BETWEEN 10001 AND 100000 AND (ISNULL(sp.modification_counter, 0) >= (ISNULL(sp.rows, 0) * 15) / 100 + 500 OR ISNULL(sp.modification_counter, 0) >= SQRT(CONVERT(float, ISNULL(sp.rows, 1)) * 1000)))
                          OR (ISNULL(sp.rows, 0) BETWEEN 100001 AND 1000000 AND (ISNULL(sp.modification_counter, 0) >= (ISNULL(sp.rows, 0) * 10) / 100 + 500 OR ISNULL(sp.modification_counter, 0) >= SQRT(CONVERT(float, ISNULL(sp.rows, 1)) * 1000)))
                          OR (ISNULL(sp.rows, 0) >= 1000001 AND (ISNULL(sp.modification_counter, 0) >= CONVERT(bigint, CONVERT(float, ISNULL(sp.rows, 0)) * 5 / 100) + 500 OR ISNULL(sp.modification_counter, 0) >= SQRT(CONVERT(float, ISNULL(sp.rows, 1)) * 1000)))
                      ))
                  )
                  AND (
                      /* Days stale - must be met if specified */
                      @DaysStaleThreshold_param IS NULL
                      OR DATEDIFF(DAY, sp.last_updated, GETDATE()) >= @DaysStaleThreshold_param
                  )
              )
              ) /* End of threshold logic wrapper - ensures table/exclusion filters apply to both OR and AND modes */
        /* Table filter */
        AND   (
                  @Tables_param IS NULL
               OR OBJECT_SCHEMA_NAME(s.object_id) + N''.'' + OBJECT_NAME(s.object_id) IN
                  (
                      SELECT
                          LTRIM(RTRIM(ss.value))
                      FROM STRING_SPLIT(@Tables_param, N'','') AS ss
                  )
               OR OBJECT_NAME(s.object_id) IN
                  (
                      SELECT
                          LTRIM(RTRIM(ss.value))
                      FROM STRING_SPLIT(@Tables_param, N'','') AS ss
                  )
              )
        /* Statistics exclusion filter (pattern-based) */
        AND   (
                  @ExcludeStatistics_param IS NULL
               OR NOT EXISTS
                  (
                      SELECT 1
                      FROM STRING_SPLIT(@ExcludeStatistics_param, N'','') AS ex
                      WHERE s.name LIKE LTRIM(RTRIM(ex.value))
                  )
              )
        /*
        FILTERED STATISTICS FILTER
        ==========================
        INCLUDE (default) = normal behavior, include all stats
        EXCLUDE = skip filtered stats entirely
        ONLY = only process filtered stats
        PRIORITY = include all, but filtered stats with drift get priority boost
        */
        AND   (
                  @FilteredStatsMode_param = N''INCLUDE''
               OR @FilteredStatsMode_param = N''PRIORITY''
               OR (@FilteredStatsMode_param = N''EXCLUDE'' AND s.has_filter = 0)
               OR (@FilteredStatsMode_param = N''ONLY'' AND s.has_filter = 1)
              )
        /*
        FILTERED STATS STALENESS: In PRIORITY mode, filtered stats use a LOWER modification
        threshold proportional to their selectivity. A filter covering 10% of rows should
        update after 10% of the normal modifications (adjusted by @FilteredStatsStaleFactor).

        The formula: mod_counter >= @ModificationThreshold * selectivity * factor
        Where selectivity = rows / unfiltered_rows (0.1 for 10% filter)

        Note: unfiltered_rows/rows measures selectivity, not staleness. Filtered stats on
        small subsets need lower absolute thresholds since each modification has more impact.
        */
        AND   (
                  @FilteredStatsMode_param <> N''PRIORITY''
               OR s.has_filter = 0
               OR sp.unfiltered_rows IS NULL
               OR ISNULL(sp.rows, 0) = 0
               /* Filtered stat qualifies if mod_counter exceeds selectivity-adjusted threshold */
               OR ISNULL(sp.modification_counter, 0) >=
                  (@ModificationThreshold_param * @FilteredStatsStaleFactor_param *
                   (CONVERT(float, ISNULL(sp.rows, 1)) / CONVERT(float, ISNULL(sp.unfiltered_rows, 1))))
               /* Or if it meets the normal threshold anyway */
               OR ISNULL(sp.modification_counter, 0) >= @ModificationThreshold_param
              )
        ORDER BY
            priority
        OPTION (RECOMPILE);'; /*Prevents plan caching issues with varying parameters*/

            SELECT
                @discovery_params = N'
                @SortOrder_param nvarchar(50),
                @TargetNorecompute_param nvarchar(10),
                @ModificationThreshold_param bigint,
                @ModificationPercent_param float,
                @TieredThresholds_param bit,
                @ThresholdLogic_param nvarchar(3),
                @DaysStaleThreshold_param integer,
                @MinPageCount_param bigint,
                @IncludeSystemObjects_param nvarchar(1),
                @Tables_param nvarchar(max),
                @ExcludeStatistics_param nvarchar(max),
                @FilteredStatsMode_param nvarchar(10),
                @FilteredStatsStaleFactor_param float,
                @QueryStorePriority_param nvarchar(1),
                @QueryStoreMetric_param nvarchar(20),
                @QueryStoreMinExecutions_param bigint,
                @QueryStoreRecentHours_param integer';

        /*
        Execute discovery in target database
        */
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
            is_heap,
            modification_counter,
            row_count,
            days_stale,
            page_count,
            persisted_sample_percent,
            has_filter,
            filter_definition,
            unfiltered_rows,
            qs_plan_count,
            qs_total_executions,
            qs_total_cpu_ms,
            qs_total_duration_ms,
            qs_total_logical_reads,
            qs_last_execution,
            qs_priority_boost,
            priority
        )
            EXECUTE sys.sp_executesql
                @discovery_sql,
                @discovery_params,
                @SortOrder_param = @SortOrder,
                @TargetNorecompute_param = @TargetNorecompute,
                @ModificationThreshold_param = @ModificationThreshold,
                @ModificationPercent_param = @ModificationPercent,
                @TieredThresholds_param = @TieredThresholds,
                @ThresholdLogic_param = @ThresholdLogic,
                @DaysStaleThreshold_param = @DaysStaleThreshold,
                @MinPageCount_param = @MinPageCount,
                @IncludeSystemObjects_param = @IncludeSystemObjects,
                @Tables_param = @Tables,
                @ExcludeStatistics_param = @ExcludeStatistics,
                @FilteredStatsMode_param = @FilteredStatsMode,
                @FilteredStatsStaleFactor_param = @FilteredStatsStaleFactor,
                @QueryStorePriority_param = @QueryStorePriority,
                @QueryStoreMetric_param = @QueryStoreMetric,
                @QueryStoreMinExecutions_param = @QueryStoreMinExecutions,
                @QueryStoreRecentHours_param = @QueryStoreRecentHours;

            /*
            Mark this database as completed
            */
            UPDATE @tmpDatabases
            SET Completed = 1
            WHERE ID = @CurrentDatabaseID;

        END; /* End of WHILE database loop */
    END; /* End of IF @mode = N'DISCOVERY' */

    /*
    ============================================================================
    REPORT DISCOVERED STATISTICS
    ============================================================================
    */
    DECLARE
        @total_stats integer =
        (
            SELECT
                COUNT_BIG(*)
            FROM #stats_to_process
        ),
        @norecompute_stats integer =
        (
            SELECT
                COUNT_BIG(*)
            FROM #stats_to_process AS stp
            WHERE stp.no_recompute = 1
        ),
        @incremental_stats integer =
        (
            SELECT
                COUNT_BIG(*)
            FROM #stats_to_process AS stp
            WHERE stp.is_incremental = 1
        ),
        @heap_stats integer =
        (
            SELECT
                COUNT_BIG(*)
            FROM #stats_to_process AS stp
            WHERE stp.is_heap = 1
        ),
        @memory_optimized_stats integer =
        (
            SELECT
                COUNT_BIG(*)
            FROM #stats_to_process AS stp
            WHERE stp.is_memory_optimized = 1
        ),
        @persisted_sample_stats integer =
        (
            SELECT
                COUNT_BIG(*)
            FROM #stats_to_process AS stp
            WHERE stp.persisted_sample_percent IS NOT NULL
        );

    RAISERROR(N'', 10, 1) WITH NOWAIT;
    RAISERROR(N'Found %d qualifying statistics:', 10, 1, @total_stats) WITH NOWAIT;
    RAISERROR(N'  - NORECOMPUTE:      %d', 10, 1, @norecompute_stats) WITH NOWAIT;
    RAISERROR(N'  - Incremental:      %d', 10, 1, @incremental_stats) WITH NOWAIT;
    RAISERROR(N'  - On heaps:         %d', 10, 1, @heap_stats) WITH NOWAIT;
    RAISERROR(N'  - Memory-optimized: %d', 10, 1, @memory_optimized_stats) WITH NOWAIT;
    RAISERROR(N'  - Persisted sample: %d', 10, 1, @persisted_sample_stats) WITH NOWAIT;
    RAISERROR(N'', 10, 1) WITH NOWAIT;

    IF @total_stats = 0
    BEGIN
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
            @DurationSecondsOut = DATEDIFF(second, @start_time, GETDATE());

        RETURN 0;
    END;

    /*
    ============================================================================
    PARALLEL MODE: QUEUE INITIALIZATION
    ============================================================================

    When @StatsInParallel = 'Y':
    - First worker creates/claims Queue row and populates QueueStatistic
    - QueueStatistic stores ONE ROW PER TABLE (not per stat)
    - This ensures no two workers update stats on the same table concurrently
    - Dead workers detected via sys.dm_exec_requests

    Queue claim uses UPDLOCK, HOLDLOCK pattern from Ola Hallengren.
    */
    IF @StatsInParallel = N'Y'
    BEGIN
        RAISERROR(N'', 10, 1) WITH NOWAIT;
        RAISERROR(N'Parallel mode: Initializing work queue...', 10, 1) WITH NOWAIT;

        /*
        Build parameters string to identify this run.
        Workers with matching parameters share the same queue.
        */
        /*
        Normalize parameter values with TRIM to prevent duplicate queues from
        whitespace differences (e.g., 'MyDB' vs ' MyDB').
        */
        SELECT
            @parameters_string =
                N'@Databases=' + ISNULL(LTRIM(RTRIM(@Databases)), N'') +
                N',@Tables=' + ISNULL(LTRIM(RTRIM(@Tables)), N'') +
                N',@TargetNorecompute=' + ISNULL(LTRIM(RTRIM(@TargetNorecompute)), N'') +
                N',@ModificationThreshold=' + ISNULL(CONVERT(nvarchar(20), @ModificationThreshold), N'') +
                N',@MinPageCount=' + ISNULL(CONVERT(nvarchar(20), @MinPageCount), N'') +
                N',@IncludeSystemObjects=' + ISNULL(LTRIM(RTRIM(@IncludeSystemObjects)), N'') +
                N',@SortOrder=' + ISNULL(LTRIM(RTRIM(@SortOrder)), N'');

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
                END;

                COMMIT TRANSACTION;
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
            Only claim if previous leader is dead (not in dm_exec_requests)
            */
            AND   NOT EXISTS
                  (
                      SELECT
                          1
                      FROM sys.dm_exec_requests AS r
                      WHERE r.session_id = q.SessionID
                      AND   r.request_id = q.RequestID
                      AND   r.start_time = q.RequestStartTime
                  )
            /*
            And no active workers in QueueStatistic
            */
            AND   NOT EXISTS
                  (
                      SELECT
                          1
                      FROM dbo.QueueStatistic AS qs
                      JOIN sys.dm_exec_requests AS r
                        ON r.session_id = qs.SessionID
                       AND r.request_id = qs.RequestID
                       AND r.start_time = qs.RequestStartTime
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
                        ORDER BY
                            CASE WHEN @SortOrder = N'MODIFICATION_COUNTER' THEN MAX(stp.modification_counter) END DESC,
                            CASE WHEN @SortOrder = N'PAGE_COUNT' THEN MAX(stp.page_count) END DESC,
                            CASE WHEN @SortOrder = N'DAYS_STALE' THEN MAX(stp.days_stale) END DESC,
                            CASE WHEN @SortOrder = N'RANDOM' THEN CHECKSUM(NEWID()) END DESC
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
            modifications = FORMAT(stp.modification_counter, N'N0'),
            days_stale = stp.days_stale,
            pages = FORMAT(stp.page_count, N'N0')
        FROM #stats_to_process AS stp
        ORDER BY
            stp.priority;
    END;

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
    WHILE 1 = 1
    BEGIN
        /*
        Capture iteration time once for consistent elapsed calculations
        */
        SELECT @iteration_time = SYSDATETIME();

        /*
        Check time limit
        */
        IF  @TimeLimit IS NOT NULL
        AND DATEDIFF(SECOND, @start_time, SYSDATETIME()) >= @TimeLimit
        BEGIN
            RAISERROR(N'', 10, 1) WITH NOWAIT;
            RAISERROR(N'Time limit (%d seconds) reached. Stopping gracefully.', 10, 1, @TimeLimit) WITH NOWAIT;
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
                A worker is dead if its session/request is no longer in dm_exec_requests.
                */
                UPDATE
                    qs
                SET
                    qs.TableStartTime = NULL,
                    qs.SessionID = NULL,
                    qs.RequestID = NULL,
                    qs.RequestStartTime = NULL
                FROM dbo.QueueStatistic AS qs
                WHERE qs.QueueID = @queue_id
                AND   qs.TableStartTime IS NOT NULL
                AND   qs.TableEndTime IS NULL
                AND   NOT EXISTS
                      (
                          SELECT
                              1
                          FROM sys.dm_exec_requests AS r
                          WHERE r.session_id = qs.SessionID
                          AND   r.request_id = qs.RequestID
                          AND   r.start_time = qs.RequestStartTime
                      );

                DECLARE
                    @released_count integer = ROWCOUNT_BIG();

                IF @released_count > 0
                AND @Debug = 1
                BEGIN
                    RAISERROR(N'  Released %d tables from dead workers', 10, 1, @released_count) WITH NOWAIT;
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

                UPDATE
                    qs
                SET
                    qs.TableStartTime = SYSDATETIME(),
                    qs.SessionID = @@SPID,
                    qs.RequestID =
                    (
                        SELECT
                            r.request_id
                        FROM sys.dm_exec_requests AS r
                        WHERE r.session_id = @@SPID
                    ),
                    qs.RequestStartTime =
                    (
                        SELECT
                            r.start_time
                        FROM sys.dm_exec_requests AS r
                        WHERE r.session_id = @@SPID
                    )
                OUTPUT
                    inserted.DatabaseName,
                    inserted.SchemaName,
                    inserted.ObjectName,
                    inserted.ObjectID
                INTO @claimed_tables
                FROM dbo.QueueStatistic AS qs
                WHERE qs.QueueID = @queue_id
                AND   qs.TableStartTime IS NULL
                AND   qs.TablePriority =
                      (
                          SELECT
                              MIN(qs2.TablePriority)
                          FROM dbo.QueueStatistic AS qs2
                          WHERE qs2.QueueID = @queue_id
                          AND   qs2.TableStartTime IS NULL
                      );

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
                END;
                ELSE
                BEGIN
                    /*
                    No more tables to claim - all work done or claimed by others
                    */
                    SELECT
                        @stop_reason = N'PARALLEL_COMPLETE';
                    BREAK;
                END;
            END;
        END;

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
            @current_modification_counter = stp.modification_counter,
            @current_row_count = stp.row_count,
            @current_days_stale = stp.days_stale,
            @current_page_count = stp.page_count,
            @current_persisted_sample_percent = stp.persisted_sample_percent,
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
            @current_qs_last_execution = stp.qs_last_execution,
            @current_qs_priority_boost = stp.qs_priority_boost
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
            HEAP FORWARDING POINTERS: Query dm_db_index_physical_stats for heaps only.
            Forwarding pointers occur when rows grow and must be relocated.
            High counts indicate fragmentation that hurts scan performance.
            Note: Uses SAMPLED mode for performance (DETAILED is very slow).
            */
            IF @current_is_heap = 1
            BEGIN
                SELECT
                    @current_forwarded_records = ps.forwarded_record_count
                FROM sys.dm_db_index_physical_stats
                (
                    DB_ID(@current_database),
                    @current_object_id,
                    0,
                    NULL,
                    N'SAMPLED'
                ) AS ps
                WHERE ps.index_id = 0
                AND   ps.alloc_unit_type_desc = N'IN_ROW_DATA';
            END;

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

        SELECT
            @stats_processed += 1,
            @current_start_time = SYSDATETIME();

        /*
        ========================================================================
        BUILD UPDATE STATISTICS COMMAND
        ========================================================================
        */
        SELECT
            @current_command = N'';

        /*
        Lock timeout
        */
        IF @LockTimeout IS NOT NULL
        BEGIN
            SELECT
                @current_command =
                    N'SET LOCK_TIMEOUT ' +
                    CONVERT(nvarchar(20), CONVERT(bigint, @LockTimeout) * 1000) +
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
        */
        DECLARE
            @has_with_option bit = 0,
            @with_clause nvarchar(max) = N'';

        /*
        Memory-optimized tables have special requirements
        - SQL Server 2014: Requires FULLSCAN or RESAMPLE, no sampling
        - SQL Server 2016+: Supports sampling
        */
        IF  @current_is_memory_optimized = 1
        AND @sql_version < 13
        AND @StatisticsSample IS NOT NULL
        AND @StatisticsSample < 100
        AND @StatisticsResample = 0
        BEGIN
            /*
            Force FULLSCAN for memory-optimized on SQL 2014
            */
            SELECT
                @with_clause = N'FULLSCAN',
                @has_with_option = 1;

            IF @Debug = 1
            BEGIN
                RAISERROR(N'  Note: Memory-optimized table on SQL 2014 requires FULLSCAN', 10, 1) WITH NOWAIT;
            END;
        END;
        ELSE IF @StatisticsResample = 1
        BEGIN
            /*
            RESAMPLE: use previous sample rate (required for incremental stats)
            */
            SELECT
                @with_clause = N'RESAMPLE',
                @has_with_option = 1;
        END;
        ELSE IF @StatisticsSample = 100
        BEGIN
            SELECT
                @with_clause = N'FULLSCAN',
                @has_with_option = 1;
        END;
        ELSE IF @StatisticsSample IS NOT NULL
        BEGIN
            SELECT
                @with_clause =
                    N'SAMPLE ' +
                    CONVERT(nvarchar(10), @StatisticsSample) +
                    N' PERCENT',
                @has_with_option = 1;
        END;
        /*
        RESPECT PERSISTED SAMPLE PERCENT
        When @StatisticsSample is NULL and the stat has a persisted sample,
        honor the existing setting by using RESAMPLE.
        This preserves the sample rate without overriding DBA-tuned values.
        */
        ELSE IF @StatisticsSample IS NULL
        AND     @current_persisted_sample_percent IS NOT NULL
        AND     @StatisticsResample = 0  /*Don't double-specify RESAMPLE*/
        BEGIN
            SELECT
                @with_clause = N'RESAMPLE',
                @has_with_option = 1;

            IF @Debug = 1
            BEGIN
                SELECT @persisted_pct_msg = CONVERT(integer, @current_persisted_sample_percent);
                RAISERROR(N'  Note: Respecting persisted sample %d%% via RESAMPLE', 10, 1, @persisted_pct_msg) WITH NOWAIT;
            END;
        END;

        /*
        Incremental statistics: ON PARTITIONS()
        Only applies to incremental stats on partitioned tables.
        Query sys.dm_db_incremental_stats_properties to find stale partitions.
        */
        DECLARE
            @incremental_partitions nvarchar(max) = NULL,
            @incremental_partition_count int = 0,
            @incremental_total_partitions int = 0;

        IF  @UpdateIncremental = 1
        AND @current_is_incremental = 1
        BEGIN
            /*
            Query dm_db_incremental_stats_properties to find partitions with modifications.
            Only update partitions that exceed the modification threshold.
            This is the correct behavior for incremental statistics - updating all
            partitions defeats the purpose of incremental stats.
            */
            DECLARE @partition_sql nvarchar(max) = N'
                SELECT @partitions_out = STRING_AGG(CONVERT(nvarchar(10), isp.partition_number), N'', '')
                                         WITHIN GROUP (ORDER BY isp.partition_number),
                       @count_out = COUNT(*),
                       @total_out = (SELECT COUNT(*) FROM sys.dm_db_incremental_stats_properties(@obj_id, @stat_id))
                FROM sys.dm_db_incremental_stats_properties(@obj_id, @stat_id) AS isp
                WHERE isp.modification_counter > 0';

            BEGIN TRY
                EXEC sys.sp_executesql
                    @partition_sql,
                    N'@obj_id int, @stat_id int, @partitions_out nvarchar(max) OUTPUT, @count_out int OUTPUT, @total_out int OUTPUT',
                    @obj_id = @current_object_id,
                    @stat_id = @current_stats_id,
                    @partitions_out = @incremental_partitions OUTPUT,
                    @count_out = @incremental_partition_count OUTPUT,
                    @total_out = @incremental_total_partitions OUTPUT;
            END TRY
            BEGIN CATCH
                /*
                If STRING_AGG fails (SQL 2016), fall back to FOR XML PATH
                */
                SELECT @partition_sql = N'
                    SELECT @partitions_out = STUFF((
                               SELECT N'', '' + CONVERT(nvarchar(10), isp.partition_number)
                               FROM sys.dm_db_incremental_stats_properties(@obj_id, @stat_id) AS isp
                               WHERE isp.modification_counter > 0
                               ORDER BY isp.partition_number
                               FOR XML PATH(''''), TYPE
                           ).value(''.'', ''nvarchar(max)''), 1, 2, N''''),
                           @count_out = (SELECT COUNT(*) FROM sys.dm_db_incremental_stats_properties(@obj_id, @stat_id)
                                         WHERE modification_counter > 0),
                           @total_out = (SELECT COUNT(*) FROM sys.dm_db_incremental_stats_properties(@obj_id, @stat_id))';

                EXEC sys.sp_executesql
                    @partition_sql,
                    N'@obj_id int, @stat_id int, @partitions_out nvarchar(max) OUTPUT, @count_out int OUTPUT, @total_out int OUTPUT',
                    @obj_id = @current_object_id,
                    @stat_id = @current_stats_id,
                    @partitions_out = @incremental_partitions OUTPUT,
                    @count_out = @incremental_partition_count OUTPUT,
                    @total_out = @incremental_total_partitions OUTPUT;
            END CATCH;

            /*
            Add ON PARTITIONS clause if we found specific stale partitions.
            If ALL partitions are stale, skip ON PARTITIONS (full RESAMPLE is more efficient).
            If NO partitions are stale, we shouldn't be here (discovery should have filtered).
            */
            IF  @incremental_partitions IS NOT NULL
            AND @incremental_partition_count > 0
            AND @incremental_partition_count < @incremental_total_partitions
            BEGIN
                SELECT
                    @current_command += N' ON PARTITIONS(' + @incremental_partitions + N')';

                IF @Debug = 1
                BEGIN
                    RAISERROR(N'  Note: Incremental statistics - updating %d of %d partitions', 10, 1,
                        @incremental_partition_count, @incremental_total_partitions) WITH NOWAIT;
                END;
            END;
            ELSE IF @Debug = 1
            BEGIN
                IF @incremental_partition_count = @incremental_total_partitions
                BEGIN
                    RAISERROR(N'  Note: Incremental statistics - all %d partitions stale, full RESAMPLE', 10, 1,
                        @incremental_total_partitions) WITH NOWAIT;
                END;
                ELSE
                BEGIN
                    RAISERROR(N'  Note: Incremental statistics - full update (partition info unavailable)', 10, 1) WITH NOWAIT;
                END;
            END;

            /*
            Incremental stats require RESAMPLE
            */
            IF @StatisticsResample = 0
            AND @StatisticsSample IS NULL
            BEGIN
                IF @has_with_option = 0
                BEGIN
                    SELECT
                        @with_clause = N'RESAMPLE',
                        @has_with_option = 1;
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
        IF  @PersistSamplePercent = N'Y'
        AND @supports_persist_sample = 1
        AND @has_with_option = 1  /* Only add if FULLSCAN/SAMPLE already specified */
        AND @with_clause NOT LIKE N'%RESAMPLE%'  /* RESAMPLE and PERSIST_SAMPLE_PERCENT conflict (Error 1052) */
        BEGIN
            SELECT
                @with_clause += N', PERSIST_SAMPLE_PERCENT = ON';
        END;
        ELSE IF @PersistSamplePercent = N'Y' AND @supports_persist_sample = 0 AND @Debug = 1
        BEGIN
            RAISERROR(N'  Note: @PersistSamplePercent ignored - requires SQL 2016 SP1 CU4+', 10, 1) WITH NOWAIT;
        END;
        ELSE IF @PersistSamplePercent = N'Y' AND @with_clause LIKE N'%RESAMPLE%' AND @Debug = 1
        BEGIN
            RAISERROR(N'  Note: @PersistSamplePercent ignored - conflicts with RESAMPLE (Error 1052)', 10, 1) WITH NOWAIT;
        END;
        ELSE IF @PersistSamplePercent = N'Y' AND @has_with_option = 0 AND @Debug = 1
        BEGIN
            RAISERROR(N'  Note: @PersistSamplePercent requires FULLSCAN or SAMPLE', 10, 1) WITH NOWAIT;
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
                N', stale: ' + CONVERT(nvarchar(10), @current_days_stale) + N' days' +
                CASE
                    WHEN @current_is_incremental = 1
                    THEN N', INCREMENTAL'
                    ELSE N''
                END +
                CASE
                    WHEN @current_is_heap = 1
                    AND  @current_forwarded_records > 0
                    THEN N', HEAP (fwd: ' + FORMAT(@current_forwarded_records, N'N0') + N')'
                    WHEN @current_is_heap = 1
                    THEN N', HEAP'
                    ELSE N''
                END +
                CASE
                    WHEN @current_is_memory_optimized = 1
                    THEN N', MEMORY'
                    ELSE N''
                END +
                N', NORECOMPUTE: ' + @norecompute_display + N')';

        RAISERROR(@progress_msg, 10, 1) WITH NOWAIT;

        IF @Debug = 1
        BEGIN
            RAISERROR(N'  Command: %s', 10, 1, @current_command) WITH NOWAIT;
        END;

        IF @Execute = N'Y'
        BEGIN
            BEGIN TRY
                EXECUTE sys.sp_executesql
                    @current_command;

                SELECT
                    @current_end_time = SYSDATETIME(),
                    @stats_succeeded += 1,
                    @claimed_table_stats_updated += CASE WHEN @StatsInParallel = N'Y' THEN 1 ELSE 0 END,
                    @duration_ms = DATEDIFF(MILLISECOND, @current_start_time, @current_end_time),
                    @progress_msg = N'  Complete (' + CONVERT(nvarchar(10), @duration_ms) + N' ms)';

                RAISERROR(@progress_msg, 10, 1) WITH NOWAIT;

                /*
                Log to CommandLog
                */
                IF  @LogToTable = N'Y'
                AND @commandlog_exists = 1
                BEGIN
                    SELECT
                        @current_extended_info =
                        (
                            SELECT
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
                                @current_forwarded_records AS ForwardedRecords,
                                @current_is_memory_optimized AS IsMemoryOptimized,
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
                                @current_qs_last_execution AS QSLastExecution,
                                @current_qs_priority_boost AS QSPriorityBoost,
                                @QueryStoreMetric AS QSMetric,
                                CASE
                                    WHEN @mode IN (N'DIRECT_STRING', N'DIRECT_TABLE')
                                    THEN N'DIRECT_MODE'
                                    WHEN @current_qs_priority_boost > 0
                                    THEN N'QUERY_STORE_PRIORITY'
                                    WHEN @current_has_filter = 1
                                    AND  @FilteredStatsMode = N'PRIORITY'
                                    AND  @current_filtered_drift_ratio >= @FilteredStatsStaleFactor
                                    THEN N'FILTERED_DRIFT'
                                    WHEN @current_no_recompute = 1
                                    AND  @TargetNorecompute IN (N'Y', N'BOTH')
                                    THEN N'NORECOMPUTE_TARGET'
                                    WHEN @current_days_stale >= ISNULL(@DaysStaleThreshold, 999999)
                                    THEN N'DAYS_STALE'
                                    WHEN @TieredThresholds = 1
                                    THEN N'TIERED_THRESHOLD'
                                    WHEN @ModificationPercent IS NOT NULL
                                    AND  (@current_modification_counter * 100.0 / NULLIF(@current_row_count, 0)) >= @ModificationPercent
                                    THEN N'MOD_PERCENT'
                                    WHEN @current_modification_counter >= ISNULL(@ModificationThreshold, 0)
                                    THEN N'MOD_COUNTER'
                                    ELSE N'THRESHOLD_MATCH'
                                END AS QualifyReason,
                                @StatisticsSample AS SamplePct,
                                @StatisticsResample AS UsedResample,
                                @mode AS Mode
                            FOR
                                XML RAW(N'ExtendedInfo'),
                                ELEMENTS
                        );

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
                        StartTime,
                        EndTime,
                        ErrorNumber,
                        ErrorMessage,
                        ExtendedInfo
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
                        @current_start_time,
                        @current_end_time,
                        0,
                        NULL,
                        @current_extended_info
                    );

                    /*
                    Progress logging at interval (for Agent job monitoring).
                    Logs SP_STATUPDATE_PROGRESS entry every N stats processed.
                    */
                    IF  @ProgressLogInterval IS NOT NULL
                    AND @stats_processed % @ProgressLogInterval = 0
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
                SELECT
                    @current_end_time = SYSDATETIME(),
                    @current_error_number = ERROR_NUMBER(),
                    @current_error_message = ERROR_MESSAGE(),
                    @stats_failed += 1,
                    @claimed_table_stats_failed += CASE WHEN @StatsInParallel = N'Y' THEN 1 ELSE 0 END;

                RAISERROR(N'  X Error %d: %s', 16, 1, @current_error_number, @current_error_message) WITH NOWAIT;

                /*
                Log error to CommandLog
                */
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
                            StartTime,
                            EndTime,
                            ErrorNumber,
                            ErrorMessage
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
                            @current_start_time,
                            @current_end_time,
                            @current_error_number,
                            @current_error_message
                        );
                    END TRY
                    BEGIN CATCH
                        SELECT @log_error_msg = LEFT(ERROR_MESSAGE(), 3900);
                        RAISERROR(N'  WARNING: Failed to log error to CommandLog (%s)', 10, 1, @log_error_msg) WITH NOWAIT;
                    END CATCH;
                END;

                SELECT
                    @return_code = @current_error_number;

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
            END CATCH;
        END;
        ELSE
        BEGIN
            /*
            Dry run - just show command
            */
            RAISERROR(N'  [DRY RUN] %s', 10, 1, @current_command) WITH NOWAIT;
            SELECT
                @stats_skipped += 1,
                @claimed_table_stats_skipped += CASE WHEN @StatsInParallel = N'Y' THEN 1 ELSE 0 END;
        END;

        /*
        Delay between stats if specified
        */
        IF  @DelayBetweenStats IS NOT NULL
        AND @DelayBetweenStats > 0
        AND @Execute = N'Y'
        BEGIN
            DECLARE
                @delay_time datetime = DATEADD(SECOND, @DelayBetweenStats, '00:00:00');

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

    /*
    ============================================================================
    SUMMARY
    ============================================================================
    */
    DECLARE
        @end_time datetime2(7) = SYSDATETIME(),
        @duration_seconds integer = DATEDIFF(SECOND, @start_time, SYSDATETIME()),
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
    RAISERROR(N'Duration:        %d seconds', 10, 1, @duration_seconds) WITH NOWAIT;
    RAISERROR(N'', 10, 1) WITH NOWAIT;
    RAISERROR(N'Stats processed: %d / %d', 10, 1, @stats_processed, @total_stats) WITH NOWAIT;
    RAISERROR(N'  Succeeded:     %d', 10, 1, @stats_succeeded) WITH NOWAIT;
    RAISERROR(N'  Failed:        %d', 10, 1, @stats_failed) WITH NOWAIT;
    RAISERROR(N'  Skipped:       %d (dry run)', 10, 1, @stats_skipped) WITH NOWAIT;
    RAISERROR(N'  Remaining:     %d', 10, 1, @remaining_stats) WITH NOWAIT;
    RAISERROR(N'', 10, 1) WITH NOWAIT;

    IF @remaining_stats > 0
    BEGIN
        RAISERROR(N'Note: %d stats remain. Re-run sp_StatUpdate to continue.', 10, 1, @remaining_stats) WITH NOWAIT;
    END;

    RAISERROR(N'===============================================================================', 10, 1) WITH NOWAIT;

    /*
    ============================================================================
    LOG RUN_FOOTER TO COMMANDLOG
    ============================================================================
    */
    IF  @LogToTable = N'Y'
    AND @Execute = N'Y'
    AND @commandlog_exists = 1
    BEGIN
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
                    @remaining_stats AS StatsRemaining,
                    @duration_seconds AS DurationSeconds,
                    @stop_reason AS StopReason
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
    SELECT
        @StatsFoundOut = @total_stats,
        @StatsProcessedOut = @stats_processed,
        @StatsSucceededOut = @stats_succeeded,
        @StatsFailedOut = @stats_failed,
        @StatsRemainingOut = @remaining_stats,
        @DurationSecondsOut = @duration_seconds;

    /*
    ============================================================================
    RETURN SUMMARY RESULT SET
    ============================================================================
    Provides programmatic access to run statistics.
    Enables automation scripts to capture and react to results.
    */
    SELECT
        StatsFound = @total_stats,
        StatsProcessed = @stats_processed,
        StatsSucceeded = @stats_succeeded,
        StatsFailed = @stats_failed,
        StatsSkipped = @stats_skipped,
        StatsRemaining = @remaining_stats,
        DatabasesProcessed = @database_count,
        DurationSeconds = @duration_seconds,
        StopReason = @stop_reason,
        RunLabel = @run_label,
        Version = @procedure_version;

    /*
    Ensure non-zero return code when failures occurred (Agent job detection)
    */
    IF @stats_failed > 0 AND @return_code = 0
    BEGIN
        SELECT @return_code = 1; /*Generic failure code for Agent jobs*/
    END;

    RETURN @return_code;
END;
GO

/*
===============================================================================
USAGE EXAMPLES
===============================================================================

-------------------------------------------------------------------------------
HELP
-------------------------------------------------------------------------------

-- Show help in SSMS result set
EXECUTE dbo.sp_StatUpdate @Help = 1;

-- Get version info
DECLARE @v varchar(20), @d datetime;
EXECUTE dbo.sp_StatUpdate @version = @v OUTPUT, @version_date = @d OUTPUT;
SELECT @v AS version, @d AS version_date;

-------------------------------------------------------------------------------
DIRECT MODE - Update specific statistics
-------------------------------------------------------------------------------

-- Update a single specific statistic
EXECUTE dbo.sp_StatUpdate
    @Statistics = N'dbo.Customers._WA_Sys_00000001_ABC123';

-- Update multiple specific statistics
EXECUTE dbo.sp_StatUpdate
    @Statistics = N'dbo.Customers._WA_Sys_00000001_ABC123, dbo.Orders.PK_Orders',
    @Execute = N'N';

-------------------------------------------------------------------------------
DIRECT MODE (TABLE) - From QueryStore priority queue
-------------------------------------------------------------------------------

EXECUTE dbo.sp_StatUpdate
    @StatisticsFromTable = N'Maintenance.dbo.StatsPriorityQueue',
    @StatisticsSample = 100,
    @TimeLimit = 3600;

-------------------------------------------------------------------------------
DISCOVERY MODE - DMV-based candidate selection
-------------------------------------------------------------------------------

-- Update NORECOMPUTE stats only
EXECUTE dbo.sp_StatUpdate
    @TargetNorecompute = N'Y',
    @ModificationThreshold = 10000,
    @TimeLimit = 1800;

-- Update all stale stats > 30 days
EXECUTE dbo.sp_StatUpdate
    @TargetNorecompute = N'BOTH',
    @DaysStaleThreshold = 30,
    @TimeLimit = 7200;

-- Large tables only (1GB+)
EXECUTE dbo.sp_StatUpdate
    @MinPageCount = 125000,
    @TimeLimit = 14400;

-------------------------------------------------------------------------------
WITH RESAMPLE (preserves sample rate, required for incremental stats)
-------------------------------------------------------------------------------

EXECUTE dbo.sp_StatUpdate
    @StatisticsResample = 1,
    @TimeLimit = 7200;

-------------------------------------------------------------------------------
DRY RUN
-------------------------------------------------------------------------------

EXECUTE dbo.sp_StatUpdate
    @TargetNorecompute = N'BOTH',
    @Execute = N'N',
    @Debug = 1;

===============================================================================
*/
