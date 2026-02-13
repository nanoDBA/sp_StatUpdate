# sp_StatUpdate

**Priority-based statistics maintenance for SQL Server 2016+**

*Updates worst stats first. Stops when you tell it to. Tells you if it got killed.*

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![SQL Server 2016+](https://img.shields.io/badge/SQL%20Server-2016%2B-blue.svg)](https://www.microsoft.com/sql-server)

## Why This Exists

| Problem | Fix |
|---------|-----|
| Alphabetical ordering | `@SortOrder = 'MODIFICATION_COUNTER'` - worst first |
| 10-hour jobs killed at 5 AM | `@TimeLimit` - stops gracefully, logs what's left |
| "Did it finish or get killed?" | START/END markers in CommandLog |
| NORECOMPUTE orphans | `@TargetNorecompute = 'Y'` - finds and refreshes them |
| Large stats that never finish | `@LongRunningThresholdMinutes` - auto-reduce sample rate |
| Query Store knows what's hot | `@QueryStorePriority = 'Y'` - prioritize by CPU/reads |

## Quick Start

```sql
-- 1. Install prerequisites (Ola Hallengren's CommandLog table)
-- Download from: https://ola.hallengren.com/scripts/CommandLog.sql

-- 2. Install sp_StatUpdate
-- Run sp_StatUpdate.sql in your maintenance database

-- 3. Run statistics maintenance
EXEC dbo.sp_StatUpdate
    @Databases = N'YourDatabase',
    @TargetNorecompute = N'BOTH',      -- All stats (NORECOMPUTE + regular)
    @TimeLimit = 3600;                  -- 1 hour limit
    -- Defaults: @TieredThresholds=1, @ModificationThreshold=5000, @LogToTable='Y'
```

## Requirements

**DROP-IN COMPATIBLE** with [Ola Hallengren's SQL Server Maintenance Solution](https://ola.hallengren.com).

### SQL Server Version

- **Minimum**: SQL Server 2016 (uses `STRING_SPLIT`)
- **Recommended**: SQL Server 2016 SP2+ (MAXDOP for UPDATE STATISTICS)

### Required Dependencies

| Object | URL | Notes |
|--------|-----|-------|
| `dbo.CommandLog` | [CommandLog.sql](https://ola.hallengren.com/scripts/CommandLog.sql) | Or set `@LogToTable = 'N'` |

### Optional Dependencies (for `@StatsInParallel = 'Y'`)

| Object | Source |
|--------|--------|
| `dbo.Queue` | [Queue.sql](https://ola.hallengren.com/scripts/Queue.sql) (Ola Hallengren) |

**Note**: `dbo.QueueStatistic` is auto-created on first parallel run if `dbo.Queue` exists.

**Note**: `dbo.CommandExecute` is NOT required. sp_StatUpdate handles its own command execution.

## Real-World Scenarios

### Quick Start with Presets (v1.9+)

Don't want to figure out all the parameters? Use a preset:

```sql
-- Nightly maintenance (1hr, tiered thresholds, balanced)
EXEC dbo.sp_StatUpdate @Preset = 'NIGHTLY_MAINTENANCE', @Databases = 'USER_DATABASES';

-- Weekly comprehensive (4hr, lower thresholds)
EXEC dbo.sp_StatUpdate @Preset = 'WEEKLY_FULL', @Databases = 'USER_DATABASES';

-- OLTP with minimal impact (30min, high thresholds, delays)
EXEC dbo.sp_StatUpdate @Preset = 'OLTP_LIGHT', @Databases = 'MyOLTPDatabase';

-- Data warehouse full refresh (no limit, FULLSCAN)
EXEC dbo.sp_StatUpdate @Preset = 'WAREHOUSE_AGGRESSIVE', @Databases = 'MyDW';
```

### "Our maintenance job keeps getting killed at 5 AM"

The job runs alphabetically, never reaches the important tables, and gets killed when the business day starts. You need worst-first ordering with a hard stop time.

```sql
-- Nightly job: 11 PM - 4 AM window (5 hours)
EXEC dbo.sp_StatUpdate
    @Databases = N'USER_DATABASES, -DevDB, -ReportingDB',
    @TimeLimit = 18000,
    @SortOrder = N'MODIFICATION_COUNTER';  -- Worst stats first

-- Check next morning: did it finish or get killed?
SELECT CommandType, StartTime,
    ExtendedInfo.value('(/Summary/StopReason)[1]', 'nvarchar(50)') AS StopReason
FROM dbo.CommandLog
WHERE CommandType LIKE 'SP_STATUPDATE%'
ORDER BY StartTime DESC;
```

### "Someone set NORECOMPUTE in 2019 and forgot"

NORECOMPUTE stats never auto-update. SQL Server ignores them. Meanwhile, the data has changed 500 million times.

```sql
-- Find and refresh the forgotten NORECOMPUTE stats
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @TargetNorecompute = N'Y',            -- Only NORECOMPUTE stats
    @ModificationThreshold = 50000,
    @TimeLimit = 1800;
```

### "The Orders table gets 10M inserts/day"

High-churn tables with ascending keys need attention, but UPDATE STATISTICS takes locks. Run during low-activity windows, not business hours.

```sql
-- Midday lull or after-hours for high-churn tables
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @Tables = N'Sales.Orders, Sales.OrderDetails',
    @ModificationThreshold = 100000,
    @LockTimeout = 5,                     -- Skip if blocked after 5 sec
    @TimeLimit = 600;
```

### "Query Store shows one stat is causing 40% of our CPU"

Use Query Store metrics to prioritize stats that are actually hurting performance.

```sql
-- Let Query Store tell you what matters
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @QueryStorePriority = N'Y',
    @QueryStoreMetric = N'CPU',           -- Or DURATION, READS
    @QueryStoreRecentHours = 24,          -- Last 24 hours
    @SortOrder = N'QUERY_STORE',
    @TimeLimit = 3600;
```

### "The 100% FULLSCAN stats never finish"

Some stats were configured with 100% sample years ago. They take 6 hours each and blow your maintenance window. Use adaptive sampling to auto-reduce them.

```sql
-- Stats that historically took >2 hours get 5% sample
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @LongRunningThresholdMinutes = 120,
    @LongRunningSamplePercent = 5,
    @TimeLimit = 14400;
```

### "Don't waste time on Archive tables"

Skip tables you don't care about. Focus on what matters.

```sql
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @ExcludeTables = N'%Archive%, %History%',
    @ExcludeStatistics = N'_WA_Sys%',
    @TimeLimit = 7200;
```

### "Preview before committing"

See exactly what commands would run without executing anything.

```sql
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @Execute = N'N',
    @WhatIfOutputTable = N'#Preview',
    @Debug = 1;

-- Review the commands
SELECT DatabaseName, SchemaName, TableName, StatName, Command
FROM #Preview
ORDER BY SequenceNum;
```

## Parameter Reference

### Database Selection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@Databases` | Current DB | Keywords: `USER_DATABASES`, `SYSTEM_DATABASES`, `ALL_DATABASES`, `AVAILABILITY_GROUP_DATABASES`. Supports wildcards (`%Prod%`), exclusions (`-DevDB`), comma-separated lists |
| `@Tables` | All | Table filter (comma-separated `Schema.Table`) |
| `@ExcludeTables` | `NULL` | Exclude tables by pattern (`dbo.Archive%`) |
| `@ExcludeStatistics` | `NULL` | Exclude stats by pattern (`_WA_Sys%`) |
| `@IncludeSystemObjects` | `'N'` | Include stats on system objects |

### Threshold Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@TargetNorecompute` | `'BOTH'` | `'Y'`=NORECOMPUTE only, `'N'`=regular only, `'BOTH'`=all |
| `@ModificationThreshold` | `5000` | Minimum modifications to qualify |
| `@ModificationPercent` | `NULL` | SQRT-based threshold (scales with table size) |
| `@TieredThresholds` | `1` | Use Tiger Toolbox 5-tier adaptive formula |
| `@ThresholdLogic` | `'OR'` | `'OR'`=any threshold qualifies, `'AND'`=all must be met |
| `@DaysStaleThreshold` | `NULL` | Minimum days since last update |
| `@MinPageCount` | `0` | Minimum pages (125000 = ~1GB) |

### Query Store Integration (v1.4+)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@QueryStorePriority` | `'N'` | Prioritize stats used by Query Store plans |
| `@QueryStoreMetric` | `'CPU'` | `CPU`, `DURATION`, `READS`, or `EXECUTIONS` |
| `@QueryStoreMinExecutions` | `100` | Minimum plan executions to boost priority |
| `@QueryStoreRecentHours` | `168` | Only consider plans from last N hours (default: 7 days) |

### Filtered Stats (v1.4+)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@FilteredStatsMode` | `'INCLUDE'` | `INCLUDE`, `EXCLUDE`, `ONLY`, or `PRIORITY` |
| `@FilteredStatsStaleFactor` | `2.0` | Filtered stats stale when unfiltered_rows/rows > factor |

### Adaptive Sampling (v1.6+)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@LongRunningThresholdMinutes` | `NULL` | Stats that took longer get forced sample rate |
| `@LongRunningSamplePercent` | `10` | Sample percent for long-running stats |

### Presets (v1.9+)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@Preset` | `NULL` | Common configurations: `NIGHTLY_MAINTENANCE`, `WEEKLY_FULL`, `OLTP_LIGHT`, `WAREHOUSE_AGGRESSIVE` |

### Join Pattern Grouping (v1.9+)

Re-sorts processing order so commonly-joined tables (detected via Query Store) update consecutively. Without this, a time-limited run might update Table A but leave its join partner Table B stale — the optimizer sees mismatched cardinality estimates and picks bad join orders. Falls back silently if QS disabled. **Not compatible with `@StatsInParallel`.**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@GroupByJoinPattern` | `'Y'` | Re-order stats so joined tables update together. `'N'` = priority only |
| `@JoinPatternMinExecutions` | `100` | Minimum plan executions to detect join patterns |

### Execution Control

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@TimeLimit` | `3600` | Max seconds (1 hour) |
| `@BatchLimit` | `NULL` | Max stats per run |
| `@LockTimeout` | `NULL` | Seconds to wait for locks per stat |
| `@DelayBetweenStats` | `NULL` | Milliseconds to pause between stats |
| `@SortOrder` | `'MODIFICATION_COUNTER'` | Priority order (see below) |
| `@Execute` | `'Y'` | `'N'` for dry run |
| `@FailFast` | `0` | `1` = abort on first error |

### Sort Orders

| Value | Description |
|-------|-------------|
| `MODIFICATION_COUNTER` | Most modifications first (default) |
| `DAYS_STALE` | Oldest stats first |
| `PAGE_COUNT` | Largest tables first |
| `QUERY_STORE` | Highest Query Store metric first |
| `FILTERED_DRIFT` | Filtered stats with drift first |
| `AUTO_CREATED` | User-created stats before auto-created |
| `RANDOM` | Random order |

### Sampling Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@StatisticsSample` | `NULL` | `NULL`=SQL Server decides, `100`=FULLSCAN |
| `@UpdateIncremental` | `1` | Update incremental stats by partition |
| `@PersistSamplePercent` | `'N'` | Add PERSIST_SAMPLE_PERCENT (SQL 2016 SP1 CU4+) |

### Logging & Output

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@LogToTable` | `'Y'` | Log to dbo.CommandLog |
| `@WhatIfOutputTable` | `NULL` | Table for dry-run commands |
| `@ProgressLogInterval` | `NULL` | Log progress every N stats (secure - uses CommandLog) |
| `@ExposeProgressToAllSessions` | `'N'` | `'Y'` = create ##sp_StatUpdate_Progress (**SECURITY:** visible to all sessions) |
| `@Debug` | `0` | `1` = verbose diagnostic output |

### Parallel Mode

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@StatsInParallel` | `'N'` | Use queue-based parallel processing |
| `@DeadWorkerTimeoutMinutes` | `15` | Consider worker dead if no progress |

### Maintenance

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@CleanupOrphanedRuns` | `'Y'` | Mark killed runs (>24h old) with END markers |
| `@Help` | `0` | `1` = show parameter documentation |

Run `EXEC sp_StatUpdate @Help = 1` for complete parameter documentation.

## Monitoring

### Real-Time Progress (v1.9+)

**Opt-in due to security:** Global temp tables are visible to ALL sessions on the server.

```sql
-- Enable progress monitoring (exposes database/table names to all sessions)
EXEC sp_StatUpdate
    @Databases = 'USER_DATABASES',
    @ExposeProgressToAllSessions = 'Y';

-- Query from another session while running:
SELECT * FROM ##sp_StatUpdate_Progress;
-- Returns: RunLabel, StatsFound, StatsProcessed, StatsSucceeded, StatsFailed,
--          CurrentDatabase, CurrentTable, ElapsedSeconds, Status
```

**Secure alternative:** Use `@ProgressLogInterval` which writes to CommandLog (access-controlled):

```sql
EXEC sp_StatUpdate @Databases = 'USER_DATABASES', @ProgressLogInterval = 50;
```

### Run History

```sql
-- Recent runs: did they finish or get killed?
SELECT
    CASE WHEN e.ID IS NOT NULL THEN 'Completed' ELSE 'KILLED' END AS Status,
    s.StartTime,
    e.ExtendedInfo.value('(/Summary/StopReason)[1]', 'nvarchar(50)') AS StopReason,
    e.ExtendedInfo.value('(/Summary/StatsProcessed)[1]', 'int') AS Processed,
    e.ExtendedInfo.value('(/Summary/StatsRemaining)[1]', 'int') AS Remaining
FROM dbo.CommandLog s
LEFT JOIN dbo.CommandLog e
    ON e.CommandType = 'SP_STATUPDATE_END'
    AND e.ExtendedInfo.value('(/Summary/RunLabel)[1]', 'nvarchar(100)') =
        s.ExtendedInfo.value('(/Parameters/RunLabel)[1]', 'nvarchar(100)')
WHERE s.CommandType = 'SP_STATUPDATE_START'
ORDER BY s.StartTime DESC;
```

### Why Did This Stat Use That Sample Rate?

Three systems can influence sample rate: explicit `@StatisticsSample`, adaptive sampling (history-based), and SQL Server auto-sample. The `SampleSource` field in ExtendedInfo tells you exactly which one was used:

```sql
-- Find sample rate decisions for a specific stat
SELECT
    StatisticsName,
    StartTime,
    ExtendedInfo.value('(/ExtendedInfo/RequestedSamplePct)[1]', 'int') AS RequestedPct,
    ExtendedInfo.value('(/ExtendedInfo/EffectiveSamplePct)[1]', 'int') AS EffectivePct,
    ExtendedInfo.value('(/ExtendedInfo/SampleSource)[1]', 'nvarchar(20)') AS SampleSource
FROM dbo.CommandLog
WHERE CommandType = 'UPDATE_STATISTICS'
AND StatisticsName = 'YourStatName'
ORDER BY StartTime DESC;

-- SampleSource values:
--   EXPLICIT        = User passed @StatisticsSample
--   ADAPTIVE        = History showed this stat was slow, overridden
--   ADAPTIVE_CAPPED = Adaptive but capped at ~10M rows
--   AUTO            = SQL Server decided (NULL passed)
--   RESAMPLE_PERSIST= Respecting PERSIST_SAMPLE_PERCENT setting
--   RESAMPLE_INCR   = Incremental stats require RESAMPLE
--   FULLSCAN_MEMOPT = Memory-optimized table on SQL 2014
```

### Heap Health Monitoring (v2.0+)

When `@CollectHeapForwarding = 'Y'`, CommandLog captures forwarding pointer counts for heap tables. High forwarding ratios indicate fragmentation that degrades scan performance.

```sql
-- Heap Statistics Health Report - Find heaps with forwarding pointer problems
-- Shows which heap statistics have the most forwarding pointers and may need rebuild
SELECT TOP 20
    DatabaseName,
    SchemaName,
    ObjectName,
    StatisticsName,
    StartTime,
    -- Heap health metrics
    ExtendedInfo.value('(/ExtendedInfo/ForwardedRecords)[1]', 'bigint') AS ForwardedRecords,
    ExtendedInfo.value('(/ExtendedInfo/RowCount)[1]', 'bigint') AS [RowCount],
    -- Calculate forwarding ratio (high % = fragmented heap)
    CAST(
        ExtendedInfo.value('(/ExtendedInfo/ForwardedRecords)[1]', 'bigint') * 100.0 /
        NULLIF(ExtendedInfo.value('(/ExtendedInfo/RowCount)[1]', 'bigint'), 0)
        AS decimal(5,2)
    ) AS ForwardingPct,
    ExtendedInfo.value('(/ExtendedInfo/SizeMB)[1]', 'int') AS SizeMB,
    -- Modification activity
    ExtendedInfo.value('(/ExtendedInfo/ModificationCounter)[1]', 'bigint') AS Modifications,
    ExtendedInfo.value('(/ExtendedInfo/ModificationPct)[1]', 'decimal(18,2)') AS ModificationPct,
    -- When last updated
    DATEDIFF(day, StartTime, GETDATE()) AS DaysAgo,
    -- Actionable recommendation
    CASE
        WHEN ExtendedInfo.value('(/ExtendedInfo/ForwardedRecords)[1]', 'bigint') * 100.0 /
             NULLIF(ExtendedInfo.value('(/ExtendedInfo/RowCount)[1]', 'bigint'), 0) > 20
        THEN 'CRITICAL: Rebuild heap immediately'
        WHEN ExtendedInfo.value('(/ExtendedInfo/ForwardedRecords)[1]', 'bigint') * 100.0 /
             NULLIF(ExtendedInfo.value('(/ExtendedInfo/RowCount)[1]', 'bigint'), 0) > 10
        THEN 'WARNING: Consider rebuild'
        ELSE 'OK'
    END AS Recommendation
FROM dbo.CommandLog
WHERE CommandType = 'UPDATE_STATISTICS'
AND ExtendedInfo.value('(/ExtendedInfo/IsHeap)[1]', 'bit') = 1
AND ExtendedInfo.value('(/ExtendedInfo/ForwardedRecords)[1]', 'bigint') > 0
ORDER BY
    -- Sort by worst forwarding ratio first
    ExtendedInfo.value('(/ExtendedInfo/ForwardedRecords)[1]', 'bigint') * 100.0 /
    NULLIF(ExtendedInfo.value('(/ExtendedInfo/RowCount)[1]', 'bigint'), 0) DESC;
```

**Example output:**

| DatabaseName | ObjectName   | ForwardedRecords | RowCount | ForwardingPct | SizeMB | Recommendation                       |
|--------------|--------------|------------------|----------|---------------|--------|--------------------------------------|
| OrdersDB     | AuditLog     | 45230            | 180000   | 25.13         | 1200   | CRITICAL: Rebuild heap immediately   |
| CustomerDB   | EventHistory | 12400            | 95000    | 13.05         | 450    | WARNING: Consider rebuild            |

**Forwarding ratio thresholds:**

- **>20%**: Critical fragmentation - rebuild immediately with `ALTER TABLE ... REBUILD`
- **>10%**: Warning level - schedule rebuild during next maintenance window
- **<10%**: Acceptable level - monitor only

## Diagnostic Tool (v1.0)

**sp_StatUpdate_Diag** analyzes your CommandLog history and produces actionable recommendations — killed run detection, repeated failures, degrading throughput, suboptimal parameters, and more.

### Single Server

```sql
-- Install: run sp_StatUpdate_Diag.sql on your maintenance database

-- Basic diagnostic (last 30 days)
EXEC dbo.sp_StatUpdate_Diag;

-- Obfuscated for sharing with support (hashes server/database/table names)
EXEC dbo.sp_StatUpdate_Diag @Obfuscate = 1;

-- Debug mode with custom window
EXEC dbo.sp_StatUpdate_Diag @DaysBack = 90, @Debug = 1;
```

Returns 8 result sets: Recommendations (severity-categorized), Run Health Summary, Run Detail, Top Tables by Maintenance Cost, Failing Statistics, Long-Running Statistics, Parameter Change History, and Obfuscation Map (when `@Obfuscate = 1`).

### Multi-Server (PowerShell)

```powershell
# Requires: PowerShell 7+, SqlServer module
# Install sp_StatUpdate_Diag on each server first

.\Invoke-StatUpdateDiag.ps1 `
    -Servers "Server1", "Server2,2500", "Server3" `
    -CommandLogDatabase "Maintenance" `
    -OutputPath ".\diag_output"

# Obfuscated report for external sharing
.\Invoke-StatUpdateDiag.ps1 `
    -Servers "Server1", "Server2" `
    -Obfuscate `
    -OutputPath ".\diag_output"
```

Generates a Markdown report with executive summary, per-server findings, and cross-server analysis (version skew, parameter inconsistency).

### Diagnostic Checks

| Severity | Checks |
|----------|--------|
| CRITICAL | Killed runs, repeated stat failures, time limit exhaustion, degrading throughput |
| WARNING | Suboptimal parameters, long-running stats, stale-stats backlog, overlapping runs, ineffective Query Store |
| INFO | Run health trends, parameter history, top tables by cost, unused features, version history |

### Diagnostic Scenarios

#### "Our nightly job didn't finish — was it killed?"

Killed runs leave orphaned START markers with no END. The diagnostic tool detects these automatically.

```sql
-- Look for killed runs in the last 7 days
EXEC dbo.sp_StatUpdate_Diag @DaysBack = 7;
-- Check CRITICAL findings for "Killed runs detected"
-- Evidence shows which RunLabels have START without END
```

#### "The same stat keeps failing every night"

Repeated failures on the same statistic often indicate corruption, permission issues, or persistent lock contention. The tool flags stats failing 3+ times as CRITICAL.

```sql
-- Lower the failure threshold to catch stats failing just twice
EXEC dbo.sp_StatUpdate_Diag
    @DaysBack = 14,
    @FailureThreshold = 2;
-- Result set 5 (Failing Statistics) groups by stat + error message
```

#### "Runs are getting slower every week"

Throughput degradation can indicate table growth, I/O pressure, or plan regression. The tool compares recent throughput against a prior baseline window.

```sql
-- Compare last 7 days vs prior 7 days (default)
EXEC dbo.sp_StatUpdate_Diag @DaysBack = 30;

-- Use a wider comparison window for seasonal patterns
EXEC dbo.sp_StatUpdate_Diag
    @DaysBack = 90,
    @ThroughputWindowDays = 14;
-- CRITICAL finding if recent avg sec/stat is >50% worse
```

#### "Which tables eat the most maintenance time?"

The "Top Tables by Maintenance Cost" result set ranks tables by cumulative UPDATE STATISTICS duration.

```sql
-- Show top 50 most expensive tables over 90 days
EXEC dbo.sp_StatUpdate_Diag
    @DaysBack = 90,
    @TopN = 50;
-- Result set 4: total duration, avg duration, update count per table
```

#### "Share diagnostics with Microsoft support (obfuscated)"

Obfuscation mode hashes all server, database, schema, table, and statistic names with MD5. Safe to share externally — the obfuscation map (result set 8) stays on your server.

```sql
EXEC dbo.sp_StatUpdate_Diag
    @DaysBack = 30,
    @Obfuscate = 1;
-- All result sets use hashed names (e.g., "DB_7F3A2B..." instead of "Production")
-- Result set 8 shows the mapping (keep this private!)
```

#### "Compare diagnostics across 5 production servers"

The PowerShell wrapper runs sp_StatUpdate_Diag in parallel across servers, merges results, and detects cross-server issues like version skew and parameter inconsistency.

```powershell
# Compare all production servers — parallel execution, merged report
.\Invoke-StatUpdateDiag.ps1 `
    -Servers "SQL-PROD1", "SQL-PROD2,2500", "SQL-PROD3" `
    -CommandLogDatabase "DBAMaintenance" `
    -DaysBack 30 `
    -OutputFormat "Markdown" `
    -OutputPath ".\diag_reports"

# Obfuscated version for vendor support ticket
.\Invoke-StatUpdateDiag.ps1 `
    -Servers "SQL-PROD1", "SQL-PROD2,2500" `
    -Obfuscate `
    -OutputPath ".\diag_reports"
```

## Troubleshooting

### Diagnostic Report

The fastest way to troubleshoot sp_StatUpdate issues is the diagnostic tool:

```sql
EXEC dbo.sp_StatUpdate_Diag @Debug = 1;
```

See [Diagnostic Tool](#diagnostic-tool-v10) above for details.

### Extended Events Session

An Extended Events session is included for runtime troubleshooting:

```sql
-- Create the XE session (see sp_StatUpdate_XE_Session.sql)
-- Start monitoring before running sp_StatUpdate
ALTER EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER STATE = START;

-- Run sp_StatUpdate...

-- View captured events (queries included in the XE script)
```

The session captures UPDATE STATISTICS commands (both starting and completed events for duration correlation), errors, lock waits, lock escalation, and long-running statements.

## When to Use This (vs IndexOptimize)

**IndexOptimize** is battle-tested and handles indexes + stats together. Use it for general maintenance.

**sp_StatUpdate** is for when you need:
- Priority ordering (worst stats first)
- Time-limited runs with graceful stops
- NORECOMPUTE targeting
- Query Store-driven prioritization
- Adaptive sampling for problematic stats

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Test changes with `@Execute = 'N'` dry runs
4. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) for details.

Based on patterns from [Ola Hallengren's SQL Server Maintenance Solution](https://ola.hallengren.com) (MIT License).

## Version History

- **sp_StatUpdate_Diag 1.0** - Diagnostic & recommendation engine. T-SQL procedure (`sp_StatUpdate_Diag`) + PowerShell multi-server wrapper (`Invoke-StatUpdateDiag.ps1`). Analyzes CommandLog for killed runs, repeated failures, degrading throughput, overlapping runs, suboptimal parameters. Obfuscation mode for external sharing. 53-test automated suite.
- **2.0.2026.0212** - Phase 1: Environment Intelligence (CE version/compat level, trace flag detection, DB-scoped config detection in debug output). Phase 2: Staged discovery auto-fallback to legacy mode on unexpected row loss (TRY/CATCH with signal table). Truncated partitions no longer force full RESAMPLE — only stale partitions updated via ON PARTITIONS(). XE session overhaul: added starting events for during-execution visibility, fixed wait_type predicates to use numeric map_key values, added lock_escalation and attention events, 8MB ring buffer.
- **1.9.2026.0206** - Status and StatusMessage columns in summary result set (SUCCESS/WARNING/ERROR for Agent job alerting). Batch Query Store enrichment (O(n) to O(1) via CTE + GROUP BY). Early-return path now returns summary result set for INSERT...EXEC consumers.
- **1.9.2026.0128** - New @Preset parameter (NIGHTLY_MAINTENANCE, WEEKLY_FULL, OLTP_LIGHT, WAREHOUSE_AGGRESSIVE), @GroupByJoinPattern (update joined tables together), ##sp_StatUpdate_Progress global temp table (opt-in via @ExposeProgressToAllSessions for security), @CleanupOrphanedRuns default Y, LOCK_TIMEOUT restores original value, XML entity decoding complete, READPAST hint for parallel mode, @WhatIfOutputTable data type validation, Query Store READ_ONLY warning, debug threshold explanation
- **1.8.2026.0128** - Code review fixes (P1/P2): @LongRunningSamplePercent row cap, staged discovery phase validation, FOR XML entity decoding, LOCK_TIMEOUT reset, incremental partition cross-ref, @WhatIfOutputTable schema validation. Added XE troubleshooting session.
- **1.7.2026.0127** - BREAKING: @ModificationThreshold default 1000 → 5000 (less aggressive)
- **1.6.2026.0128** - Staged discovery (6-phase for 10K+ stats), adaptive sampling (@LongRunningThresholdMinutes), @ExcludeTables, @WhatIfOutputTable, @CleanupOrphanedRuns, @CollectHeapForwarding, AUTO_CREATED sort order
- **1.5.2026.0120** - CRITICAL: Fixed @ExcludeStatistics filter, incremental partition targeting
- **1.4.2026.0119** - Query Store prioritization (@QueryStorePriority, @QueryStoreMetric), filtered stats handling (@FilteredStatsMode), FILTERED_DRIFT sort order
- **1.3.2026.0119** - Multi-database support (USER_DATABASES, wildcards, exclusions), @IncludeSystemObjects, AG secondary handling, OUTPUT parameters, return codes
- **1.2.2026.0117** - Tiger Toolbox tiered thresholds, AND/OR threshold logic, PERSIST_SAMPLE_PERCENT, MAXDOP
- **1.1.2026.0117** - Erik Darling style refactor, @Help, incremental stats, AG awareness, parallel mode
- **1.0.2026.0117** - Initial public release

## Acknowledgments

- [Ola Hallengren](https://ola.hallengren.com) - sp_StatUpdate wouldn't exist without his SQL Server Maintenance Solution. We use his CommandLog table, Queue patterns, and database selection syntax. If you're not already using his tools, start there.
- [Brent Ozar](https://www.brentozar.com) - years of emphasizing stats over index rebuilds, First Responder Kit, and community education.
- [Erik Darling](https://www.erikdarling.com) - T-SQL coding style and performance insights. sp_LogHunter and sp_QuickieStore are excellent.
- [Tiger Team's AdaptiveIndexDefrag](https://github.com/microsoft/tigertoolbox) - the 5-tier adaptive threshold formula
- [Colleen Morrow](https://www.sqlservercentral.com/blogs/better-living-thru-powershell-update-statistics-in-parallel) - parallel statistics maintenance concept
