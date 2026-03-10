# sp_StatUpdate

**Priority-based statistics maintenance for SQL Server 2016+**

*Updates worst stats first. Stops when you tell it to. Tells you if it got killed.*

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![SQL Server 2016+](https://img.shields.io/badge/SQL%20Server-2016%2B-blue.svg)](https://www.microsoft.com/sql-server)
[![Azure SQL](https://img.shields.io/badge/Azure%20SQL-Supported-0078D4.svg)](https://azure.microsoft.com/products/azure-sql)

## Why This Exists

| Problem | Fix |
|---------|-----|
| Alphabetical ordering | `@SortOrder = 'MODIFICATION_COUNTER'` - worst first |
| 10-hour jobs killed at 5 AM | `@TimeLimit` - stops gracefully, logs what's left |
| "Did it finish or get killed?" | START/END markers in CommandLog |
| NORECOMPUTE orphans | `@TargetNorecompute = 'Y'` - finds and refreshes them |
| Large stats that never finish | `@LongRunningThresholdMinutes` - auto-reduce sample rate |
| Query Store knows what's hot | `@QueryStorePriority = 'Y'` - prioritize by CPU/reads |
| Cascading failures | `@MaxConsecutiveFailures` - stops after N failures |
| AG secondary falls behind | `@MaxAGRedoQueueMB` - pauses when redo queue is deep |
| tempdb pressure during FULLSCAN | `@MinTempdbFreeMB` - checks before each stat update |
| Azure DTU/vCore concerns | Auto-detects Azure SQL DB vs MI, platform-specific warnings |
| Indexed view stats ignored | `@IncludeIndexedViews = 'Y'` - discovers view statistics |
| No audit trail for skipped stats | `@LogSkippedToCommandLog = 'Y'` - TOCTOU skip logging |

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

| Requirement | Details |
|-------------|---------|
| **SQL Server** | 2016+ (uses `STRING_SPLIT`). 2016 SP2+ recommended for MAXDOP support |
| **Azure SQL** | Database (EngineEdition 5), Managed Instance (8), and Edge (9) supported |
| **dbo.CommandLog** | [CommandLog.sql](https://ola.hallengren.com/scripts/CommandLog.sql) or set `@LogToTable = 'N'` |
| **dbo.Queue** | [Queue.sql](https://ola.hallengren.com/scripts/Queue.sql) - only for `@StatsInParallel = 'Y'` |

**Note**: `dbo.QueueStatistic` is auto-created on first parallel run. `dbo.CommandExecute` is NOT required.

## Presets (Quick Configuration)

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

## Common Scenarios

### Time-Limited Nightly Runs

```sql
-- Nightly job: 11 PM - 4 AM window (5 hours)
EXEC dbo.sp_StatUpdate
    @Databases = N'USER_DATABASES, -DevDB, -ReportingDB',
    @TimeLimit = 18000,
    @SortOrder = N'MODIFICATION_COUNTER';  -- Worst stats first
```

### Query Store-Driven Prioritization

```sql
-- Let Query Store tell you what matters
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @QueryStorePriority = N'Y',
    @QueryStoreMetric = N'CPU',           -- Or DURATION, READS
    @SortOrder = N'QUERY_STORE',
    @TimeLimit = 3600;
```

### NORECOMPUTE Stats Refresh

```sql
-- Find and refresh forgotten NORECOMPUTE stats
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @TargetNorecompute = N'Y',
    @ModificationThreshold = 50000,
    @TimeLimit = 1800;
```

### AG-Safe Maintenance

```sql
-- Pause if any secondary falls behind by 500 MB redo
EXEC dbo.sp_StatUpdate
    @Databases = N'USER_DATABASES',
    @MaxAGRedoQueueMB = 500,
    @MaxAGWaitMinutes = 10,               -- Wait up to 10 min for drain
    @TimeLimit = 3600;
```

### Adaptive Sampling for Slow Stats

```sql
-- Stats that historically took >2 hours get 5% sample
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @LongRunningThresholdMinutes = 120,
    @LongRunningSamplePercent = 5,
    @TimeLimit = 14400;
```

### Dry Run Preview

```sql
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @Execute = N'N',
    @WhatIfOutputTable = N'#Preview',
    @Debug = 1;

SELECT * FROM #Preview ORDER BY SequenceNum;
```

### ETL Completion Notification

```sql
-- Downstream ETL waits for this table to have a row
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @TimeLimit = 3600,
    @CompletionNotifyTable = N'dbo.StatUpdateNotify';

-- ETL checks: SELECT * FROM dbo.StatUpdateNotify WHERE RunLabel = ...
```

## Parameter Reference

Run `EXEC sp_StatUpdate @Help = 1` for complete documentation including operational notes.

### Database & Table Selection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@Databases` | Current DB | `USER_DATABASES`, `SYSTEM_DATABASES`, `ALL_DATABASES`, `AVAILABILITY_GROUP_DATABASES`, wildcards (`%Prod%`), exclusions (`-DevDB`) |
| `@Tables` | All | Table filter (comma-separated `Schema.Table`) |
| `@ExcludeTables` | `NULL` | Exclude tables by LIKE pattern (`%Archive%`) |
| `@ExcludeStatistics` | `NULL` | Exclude stats by LIKE pattern (`_WA_Sys%`) |
| `@Statistics` | `NULL` | Direct stat references (`Schema.Table.Stat`, comma-separated) |
| `@StatisticsFromTable` | `NULL` | Table containing stat references (`#MyStats`, `dbo.StatsQueue`) |
| `@IncludeSystemObjects` | `'N'` | Include stats on system objects |
| `@IncludeIndexedViews` | `'N'` | Include statistics on indexed views (v2.8+) |

### Threshold Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@TargetNorecompute` | `'BOTH'` | `'Y'`=NORECOMPUTE only, `'N'`=regular only, `'BOTH'`=all |
| `@ModificationThreshold` | `5000` | Minimum modifications to qualify |
| `@ModificationPercent` | `NULL` | Alternative: min mod % of rows (SQRT-based) |
| `@TieredThresholds` | `1` | Use Tiger Toolbox 5-tier adaptive formula |
| `@ThresholdLogic` | `'OR'` | `'OR'`=any threshold, `'AND'`=all must be met |
| `@DaysStaleThreshold` | `NULL` | Minimum days since last update |
| `@HoursStaleThreshold` | `NULL` | Alternative: minimum hours since last update |
| `@MinPageCount` | `0` | Minimum pages (125 = ~1MB, 125000 = ~1GB) |

### Execution Control

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@TimeLimit` | `3600` | Max seconds (1 hour). `NULL` = unlimited |
| `@StopByTime` | `NULL` | Absolute wall-clock stop time (`'04:00'` = 4 AM) |
| `@BatchLimit` | `NULL` | Max stats per run |
| `@MaxConsecutiveFailures` | `NULL` | Stop after N consecutive failures |
| `@LockTimeout` | `NULL` | Seconds to wait for schema locks per stat |
| `@DelayBetweenStats` | `NULL` | Seconds to pause between stats |
| `@SortOrder` | `'MODIFICATION_COUNTER'` | Priority order (see below) |
| `@Execute` | `'Y'` | `'N'` for dry run |
| `@FailFast` | `0` | `1` = abort on first error |
| `@MaxGrantPercent` | `10` | Memory grant cap (%) on candidate discovery SELECT (1–100, `NULL` = disabled). Prevents discovery query from reserving excessive memory on busy servers. (v2.4+) |
| `@MaxSecondsPerStat` | `NULL` | Per-stat duration cap (seconds). Stats estimated to exceed the remaining `@StopByTime` budget are skipped rather than started. Prevents overshoot on tight maintenance windows. (v2.4+) |

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

### Safety Checks (v2.7+)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@MaxAGRedoQueueMB` | `NULL` | Pause when AG secondary redo queue exceeds this MB |
| `@MaxAGWaitMinutes` | `10` | Max minutes to wait for redo queue to drain |
| `@MinTempdbFreeMB` | `NULL` | Min tempdb free space (MB). `@FailFast=1` aborts, else warns |

### Query Store Integration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@QueryStorePriority` | `'N'` | Prioritize stats used by Query Store plans |
| `@QueryStoreMetric` | `'CPU'` | `CPU`, `DURATION`, `READS`, `EXECUTIONS`, or `AVG_CPU` |
| `@QueryStoreMinExecutions` | `100` | Minimum plan executions to boost |
| `@QueryStoreRecentHours` | `168` | Only consider plans from last N hours |
| `@GroupByJoinPattern` | `'Y'` | Update joined tables together (prevents optimization cliffs) |

### Adaptive Sampling

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@LongRunningThresholdMinutes` | `NULL` | Stats that took longer get forced sample rate |
| `@LongRunningSamplePercent` | `10` | Sample percent for long-running stats |
| `@StatisticsSample` | `NULL` | `NULL`=SQL Server decides, `100`=FULLSCAN |
| `@PersistSamplePercent` | `'Y'` | PERSIST_SAMPLE_PERCENT (SQL 2016 SP1 CU4+) |
| `@PersistSampleMinRows` | `1000000` | Minimum sampled rows required before the RESAMPLE_PERSIST path is used. When `rowcount * sample_pct / 100` is below this value, falls back to a full FULLSCAN pass to ensure histogram quality. Prevents persisting under-sampled stats on small tables. (v2.4+) |
| `@MaxDOP` | `NULL` | MAXDOP for UPDATE STATISTICS (SQL 2016 SP2+) |

### Logging & Output

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@LogToTable` | `'Y'` | Log to dbo.CommandLog |
| `@LogSkippedToCommandLog` | `'N'` | Log TOCTOU-skipped stats for audit trail (v2.8+) |
| `@ProgressLogInterval` | `NULL` | Log progress every N stats |
| `@ReturnDetailedResults` | `0` | `1` = return per-statistic detail result set (v2.8+) |
| `@CompletionNotifyTable` | `NULL` | Table for completion notification row (v2.8+) |
| `@WhatIfOutputTable` | `NULL` | Table for dry-run commands (`@Execute = 'N'` required) |
| `@Debug` | `0` | `1` = verbose diagnostic output |

### OUTPUT Parameters

| Parameter | Description |
|-----------|-------------|
| `@Version` | Procedure version string |
| `@VersionDate` | Procedure version date |
| `@StatsFoundOut` | Total qualifying stats discovered |
| `@StatsProcessedOut` | Stats attempted (succeeded + failed) |
| `@StatsSucceededOut` | Stats updated successfully |
| `@StatsFailedOut` | Stats that failed to update |
| `@StatsRemainingOut` | Stats not processed (time/batch limit) |
| `@DurationSecondsOut` | Total run duration in seconds |
| `@WarningsOut` | Collected warnings (see below) |
| `@StopReasonOut` | Why execution stopped (see below) |

### StopReason Values

`COMPLETED`, `TIME_LIMIT`, `BATCH_LIMIT`, `FAIL_FAST`, `CONSECUTIVE_FAILURES`, `AG_REDO_QUEUE`, `TEMPDB_PRESSURE`, `NO_QUALIFYING_STATS`, `KILLED`

### Warning Values

`LOW_UPTIME`, `BACKUP_RUNNING`, `AZURE_SQL`, `AZURE_MI`, `RESOURCE_GOVERNOR`, `AG_REDO_ELEVATED`, `TEMPDB_LOW`, `RLS_DETECTED`, `COLUMNSTORE_CONTEXT`, `QS_FORCED_PLANS`, `LOG_SPACE_HIGH`, `WIDE_STATS`, `FILTER_MISMATCH`

## Environment Detection (v2.0+)

Debug mode (`@Debug = 1`) automatically reports:

- **SQL Server version** and build number
- **Cardinality Estimator** version per database (Legacy CE 70 vs New CE 120+)
- **Trace flags** affecting statistics (2371, 9481, 2389/2390, 4139)
- **DB-scoped configs** (LEGACY_CARDINALITY_ESTIMATION)
- **Hardware context** (CPU count, memory, NUMA nodes, uptime)
- **Azure platform** (SQL DB vs Managed Instance vs Edge, with platform-specific guidance)
- **AG primary status** and redo queue depth (v2.7+)
- **tempdb free space** (v2.7+)
- **Resource Governor** active resource pools (v2.5+)

### Per-Database Detection (v2.8+)

When `@Debug = 1`, after discovery the proc checks each database for:

- **Row-Level Security** policies that may bias histograms
- **Wide statistics** (>8 columns) that increase tempdb/memory pressure
- **Filtered index mismatches** where stat filter differs from index filter
- **Columnstore indexes** where `modification_counter` underreports
- **Non-persisted computed columns** with evaluation cost during stat updates
- **Stretch Database** tables (auto-skipped, deprecated feature)
- **Query Store forced plans** on updated tables (post-update check, automatic)
- **Transaction log space** >90% full during FULLSCAN operations

## Monitoring

### Summary Result Set

Every run returns a summary row:

```text
Status         StatusMessage                                      StatsFound  ...
-------------- -------------------------------------------------- ----------  ---
SUCCESS        All 142 stat(s) updated successfully                142         ...
WARNING        Incomplete: 47 stat(s) remaining (TIME_LIMIT)       189        ...
ERROR          Failed: 3 stat(s), 47 remaining (FAIL_FAST)         150        ...
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

### Programmatic Access

```sql
DECLARE @Found int, @Processed int, @Failed int, @Remaining int,
        @StopReason nvarchar(50), @Warnings nvarchar(max);

EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @TimeLimit = 3600,
    @StatsFoundOut = @Found OUTPUT,
    @StatsProcessedOut = @Processed OUTPUT,
    @StatsFailedOut = @Failed OUTPUT,
    @StatsRemainingOut = @Remaining OUTPUT,
    @StopReasonOut = @StopReason OUTPUT,
    @WarningsOut = @Warnings OUTPUT;

-- Use outputs for alerting, logging, or conditional logic
IF @Failed > 0 OR @StopReason = 'CONSECUTIVE_FAILURES'
    RAISERROR(N'Alert: Statistics maintenance had failures', 10, 1) WITH NOWAIT;

IF @Warnings LIKE '%AG_REDO_ELEVATED%'
    RAISERROR(N'Note: AG redo queue was elevated during maintenance', 10, 1) WITH NOWAIT;
```

### Real-Time Progress

```sql
-- Secure option: log to CommandLog every 50 stats
EXEC sp_StatUpdate @Databases = 'USER_DATABASES', @ProgressLogInterval = 50;

-- Opt-in global temp table (visible to all sessions - security consideration)
EXEC sp_StatUpdate @Databases = 'USER_DATABASES', @ExposeProgressToAllSessions = 'Y';
-- Query from another session: SELECT * FROM ##sp_StatUpdate_Progress;
```

## Diagnostic Tool

**sp_StatUpdate_Diag** analyzes CommandLog history and produces actionable recommendations. Two viewing modes: management-friendly dashboard or full DBA deep-dive.

### Management View (Default)

```sql
-- Show your boss: letter grades + plain English recommendations
EXEC dbo.sp_StatUpdate_Diag;
```

Returns 2 result sets:

| RS | Name | What It Shows |
|----|------|---------------|
| 1 | **Executive Dashboard** | A-F letter grades for Overall, Completion, Reliability, Speed, and Workload Focus |
| 2 | **Recommendations** | Severity-categorized findings with fix-it SQL |

Example dashboard output:

```text
Category         Grade  Score  Headline
---------------- -----  -----  --------------------------------------------------------
OVERALL          B      75     Statistics maintenance is healthy with minor opportunities...
COMPLETION       A      92     Nearly all qualifying statistics are being updated each run.
RELIABILITY      C      55     2 run(s) were killed before completing. Check SQL Agent...
SPEED            A      90     Statistics are being updated very quickly (0.4 sec/stat).
WORKLOAD FOCUS   B      78     Query Store prioritization is working well.
```

### DBA Deep-Dive

```sql
-- Full technical detail: 13 result sets
EXEC dbo.sp_StatUpdate_Diag @ExpertMode = 1;
```

| RS | Name | Description |
|----|------|-------------|
| 1 | Executive Dashboard | Letter grades (always returned) |
| 2 | Recommendations | Findings with remediation SQL (always returned) |
| 3 | Run Health Summary | Aggregate metrics: total runs, killed, completion %, QS coverage |
| 4 | Run Detail | Per-run: duration, stats found/processed, stop reason, efficacy |
| 5 | Top Tables | Tables consuming the most maintenance time |
| 6 | Failing Statistics | Stats with repeated errors |
| 7 | Long-Running Statistics | Stats exceeding the duration threshold |
| 8 | Parameter Change History | How parameters changed across runs |
| 9 | Obfuscation Map | Only when `@Obfuscate = 1` |
| 10 | Efficacy Trend (Weekly) | Week-over-week QS prioritization metrics |
| 11 | Efficacy Detail (Per-Run) | Run-over-run with delta vs prior |
| 12 | High-CPU Stat Positions | Top-workload stats from most recent run |
| 13 | QS Performance Correlation | Per-stat CPU trend: are queries getting faster after updates? |

### Proving QS Prioritization Value

After switching from modification-counter to Query Store CPU-based sort order, show leadership the impact:

```sql
-- Broad trending: last 100 days, close-up on last 14
EXEC dbo.sp_StatUpdate_Diag
    @ExpertMode = 1,
    @EfficacyDaysBack = 100,
    @EfficacyDetailDays = 14;
```

Key result sets for this story:

- **RS 10 (Efficacy Trend)**: Weekly roll-up showing high-CPU stats reaching first quartile, workload coverage %, trend direction
- **RS 11 (Efficacy Detail)**: Per-run showing completion %, time-to-critical stats, delta vs prior run
- **RS 13 (QS Performance Correlation)**: Per-stat CPU before vs after — "5 of 5 tracked stats show 8% lower query CPU"
- **I7 check**: Automatically detects the configuration change point and compares before/after metrics
- **I8 check**: Summarizes whether queries are actually faster after stat updates

### Persistent History

The diagnostic tool auto-creates `dbo.StatUpdateDiagHistory` to track health scores over time. Each run inserts only new data (watermark-based, no duplicates).

```sql
-- View health score trend
SELECT CapturedAt, RunLabel, HealthScore, OverallGrade, CompletionPct, WorkloadCoveragePct
FROM dbo.StatUpdateDiagHistory
ORDER BY CapturedAt DESC;

-- Skip history creation (testing or ephemeral environments)
EXEC dbo.sp_StatUpdate_Diag @SkipHistory = 1;
```

### Obfuscated Mode

Hash all names for safe external sharing. Prefixes (`IX_`, `PK_`, `DB_`) are preserved for context.

```sql
-- Obfuscated for sharing with vendors or support
EXEC dbo.sp_StatUpdate_Diag @Obfuscate = 1, @ExpertMode = 1;

-- Deterministic hashing with a seed (reproducible across runs)
EXEC dbo.sp_StatUpdate_Diag @Obfuscate = 1, @ObfuscationSeed = N'my-secret-seed';

-- Persist the mapping table for later decoding
EXEC dbo.sp_StatUpdate_Diag @Obfuscate = 1, @ObfuscationMapTable = N'dbo.DiagObfMap';
```

#### Output Files

When running the PowerShell wrapper with `-Obfuscate`, three files are produced:

| File | Contains | Share externally? |
|------|----------|-------------------|
| `*_SAFE_TO_SHARE.{md,html,json}` | Obfuscated names only | Yes |
| `*_CONFIDENTIAL.{md,html,json}` | Real server/database/table names | **No** |
| `*_CONFIDENTIAL_DECODE.sql` | T-SQL script mapping obfuscated tokens to real names | **No** |

```powershell
.\Invoke-StatUpdateDiag.ps1 `
    -Servers (Get-Content servers.txt) `
    -Obfuscate `
    -ObfuscationSeed "my-secret-seed" `
    -OutputFormat JSON `
    -OutputPath "C:\temp\diag_report"
```

Without `-Obfuscate`, a single report file is produced as before (no suffix).

### Decoding Obfuscated Results

When a consultant returns findings referencing obfuscated tokens like `TBL_a1b2c3`, you have two options:

**Option A: Use the decode SQL file (no server access needed)**

Open the `_CONFIDENTIAL_DECODE.sql` file generated alongside the report. It contains the full obfuscation map in a temp table with ready-to-run queries:

```sql
-- 1. Run the script in SSMS (creates #ObfuscationMap)
-- 2. Decode a specific token:
SELECT * FROM #ObfuscationMap WHERE ObfuscatedName = N'TBL_a1b2c3';
-- 3. See full map:
SELECT * FROM #ObfuscationMap ORDER BY ServerName, ObjectType, OriginalName;
```

**Option B: Query the map table on prod servers**

If you used `-ObfuscationMapTable` to persist the map on each server:

```sql
-- Decode a single token
SELECT ObjectType, OriginalName, ObfuscatedName
FROM dbo.DiagObfMap
WHERE ObfuscatedName = N'TBL_a1b2c3';

-- Full map for this server
SELECT ObjectType, OriginalName, ObfuscatedName
FROM dbo.DiagObfMap
ORDER BY ObjectType, OriginalName;

-- Decode across servers (linked servers)
SELECT 'PROD-SQL01' AS [Server], ObjectType, OriginalName, ObfuscatedName
FROM [PROD-SQL01].master.dbo.DiagObfMap
UNION ALL
SELECT 'PROD-SQL02', ObjectType, OriginalName, ObfuscatedName
FROM [PROD-SQL02].master.dbo.DiagObfMap
ORDER BY [Server], ObjectType, OriginalName;
```

**Key points:**
- The seed makes hashes **deterministic** — the same name produces the same token across servers and runs, so findings are cross-referenceable
- The map table **appends** on each run (no data loss from prior runs)
- The `_CONFIDENTIAL_DECODE.sql` file is standalone — no server access needed to decode tokens
- Without the seed, the map, or access to the server, obfuscated tokens cannot be reversed

### Custom Analysis

```sql
-- Last 90 days, only top 5 items, long-running threshold at 15 minutes
EXEC dbo.sp_StatUpdate_Diag
    @DaysBack = 90,
    @TopN = 5,
    @LongRunningMinutes = 15,
    @ExpertMode = 1;

-- CommandLog in a different database
EXEC dbo.sp_StatUpdate_Diag @CommandLogDatabase = N'DBATools';

-- Single result set mode (JSON) for programmatic consumption
EXEC dbo.sp_StatUpdate_Diag @SingleResultSet = 1, @ExpertMode = 1;
```

### Multi-Server (PowerShell)

```powershell
.\Invoke-StatUpdateDiag.ps1 `
    -Servers "Server1", "Server2,2500", "Server3" `
    -CommandLogDatabase "Maintenance" `
    -OutputPath ".\diag_output" `
    -OutputFormat Markdown
```

Cross-server analysis detects version skew and parameter inconsistencies.

### Diagnostic Checks

| Severity | ID | Checks |
|----------|----|--------|
| CRITICAL | C1-C4 | Killed runs, repeated stat failures, time limit exhaustion, degrading throughput |
| WARNING | W1-W5 | Suboptimal parameters, long-running stats, stale-stats backlog, overlapping runs, QS not effective |
| INFO | I1-I5 | Run health trends, parameter history, top tables by cost, unused features, version history |
| INFO | I6 | QS efficacy: "10 of 10 highest-workload stats updated in first 1 minute" |
| INFO | I7 | QS inflection: before/after comparison when QS prioritization was enabled |
| INFO | I8 | QS performance trend: per-stat CPU correlation across runs |

### Diag Parameter Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@DaysBack` | `30` | History window in days |
| `@ExpertMode` | `0` | `0` = dashboard + recommendations, `1` = all 13 result sets |
| `@SkipHistory` | `0` | `1` = skip persistent history table |
| `@Obfuscate` | `0` | `1` = hash all names for external sharing |
| `@ObfuscationSeed` | `NULL` | Salt for deterministic hashing |
| `@ObfuscationMapTable` | `NULL` | Persist obfuscation map to a table |
| `@EfficacyDaysBack` | `NULL` | QS efficacy broad window (NULL = @DaysBack) |
| `@EfficacyDetailDays` | `NULL` | QS efficacy close-up window (NULL = 14) |
| `@LongRunningMinutes` | `10` | Threshold for long-running stat detection |
| `@FailureThreshold` | `3` | Same stat failing N+ times = CRITICAL |
| `@TimeLimitExhaustionPct` | `80` | Warn if >X% of runs hit time limit |
| `@ThroughputWindowDays` | `7` | Window for throughput trend comparison |
| `@TopN` | `20` | Top N items in detail result sets |
| `@CommandLogDatabase` | `NULL` | CommandLog location (NULL = current DB) |
| `@SingleResultSet` | `0` | `1` = JSON-formatted single result set |
| `@Debug` | `0` | `1` = verbose output |

## Extended Events

An XE session is included for runtime troubleshooting:

```sql
-- Create and start (see sp_StatUpdate_XE_Session.sql)
ALTER EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER STATE = START;
```

Captures UPDATE STATISTICS commands, errors, lock waits, lock escalation, and long-running statements.

## Version History

- **2.16.2026.0308** - QS Efficacy Trending + Executive Dashboard. sp_StatUpdate: ProcessingPosition in per-stat ExtendedInfo XML. Diag: @ExpertMode (management vs DBA view), @SkipHistory, persistent StatUpdateDiagHistory table, Executive Dashboard with A-F grades, RS 13 QS Performance Correlation (per-stat CPU trend), I6/I7/I8 checks for QS efficacy/inflection/performance correlation. 13 result sets total. @EfficacyDaysBack, @EfficacyDetailDays parameters. 122 tests.
- **2.14.2026.0304** - Bulk issue resolution (42 issues). New: @OrphanedRunThresholdHours. AG/safety guards, warnings, discovery improvements. See CLAUDE.md for full details.
- **2.8.2026.0302** - Comprehensive issue sweep (31 issues resolved). New params: @IncludeIndexedViews, @LogSkippedToCommandLog, @ReturnDetailedResults, @CompletionNotifyTable. Detection: QS forced plan warning, RLS, wide stats, columnstore context, filtered index mismatch, computed columns, Stretch DB skip, log space check. Azure MI vs DB distinction. 12-topic @Help operational notes.
- **2.7.2026.0302** - AG redo queue pause (@MaxAGRedoQueueMB, @MaxAGWaitMinutes). tempdb pressure check (@MinTempdbFreeMB). New StopReasons: AG_REDO_QUEUE, TEMPDB_PRESSURE.
- **2.6.2026.0302** - Multi-database direct mode: @Statistics and @StatisticsFromTable respect @Databases.
- **2.5.2026.0302** - CommandLog index advisory, Resource Governor detection, @LockTimeout docs.
- **2.4.2026.0302** - Region markers for LLM navigation. Collation-aware comparisons (COLLATE DATABASE_DEFAULT). Per-phase timing in debug mode. **Comprehensive bug-fix and UX sprint (12 issues):** 4 P1 fixes (MAX_GRANT_PERCENT parameterized via `@MaxGrantPercent`, SQL injection hardened on DIRECT mode inputs, RESAMPLE floor added via `@PersistSampleMinRows`, container memory diagnostics improved); 5 P2 enhancements (ROWLOCK on discovery candidate query, heartbeat TRY/CATCH so write failures don't fail the stat, StopByTime overshoot protection via `@MaxSecondsPerStat`, QS forced plan detection unconditional on startup, OLTP_LIGHT preset `@StopByTime` fix); 3 P3 UX improvements (`@Help` QUICK START preset guide, dry-run `Mode:` label in startup banner, ETR `~Xm` in per-stat Complete message).
- **2.3.2026.0302** - Bug fixes: applock release on CATCH, @@TRANCOUNT check, SET ANSI_WARNINGS/ARITHABORT, HAS_DBACCESS() filter, MAXRECURSION unlimited, midnight @StopByTime crossing, @CheckPermissionsOnly, CommandLog schema check.
- **2.1.2026.0219** - Extended Awareness. Azure SQL detection, hardware context, RCSI awareness, Replication/CDC/Temporal detection. New: @MaxConsecutiveFailures, @WarningsOut, @StopReasonOut.
- **2.0.2026.0212** - Environment Intelligence. CE version/trace flag/DB-scoped config detection. Staged discovery auto-fallback. Truncated partition handling. XE session overhaul. Diagnostic tool (sp_StatUpdate_Diag).
- **1.9.2026.0206** - Status/StatusMessage columns for Agent alerting. Batch QS enrichment (O(n) to O(1)).
- **1.9.2026.0128** - @Preset parameter, @GroupByJoinPattern, ##sp_StatUpdate_Progress, @CleanupOrphanedRuns default Y.
- **1.8.2026.0128** - Code review fixes, XE troubleshooting session.
- **1.7.2026.0127** - BREAKING: @ModificationThreshold default 1000 to 5000.
- **1.6.2026.0128** - Staged discovery, adaptive sampling, @ExcludeTables, @WhatIfOutputTable.
- **1.5.2026.0120** - CRITICAL: Fixed @ExcludeStatistics filter, incremental partition targeting.
- **1.4.2026.0119** - Query Store prioritization, filtered stats handling.
- **1.3.2026.0119** - Multi-database support, OUTPUT parameters, return codes.
- **1.2.2026.0117** - Tiger Toolbox tiered thresholds, AND/OR logic, PERSIST_SAMPLE_PERCENT.
- **1.1.2026.0117** - Style refactor, @Help, parallel mode.
- **1.0.2026.0117** - Initial public release.

## When to Use This (vs IndexOptimize)

**IndexOptimize** is battle-tested and handles indexes + stats together. Use it for general maintenance.

**sp_StatUpdate** is for when you need:
- Priority ordering (worst stats first)
- Time-limited runs with graceful stops
- NORECOMPUTE targeting
- Query Store-driven prioritization
- Adaptive sampling for problematic stats
- AG-safe maintenance with redo queue awareness
- Programmatic access to results via OUTPUT parameters

## License

MIT License - see [LICENSE](LICENSE) for details.

Based on patterns from [Ola Hallengren's SQL Server Maintenance Solution](https://ola.hallengren.com) (MIT License).

## Acknowledgments

- [Ola Hallengren](https://ola.hallengren.com) - sp_StatUpdate wouldn't exist without his SQL Server Maintenance Solution. We use his CommandLog table, Queue patterns, and database selection syntax. If you're not already using his tools, start there.
- [Brent Ozar](https://www.brentozar.com) - years of emphasizing stats over index rebuilds, First Responder Kit, and community education.
- [Erik Darling](https://www.erikdarling.com) - T-SQL coding style and performance insights. His diagnostic tools are excellent - I'm particularly fond of sp_LogHunter and sp_QuickieStore.
- [Tiger Team's AdaptiveIndexDefrag](https://github.com/microsoft/tigertoolbox) - the 5-tier adaptive threshold formula
- [Colleen Morrow](https://www.sqlservercentral.com/blogs/better-living-thru-powershell-update-statistics-in-parallel) - parallel statistics maintenance concept
