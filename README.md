# sp_StatUpdate

**Priority-based statistics maintenance for SQL Server 2016+**

*Updates worst stats first. Stops when you tell it to. Tells you if it got killed.*

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![SQL Server 2016+](https://img.shields.io/badge/SQL%20Server-2016%2B-blue.svg)](https://www.microsoft.com/sql-server)
[![Azure SQL](https://img.shields.io/badge/Azure%20SQL-Supported-0078D4.svg)](https://azure.microsoft.com/products/azure-sql)

## Why This Exists

| Problem | Fix |
|---------|-----|
| Alphabetical ordering | `@SortOrder = 'MODIFICATION_COUNTER'` -- worst first |
| 10-hour jobs killed at 5 AM | `@TimeLimit` -- stops gracefully, logs what's left |
| "Did it finish or get killed?" | START/END markers in CommandLog |
| NORECOMPUTE orphans | `@TargetNorecompute = 'Y'` -- finds and refreshes them |
| Large stats that never finish | `@LongRunningThresholdMinutes` -- auto-reduce sample rate |
| Query Store knows what's hot | `@QueryStore = 'CPU'` -- prioritize by workload metric |
| QS enrichment too slow | `@QueryStoreTopPlans = 200` -- parse only top N plans |
| Cascading failures | `@FailFast = 1` -- stop on first error |
| AG secondary falls behind | `@MaxAGRedoQueueMB` -- pauses when redo queue is deep |
| tempdb pressure during FULLSCAN | `@MinTempdbFreeMB` -- checks before each stat update |
| Azure DTU/vCore concerns | Auto-detects Azure SQL DB vs MI, platform-specific warnings |
| Priority pass finishes early | `@MopUpPass = 'Y'` -- broad sweep with remaining time |

## Quick Start

```sql
-- 1. Install prerequisites (Ola Hallengren's CommandLog table)
-- Download from: https://ola.hallengren.com/scripts/CommandLog.sql

-- 2. Install sp_StatUpdate
-- Run sp_StatUpdate.sql in your maintenance database

-- 3. Run statistics maintenance
EXEC dbo.sp_StatUpdate
    @Databases = N'YourDatabase',
    @TimeLimit = 3600;                  -- 1 hour limit
    -- Defaults: @Preset='DEFAULT', @TargetNorecompute='BOTH', @LogToTable='Y'
```

## Requirements

**DROP-IN COMPATIBLE** with [Ola Hallengren's SQL Server Maintenance Solution](https://ola.hallengren.com).

| Requirement | Details |
|-------------|---------|
| **SQL Server** | 2016+ (uses `STRING_SPLIT`). 2016 SP2+ recommended for MAXDOP support |
| **Azure SQL** | Database (EngineEdition 5), Managed Instance (8), and Edge (9) supported |
| **dbo.CommandLog** | [CommandLog.sql](https://ola.hallengren.com/scripts/CommandLog.sql) or set `@LogToTable = 'N'` |
| **dbo.Queue** | [Queue.sql](https://ola.hallengren.com/scripts/Queue.sql) -- only for `@StatsInParallel = 'Y'` |

**Note**: `dbo.QueueStatistic` is auto-created on first parallel run. `dbo.CommandExecute` is NOT required.

## Presets

v3 uses a preset-first API.  Choose a preset, then override individual parameters as needed.  Explicit parameters always win over preset defaults.

| Preset | TimeLimit | SortOrder | QueryStore | MopUp | Sample | Description |
|--------|-----------|-----------|------------|-------|--------|-------------|
| `DEFAULT` | 18000 (5h) | MODIFICATION_COUNTER | OFF | N | auto | Balanced default for any workload |
| `NIGHTLY` | 3600 (1h) | QUERY_STORE | CPU | Y | auto | QS-prioritized nightly job with mop-up |
| `WEEKLY_FULL` | 14400 (4h) | QUERY_STORE | CPU | Y | 100 | Comprehensive weekly: FULLSCAN + lower thresholds |
| `OLTP_LIGHT` | 1800 (30m) | MODIFICATION_COUNTER | OFF | N | auto | Low-impact OLTP: high threshold, inter-stat delay |
| `WAREHOUSE` | unlimited | ROWS | OFF | N | 100 | Data warehouse: FULLSCAN, no time limit |

```sql
-- Nightly maintenance (1hr, QS-prioritized, mop-up)
EXEC dbo.sp_StatUpdate @Preset = 'NIGHTLY', @Databases = 'USER_DATABASES';

-- Weekly comprehensive (4hr, FULLSCAN)
EXEC dbo.sp_StatUpdate @Preset = 'WEEKLY_FULL', @Databases = 'USER_DATABASES';

-- OLTP with minimal impact (30min, high thresholds, delays)
EXEC dbo.sp_StatUpdate @Preset = 'OLTP_LIGHT', @Databases = 'MyOLTPDatabase';

-- Data warehouse full refresh (no limit, FULLSCAN)
EXEC dbo.sp_StatUpdate @Preset = 'WAREHOUSE', @Databases = 'MyDW';

-- Preset + overrides: NIGHTLY preset but with 2hr time limit
EXEC dbo.sp_StatUpdate @Preset = 'NIGHTLY', @Databases = 'USER_DATABASES', @TimeLimit = 7200;
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
    @QueryStore = N'CPU',                   -- Or DURATION, READS, AVG_CPU, MEMORY_GRANT, etc.
    @SortOrder = N'QUERY_STORE',
    @TimeLimit = 3600;

-- Large QS catalog? Limit XML plan parsing to top 200 plans by CPU
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @QueryStore = N'CPU',
    @QueryStoreTopPlans = 200,              -- Default 500. NULL = unlimited
    @SortOrder = N'QUERY_STORE',
    @TimeLimit = 3600;
```

**Available QS metrics:** `CPU`, `DURATION`, `READS`, `EXECUTIONS`, `AVG_CPU`, `MEMORY_GRANT`, `TEMPDB_SPILLS` (SQL 2017+), `PHYSICAL_READS`, `AVG_MEMORY`, `WAITS`

**Performance note:** Phase 6 (QS enrichment) parses plan XML to find table references. On databases with 10,000+ QS plans, this can take minutes. `@QueryStoreTopPlans` limits XML parsing to the most impactful plans. The proc also skips Phase 6 entirely when QS has no recent runtime stats within `@QueryStoreRecentHours`.

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
    @MaxAGWaitMinutes = 10,                 -- Wait up to 10 min for drain
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

### Mop-Up Pass (Use Remaining Time)

```sql
-- 2-hour window: priority pass first, then broad sweep with remaining time
EXEC dbo.sp_StatUpdate
    @Databases = N'USER_DATABASES',
    @TimeLimit = 7200,
    @MopUpPass = N'Y',
    @MopUpMinRemainingSeconds = 120;
```

The priority pass applies your configured thresholds and sort order.  If it completes
with time to spare, the mop-up pass discovers every stat with `modification_counter > 0`
that wasn't already updated in this run and processes them by modification count descending.
Requires `@LogToTable = 'Y'` and `@Execute = 'Y'`.  Not compatible with `@StatsInParallel`.

### Absolute Stop Time

```sql
-- Stop at 4 AM regardless of when the job started
EXEC dbo.sp_StatUpdate
    @Databases = N'USER_DATABASES',
    @StopByTime = N'04:00';
```

## Parameter Reference

Run `EXEC sp_StatUpdate @Help = 1` for complete documentation including operational notes and preset details.

### v3 API Summary

v3 has **42 input parameters** (was 58 in v2) plus **11 OUTPUT parameters**.  23 parameters from v2 were absorbed into preset-controlled internal variables.  Explicit parameters always override preset defaults.

### Database & Table Selection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@Statistics` | `NULL` | Direct stat references: `'Schema.Table.Stat'` (comma-separated, skips discovery) |
| `@Databases` | Current DB | `USER_DATABASES`, `SYSTEM_DATABASES`, `ALL_DATABASES`, `AVAILABILITY_GROUP_DATABASES`, wildcards (`%Prod%`), exclusions (`-DevDB`) |
| `@Tables` | All | Table filter (comma-separated `Schema.Table`) |
| `@ExcludeTables` | `NULL` | Exclude tables by LIKE pattern (`%Archive%`) |
| `@ExcludeStatistics` | `NULL` | Exclude stats by LIKE pattern (`_WA_Sys%`) |

### Preset & Threshold Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@Preset` | `'DEFAULT'` | `DEFAULT`, `NIGHTLY`, `WEEKLY_FULL`, `OLTP_LIGHT`, `WAREHOUSE` |
| `@TargetNorecompute` | `'BOTH'` | `'Y'`=NORECOMPUTE only, `'N'`=regular only, `'BOTH'`=all |
| `@ModificationThreshold` | Preset-dependent | Minimum modifications to qualify (DEFAULT=5000) |
| `@StaleHours` | `NULL` | Minimum hours since last update |
| `@FilteredStatsMode` | `'INCLUDE'` | Filtered statistics handling: `INCLUDE`, `EXCLUDE`, `ONLY`, `PRIORITY` (drift-boosted) |
| `@FilteredStatsStaleFactor` | `2.0` | Filtered stat counts as drifted when `unfiltered_rows/rows` exceeds this factor |

### Execution Control

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@TimeLimit` | Preset-dependent | Max seconds (DEFAULT=18000). `NULL` = unlimited |
| `@StopByTime` | `NULL` | Absolute wall-clock stop time (`'04:00'` = 4 AM). Overrides `@TimeLimit` |
| `@BatchLimit` | `NULL` | Max stats per run |
| `@SortOrder` | Preset-dependent | Priority order (see below) |
| `@MopUpPass` | Preset-dependent | `'Y'` = broad sweep after priority pass |
| `@Execute` | `'Y'` | `'N'` for dry run |
| `@FailFast` | `0` | `1` = abort on first error |

### Sort Orders

| Value | Description |
|-------|-------------|
| `MODIFICATION_COUNTER` | Most modifications first (DEFAULT/OLTP_LIGHT preset) |
| `QUERY_STORE` | Highest Query Store metric first (NIGHTLY/WEEKLY_FULL preset) |
| `ROWS` | Largest tables first (WAREHOUSE preset) |
| `DAYS_STALE` | Oldest stats first |
| `PAGE_COUNT` | Largest tables by page count first |
| `FILTERED_DRIFT` | Filtered stats with drift first |
| `AUTO_CREATED` | User-created stats before auto-created |
| `RANDOM` | Random order |

### Query Store Integration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@QueryStore` | Preset-dependent | `'OFF'` or metric name: `CPU`, `DURATION`, `READS`, `EXECUTIONS`, `AVG_CPU`, `MEMORY_GRANT`, `TEMPDB_SPILLS`, `PHYSICAL_READS`, `AVG_MEMORY`, `WAITS`, `WAIT_CPU` |
| `@QueryStoreTopPlans` | `500` | Max plans to XML-parse. `NULL` = unlimited. Lower = faster Phase 6 |
| `@QueryStoreMinExecutions` | `100` | Minimum plan executions to boost |
| `@QueryStoreRecentHours` | `168` | Only consider plans from last N hours (7 days) |

### Sampling

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@StatisticsSample` | Preset-dependent | `NULL`=SQL Server decides, `100`=FULLSCAN |
| `@MaxDOP` | `NULL` | MAXDOP for UPDATE STATISTICS (SQL 2016 SP2+) |
| `@LongRunningThresholdMinutes` | `NULL` | Stats that took longer get forced sample rate |
| `@LongRunningSamplePercent` | `10` | Sample percent for long-running stats |

### Safety Checks

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@MaxAGRedoQueueMB` | `NULL` | Pause when AG secondary redo queue exceeds this MB |
| `@MaxAGWaitMinutes` | `10` | Max minutes to wait for redo queue to drain |
| `@MinTempdbFreeMB` | `NULL` | Min tempdb free space (MB). `@FailFast=1` aborts, else warns |

### Logging & Output

> **Two-phase logging:** Like Ola Hallengren's `CommandExecute`, sp_StatUpdate inserts a CommandLog row with NULL `EndTime` before each stat update, then updates `EndTime` on completion. Query in-progress stats with: `SELECT * FROM dbo.CommandLog WHERE EndTime IS NULL AND CommandType = 'UPDATE_STATISTICS';`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@LogToTable` | `'Y'` | Log to dbo.CommandLog |
| `@WhatIfOutputTable` | `NULL` | Table for dry-run commands (`@Execute = 'N'` required) |
| `@MopUpMinRemainingSeconds` | `60` | Minimum seconds remaining to trigger mop-up |
| `@Debug` | `0` | `1` = verbose diagnostic output |

### Parallel Execution

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@StatsInParallel` | `'N'` | `'Y'` = queue-based parallel processing via `dbo.QueueStatistic` |

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

## Environment Detection

Debug mode (`@Debug = 1`) automatically reports:

- **SQL Server version** and build number
- **Cardinality Estimator** version per database (Legacy CE 70 vs New CE 120+)
- **Trace flags** affecting statistics (2371, 9481, 2389/2390, 4139)
- **DB-scoped configs** (LEGACY_CARDINALITY_ESTIMATION)
- **Hardware context** (CPU count, memory, NUMA nodes, uptime)
- **Azure platform** (SQL DB vs Managed Instance vs Edge, with platform-specific guidance)
- **AG primary status** and redo queue depth
- **tempdb free space**
- **Resource Governor** active resource pools

### Per-Database Detection

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
-- Log progress to CommandLog every 50 stats (secure, access-controlled)
EXEC sp_StatUpdate @Databases = 'USER_DATABASES', @TimeLimit = 3600;
-- Query in-progress stats:
SELECT * FROM dbo.CommandLog WHERE EndTime IS NULL AND CommandType = 'UPDATE_STATISTICS';
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
| 2 | **Recommendations** | Severity-categorized findings with fix-it SQL. Includes I10: a synthesized `EXEC sp_StatUpdate` call tuned to your environment |

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
- **RS 13 (QS Performance Correlation)**: Per-stat CPU before vs after -- "5 of 5 tracked stats show 8% lower query CPU"
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

### Grade Overrides

Customize the Executive Dashboard when you know certain issues are expected or irrelevant.

#### @GradeOverrides -- Force Grades or Ignore Categories

Force a specific letter grade (A/B/C/D/F) or exclude a category entirely (IGNORE).

```sql
-- "I know about the 2 killed runs -- don't penalize the score"
EXEC dbo.sp_StatUpdate_Diag @GradeOverrides = 'RELIABILITY=A';

-- "I don't use Query Store -- exclude workload focus from my score"
EXEC dbo.sp_StatUpdate_Diag @GradeOverrides = 'WORKLOAD=IGNORE';

-- Force multiple: known slow stats + don't care about QS
EXEC dbo.sp_StatUpdate_Diag
    @GradeOverrides = 'SPEED=B, WORKLOAD=IGNORE';

-- "Only care about completion and speed -- ignore everything else"
EXEC dbo.sp_StatUpdate_Diag
    @GradeOverrides = 'RELIABILITY=IGNORE, WORKLOAD=IGNORE';

-- Force a low grade to flag a known problem for management visibility
EXEC dbo.sp_StatUpdate_Diag @GradeOverrides = 'COMPLETION=F';
```

**Valid categories:** `COMPLETION`, `RELIABILITY`, `SPEED`, `WORKLOAD`
**Valid values:** `A`, `B`, `C`, `D`, `F` (force grade), `IGNORE` (exclude from OVERALL score)

#### @GradeWeights -- Custom Category Weights

Change how much each category contributes to the OVERALL score. Values are integers that auto-normalize to sum to 100%.

```sql
-- Default weights: COMPLETION=30, RELIABILITY=25, SPEED=20, WORKLOAD=25

-- Single category override: bump completion importance
-- 50 + 25(default) + 20(default) + 25(default) = 120 -> normalized to 42/21/17/21
EXEC dbo.sp_StatUpdate_Diag @GradeWeights = 'COMPLETION=50';

-- Two categories: only care about completion and speed equally
-- 50 + 25(default) + 50 + 25(default) = 150 -> normalized to 33/17/33/17
EXEC dbo.sp_StatUpdate_Diag @GradeWeights = 'COMPLETION=50, SPEED=50';

-- Weight=0 is the same as IGNORE -- excludes category from OVERALL
EXEC dbo.sp_StatUpdate_Diag @GradeWeights = 'WORKLOAD=0';

-- All four explicit (auto-normalized, don't need to sum to 100)
EXEC dbo.sp_StatUpdate_Diag
    @GradeWeights = 'COMPLETION=40, RELIABILITY=10, SPEED=30, WORKLOAD=20';
```

#### Combining Overrides and Weights

```sql
-- Force reliability to A (known kills are expected) AND
-- weight completion heavily for management reporting
EXEC dbo.sp_StatUpdate_Diag
    @GradeOverrides = 'RELIABILITY=A',
    @GradeWeights = 'COMPLETION=40, WORKLOAD=40';
```

#### Dashboard Output with Overrides

```
Category        Grade  Score  Headline
--------------  -----  -----  ---------------------------------------------------
OVERALL         B         86  Statistics maintenance is healthy... [Overrides active]
COMPLETION      A        100  Nearly all qualifying statistics are being updated...
RELIABILITY     A         28  [OVERRIDE: A] 2 run(s) were killed...
SPEED           -          0  [IGNORED] Updates are slow at 17.9 sec/stat...
WORKLOAD FOCUS  D         50  Query Store prioritization is not enabled...
```

- `[OVERRIDE: A]` -- grade forced; Detail column shows `(actual score: 28)`
- `[IGNORED]` -- excluded from OVERALL; Grade=`-`, Score=0
- `[Overrides active]` -- shown on OVERALL when any override/weight change is active

**Weight normalization:** Weights are integers that auto-normalize to sum to 100%. Passing a single category (e.g., `'COMPLETION=50'`) keeps the other three at their defaults (25, 20, 25), then all four are normalized together (50+25+20+25=120 -> 42/21/17/21%). Weight of 0 is equivalent to IGNORE.

**Note:** History table (`StatUpdateDiagHistory`) always uses hardcoded weights (30/25/20/25) -- overrides only affect the current dashboard view, not persisted scores.

### Obfuscated Mode

Hash all database, table, and statistics names for safe external sharing. Prefixes (`IX_`, `PK_`, `DB_`, `_WA_Sys_`) are preserved so consultants can still reason about object types.

#### Quick Start: Share a Report with a Consultant

```sql
-- 1. Generate obfuscated report with a seed (keeps tokens stable across runs)
EXEC dbo.sp_StatUpdate_Diag
    @Obfuscate = 1,
    @ExpertMode = 1,
    @ObfuscationSeed = N'acme-2026-q1';
```

Output tokens look like: `DB_7f2a`, `TBL_e4c1`, `IX_STAT_9b3d`. The seed ensures the same object always maps to the same token -- so if a consultant says "TBL_e4c1 is slow", you can decode it consistently.

#### T-SQL Examples

```sql
-- Basic: one-off obfuscated output (random hashes, no persistence)
EXEC dbo.sp_StatUpdate_Diag @Obfuscate = 1, @ExpertMode = 1;

-- Seeded: deterministic hashes (same name = same token every time)
EXEC dbo.sp_StatUpdate_Diag
    @Obfuscate = 1,
    @ExpertMode = 1,
    @ObfuscationSeed = N'acme-2026-q1';

-- Seeded + persisted map table: decode tokens later without re-running
EXEC dbo.sp_StatUpdate_Diag
    @Obfuscate = 1,
    @ExpertMode = 1,
    @ObfuscationSeed = N'acme-2026-q1',
    @ObfuscationMapTable = N'dbo.DiagObfMap';

-- After running with @ObfuscationMapTable, the proc prints a decode query:
--   === Decode obfuscated tokens ===
--   SELECT ObjectType, OriginalName, ObfuscatedName
--   FROM dbo.DiagObfMap WHERE ObfuscatedName = N'<paste_token_here>';
```

#### PowerShell: Multi-Server Obfuscated Reports

When running the wrapper with `-Obfuscate`, three files are produced per run:

| File | Contains | Share externally? |
|------|----------|-------------------|
| `*_SAFE_TO_SHARE.{md,html,json}` | Obfuscated names only | Yes |
| `*_CONFIDENTIAL.{md,html,json}` | Real server/database/table names | **No** |
| `*_CONFIDENTIAL_DECODE.sql` | Standalone T-SQL script to decode tokens | **No** |

```powershell
# Generate reports for 3 servers -- Markdown format, seeded obfuscation
.\Invoke-StatUpdateDiag.ps1 `
    -Servers "PROD-SQL01", "PROD-SQL02", "PROD-SQL03" `
    -Obfuscate `
    -ObfuscationSeed "acme-2026-q1" `
    -OutputFormat Markdown `
    -OutputPath "C:\temp\diag"

# Output:
#   C:\temp\diag\sp_StatUpdate_Diag_20260310_SAFE_TO_SHARE.md   <-- send this
#   C:\temp\diag\sp_StatUpdate_Diag_20260310_CONFIDENTIAL.md    <-- keep this
#   C:\temp\diag\sp_StatUpdate_Diag_20260310_CONFIDENTIAL_DECODE.sql

# Also persist the map table on each server for later decoding
.\Invoke-StatUpdateDiag.ps1 `
    -Servers "PROD-SQL01", "PROD-SQL02" `
    -Obfuscate `
    -ObfuscationSeed "acme-2026-q1" `
    -ObfuscationMapTable "dbo.DiagObfMap" `
    -OutputPath "C:\temp\diag"
```

Without `-Obfuscate`, a single report file is produced (no suffix).

#### Typical Workflow: Consultant Engagement

```
1. DBA runs:     Invoke-StatUpdateDiag.ps1 -Servers ... -Obfuscate -ObfuscationSeed "..."
2. DBA sends:    *_SAFE_TO_SHARE.md to consultant (no real names visible)
3. Consultant:   "TBL_e4c1 has a C2 finding -- stat IX_STAT_9b3d fails every run"
4. DBA decodes:  Opens _CONFIDENTIAL_DECODE.sql in SSMS, searches for TBL_e4c1
5. DBA finds:    TBL_e4c1 = dbo.OrderHistory, IX_STAT_9b3d = IX_OrderHistory_Date
6. DBA fixes:    The actual object, shares updated SAFE_TO_SHARE report to confirm
```

### Decoding Obfuscated Results

When a consultant returns findings referencing tokens like `TBL_e4c1`, you have two options:

**Option A: Use the decode SQL file (no server access needed)**

The `_CONFIDENTIAL_DECODE.sql` file is a standalone T-SQL script with the full map in a temp table:

```sql
-- 1. Open _CONFIDENTIAL_DECODE.sql in SSMS and execute it (creates #ObfuscationMap)
-- 2. Decode a specific token from the consultant's findings:
SELECT * FROM #ObfuscationMap WHERE ObfuscatedName = N'TBL_e4c1';
-- 3. Decode multiple tokens at once:
SELECT * FROM #ObfuscationMap WHERE ObfuscatedName IN (N'TBL_e4c1', N'IX_STAT_9b3d', N'DB_7f2a');
-- 4. Full map sorted by server:
SELECT * FROM #ObfuscationMap ORDER BY ServerName, ObjectType, OriginalName;
```

**Option B: Query the persisted map table on the server**

If you used `@ObfuscationMapTable` (T-SQL) or `-ObfuscationMapTable` (PowerShell):

```sql
-- Decode a single token
SELECT ObjectType, OriginalName, ObfuscatedName
FROM dbo.DiagObfMap
WHERE ObfuscatedName = N'TBL_e4c1';

-- Export full map to CSV (useful for Excel cross-referencing)
-- In SSMS: Results to File, then run:
SELECT ObjectType, OriginalName, ObfuscatedName
FROM dbo.DiagObfMap
ORDER BY ObjectType, OriginalName;

-- Decode across multiple servers via linked servers
SELECT 'PROD-SQL01' AS [Server], ObjectType, OriginalName, ObfuscatedName
FROM [PROD-SQL01].master.dbo.DiagObfMap
UNION ALL
SELECT 'PROD-SQL02', ObjectType, OriginalName, ObfuscatedName
FROM [PROD-SQL02].master.dbo.DiagObfMap
ORDER BY [Server], ObjectType, OriginalName;
```

**How obfuscation works:**
- **With a seed**: Hashes are **deterministic** -- the same object always produces the same token across servers, runs, and time. This means `TBL_e4c1` in Monday's report is the same table as `TBL_e4c1` in Friday's report.
- **Without a seed**: Hashes are random per run. Useful for one-off sharing but tokens can't be correlated across runs.
- The map table **appends** on each run (no data loss from prior runs)
- The `_CONFIDENTIAL_DECODE.sql` file is standalone -- works in any SSMS session, no server access needed
- Without the seed, the map, or the decode file, obfuscated tokens **cannot** be reversed (HASHBYTES is one-way)

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
| CRITICAL | C1-C5 | Killed runs, repeated stat failures, time limit exhaustion, degrading throughput, sample rate degradation |
| WARNING | W1-W10 | Suboptimal parameters, long-running stats, stale-stats backlog, overlapping runs, QS not effective, excessive overhead, mop-up ineffective, lock timeout ineffective, parameter churn |
| INFO | I1-I5 | Run health trends, parameter history, top tables by cost, unused features, version history |
| INFO | I6 | QS efficacy: "10 of 10 highest-workload stats updated in first 1 minute" |
| INFO | I7 | QS inflection: before/after comparison when QS prioritization was enabled |
| INFO | I8 | QS performance trend: per-stat CPU correlation across runs |
| INFO | I10 | Recommended configuration: synthesized EXEC call based on diagnostic findings |
| INFO | I11-I14 | Failure clustering, QS coverage drift, parallel opportunity, mop-up missing pagecount |

### Diag Parameter Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@DaysBack` | `30` | History window in days |
| `@ExpertMode` | `0` | `0` = dashboard + recommendations, `1` = all 13 result sets |
| `@SkipHistory` | `0` | `1` = skip persistent history table |
| `@GradeOverrides` | `NULL` | Force grades or ignore categories: `'RELIABILITY=A, SPEED=IGNORE'` |
| `@GradeWeights` | `NULL` | Custom category weights (auto-normalized to 100%): `'COMPLETION=50, WORKLOAD=50'` |
| `@Obfuscate` | `0` | `1` = hash all names for external sharing |
| `@ObfuscationSeed` | `NULL` | Salt for deterministic hashing |
| `@ObfuscationMapTable` | `NULL` | Persist obfuscation map to a table |
| `@EfficacyDaysBack` | `NULL` | QS efficacy broad window (NULL = @DaysBack) |
| `@EfficacyDetailDays` | `NULL` | QS efficacy close-up window (NULL = 14) |
| `@LongRunningMinutes` | `10` | Threshold for long-running stat detection |
| `@FailureThreshold` | `3` | Same stat failing N+ times = CRITICAL |
| `@TimeLimitExhaustionPct` | `80` | Warn if >X% of runs hit time limit |
| `@ThroughputWindowDays` | `7` | Window for throughput trend comparison; C4 compares the recent N days against the preceding N days (requires `@DaysBack` >= 2 x N) |
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

## Migrating from v2

v3 simplifies the API from 58 (v2) to 42 input parameters.  Most v2 scripts work with minor changes.

### Quick Migration Guide

| v2 Parameter | v3 Equivalent |
|--------------|---------------|
| `@QueryStorePriority = 'Y', @QueryStoreMetric = 'CPU'` | `@QueryStore = 'CPU'` |
| `@DaysStaleThreshold = 7` | `@StaleHours = 168` |
| `@HoursStaleThreshold = 48` | `@StaleHours = 48` |
| `@Preset = 'NIGHTLY_MAINTENANCE'` | `@Preset = 'NIGHTLY'` |
| `@Preset = 'WAREHOUSE_AGGRESSIVE'` | `@Preset = 'WAREHOUSE'` |
| `@TieredThresholds = 1` | Preset-controlled (always on for DEFAULT/NIGHTLY/WEEKLY_FULL) |
| `@ThresholdLogic = 'OR'` | Preset-controlled |
| `@ModificationPercent = 10` | Preset-controlled |
| `@MaxConsecutiveFailures = 5` | Preset-controlled |
| `@DelayBetweenStats = 2` | Preset-controlled (OLTP_LIGHT uses 2s delay) |
| `@LockTimeout = 10` | Preset-controlled (OLTP_LIGHT uses 10s) |
| `@CleanupOrphanedRuns = 'Y'` | Always on |
| `@PersistSamplePercent = 'Y'` | Always on (when supported) |
| `@IncludeSystemObjects = 'Y'` | Removed (system objects excluded) |
| `@IncludeIndexedViews = 'Y'` | Removed (indexed views always included) |
| `@GroupByJoinPattern = 'Y'` | Removed |
| `@ExposeProgressToAllSessions = 'Y'` | Removed |
| `@CompletionNotifyTable` | Removed |
| `@LogSkippedToCommandLog` | Removed |
| `@ReturnDetailedResults` | Removed |
| `@ProgressLogInterval` | Removed |
| `@StatisticsFromTable` | Removed |
| `@FilteredStatsMode` | Public again since v3.7.0 (was preset-controlled in v3.0-v3.6) |

### Example: v2 Agent Job to v3

```sql
-- v2 (old)
EXEC dbo.sp_StatUpdate
    @Databases = N'USER_DATABASES',
    @QueryStorePriority = N'Y',
    @QueryStoreMetric = N'CPU',
    @TieredThresholds = 1,
    @TimeLimit = 3600,
    @SortOrder = N'QUERY_STORE',
    @MopUpPass = N'Y',
    @LogToTable = N'Y';

-- v3 (new) -- same behavior, fewer params
EXEC dbo.sp_StatUpdate
    @Preset = N'NIGHTLY',
    @Databases = N'USER_DATABASES';
```

## Version History

- **3.7.1.2026.07.23** - **QS score cache value + identity fix** (gh-556): the gh-503 CommandLog-backed Query Store score cache had three defects.  (1) Phase 6 zero-initialized `qs_priority_boost` for ALL candidates while live enrichment skipped cache hits -- every hit silently ranked as 0 (debug output still reported a fresh hit).  Zero-init now applies to cache misses only, and the WAITS/WAIT_CPU wait-stats UPDATEs gain the same cache-hit exclusion.  (2) The cache lookup never compared the cached `QSMetric` -- a boost computed under CPU could be reused for a DURATION/WAITS run, violating the gh-503 invalidation contract.  The lookup now requires cached metric = current metric (filtered inside the CTE, so an older same-metric row still hits after a metric flip-flop); legacy rows without a `QSMetric` element are cache misses.  (3) The cache key was DatabaseName + ObjectName + StatisticsName -- same-named table+stat pairs in different schemas shared scores.  `SchemaName` is now part of the join key.
- **Diag 2026.07.23.2** - **Empty-CommandLog single-result envelope** (gh-557): with `@SingleResultSet = 1` and no usable CommandLog data, the early-return branch previously emitted only ResultSetIDs 1-2, silently breaking the advertised stable-ID contract (IDs 1-13 always present).  IDs 3-13 now each carry one marker row whose `RowData` JSON contains `DataStatus = "UNAVAILABLE"`, `Reason = "NO_COMMANDLOG_DATA"`, and the `ResultSetName` -- automation can distinguish unavailable source data from zero findings without message parsing (consumers key on the `DataStatus` property, never on text).  IDs 1-2 keep their first-run content; the multi-result-set empty path (gh-304 placeholders) is unchanged.  Also fixed: the empty-path ID 2 `RowData` subquery was uncorrelated, concatenating all recommendation rows into one invalid JSON string per row -- now correlated per finding with sequential `RowNum`, matching the normal-path RS 2 shape.
- **Diag 2026.07.23.1** - **C4 adjacent-window fix** (gh-558): the DEGRADING_THROUGHPUT check now compares the recent `@ThroughputWindowDays` against the *immediately preceding* window of equal length.  Previously the PRIOR window had no lower bound, so every loaded run outside the recent window entered the baseline -- with defaults that meant 7 recent days vs 23 prior days, and raising `@DaysBack` silently changed C4 results.  Runs older than 2 x `@ThroughputWindowDays` are now excluded from C4 entirely, and validation requires `@DaysBack` >= 2 x `@ThroughputWindowDays` (breaking: calls with `@DaysBack` below twice the window -- e.g. `@DaysBack = 7` with the default 7-day window -- now fail validation; pass a smaller `@ThroughputWindowDays`).
- **3.7.0.2026.07.03** - QS attribution + guardrail batch.  (1) **Forced-plan failure attribution** (gh-533): before touching any stat, the proc snapshots `force_failure_count` for every forced Query Store plan per database (`MANUAL`-only on SQL 2022+ so Automatic Plan Correction noise is excluded); after the run it computes the delta.  Rising counts emit a `QS_FORCED_PLAN_FAILURE_DELTA` warning naming per-database deltas, and the END Summary XML gains a `ForcedPlanFailureDelta` element (omitted when zero) — the DBA can finally attribute "forced plan started failing overnight" to the stats job or rule it out.  Databases whose baseline capture failed transiently are excluded from the delta so pre-existing failures are never misattributed.  (2) **`@QueryStore = WAIT_CPU`** (gh-534): prioritizes by CPU-category waits only (`sys.query_store_wait_stats` `wait_category = 1`), complementing `WAITS` (all stat-influenceable categories).  (3) **Budget-overshoot guard** (gh-539): before each stat, estimated duration (MAX of the last 10 successful CommandLog updates — one 4-hour outlier must count, so not AVG) is checked against the remaining `@TimeLimit`/`@StopByTime` budget; stats that cannot fit are deferred instead of blowing through the window.  (4) **Denial artifact** (gh-551): a run refused with `ALREADY_RUNNING` now writes a durable `SP_STATUPDATE_DENIED` CommandLog row identifying the denied session and the lock holder (session, login time, lock acquire time); suppressed with `@LogToTable = N'N'`.  (5) **Filtered-drift proof surface** (gh-554): `FILTERED_DRIFT` now outranks `QUERY_STORE_PRIORITY` in `QualifyReason`, and `@FilteredStatsMode` / `@FilteredStatsStaleFactor` are public parameters again — the v3.0 API collapse had absorbed them with no preset exposing EXCLUDE/ONLY/PRIORITY, leaving those modes and the FILTERED_DRIFT sort order unreachable.  (6) **Preset contract docs** (gh-553): `@Help` preset topic rewritten as a bounded override contract listing the supported override set.  (7) IO-corruption warning deduped via flag; fresh-stat physical row counts aggregated once per object.
- **Diag 2026.07.03.1** - **I8/RS13 DataStatus** (gh-532): the QS Performance Correlation surfaces no longer go silently empty on sparse Query Store history.  Sparse I8 findings carry a machine-readable `[DataStatus: AVAILABLE|INSUFFICIENT_HISTORY|UNAVAILABLE; QSCpuRuns: N; TrackedStats: N]` suffix, and RS 13 returns a single `[NO DATA]` sentinel row (full column list, reason text explaining what to enable) instead of zero rows whenever status is not AVAILABLE — including the empty-CommandLog first-run path and `@SingleResultSet` mode.
- **Diag 2026.07.02.2** - Phase-C diagnostic batch.  **@CriticalTables coverage**: per-stat `IsCritical`/`CriticalSampleOverride` and the three `Critical*` parameters are now ingested; new **W16 CRITICAL_TABLES_NO_MATCH** (configured pattern matched zero stats -- silent misconfiguration) and **I17 CRITICAL_TABLES_COVERAGE** (leadership summary with avg processing position proving `@CriticalTablesFirst` works).  **Version-aware parameter gating** (gh-520): runs from sp_StatUpdate < 2.15 (pre full-parameter-logging) are excluded from W1/I4 parameter reasoning -- on newer runs an absent element genuinely means NULL, so those checks stay sharp without false findings on old rows.  **Executive Dashboard TIME_LIMIT annotation** (gh-521): when time-limit exhaustion crosses the C3 threshold, COMPLETION gains a `[!] budget-capped, not workload-complete` banner + CompletionReason detail (annotation only -- scores/grades unchanged).  **History watermark** (gh-524): `StatUpdateDiagHistory` gains `MaxStartTime` (purge/archive-safe) alongside the legacy ID watermark; RunLabel dedup guard unchanged.  **Hardening**: all three `#stat_updates` ingestion builds start with a `CONVERT(nvarchar(max), ...)` operand -- sub-4000-char literal pieces previously made intermediate concats `nvarchar(4000)` and silently truncated the assembled batch (the direct path lost its WHERE tail and ingested zero rows); cache CREATE brought into lockstep with its upgrade ALTERs.
- **3.6.0.2026.07.02** - Telemetry contract batch.  (1) **`WarningsCodes` in END Summary XML**: the run's pipe-delimited warning codes (same tokens as `@WarningsCodesOut`) now land in the `SP_STATUPDATE_END` row's `Summary/WarningsCodes` element on all five live END writers (four early-return paths + normal finalize), so CommandLog consumers are no longer blind to runtime warnings.  (2) **StopReason fix**: `COMPLETED_WITH_SKIPPED_DBS` is applied before the END row is written — CommandLog previously always recorded plain `COMPLETED`.  (3) **`CONSECUTIVE_FAILURES_ELEVATED`**: new peak tracker emits this code when consecutive failures reached >= 3 mid-run without hitting the bailout; `@Help` WARNINGS topic now documents the full format contract (every code token enumerated; codes may be added, never renamed/removed without a major version bump).  (4) **`AZURE_SQL_EDGE`**: EngineEdition 9 gets its own platform message + warning code.  (5) **Operator polish**: per-stat progress line gains a wall-clock + ETA suffix (once 5 stats have completed); startup advisory when a 0-1s `@LockTimeout` is combined with `@MaxConsecutiveFailures = 1` (one lock wait would abort the run).
- **Diag 2026.07.02.1** - Phase-B diagnostic batch.  Skip markers (`SKIPPED:` / `TOCTOU_SKIP`) are excluded from `#stat_updates` ingestion, so they no longer pollute durations, appearance counts, or top-tables (NULL-safe predicate).  `Summary/StatsToctou` and `Summary/QSEnrichmentSkipped` are now consumed directly (W5/I9b use the definitive signal with inference as fallback for old rows).  I4 UNUSED_FEATURES catalog refreshed (`@FirstTimeFullScanCapRows`, `@CriticalTables`, `@AbortOnIntegrityError` — the latter surfaces when IO_CORRUPTION appears in StopReason *or* WarningsCodes).  Dead v2 `GroupByJoinPattern` ingestion removed.  Two new checks: **W14 RECURRING_SAFETY_STOP** (same safety StopReason — TEMPDB_PRESSURE, AG_REDO_QUEUE, LOG_SPACE_HIGH, IO_CORRUPTION, FAIL_FAST, CONSECUTIVE_FAILURES, BATCH_LIMIT — in >= 2 runs) and **W15/I16 RECURRING_RUNTIME_WARNING** (warning codes recurring across runs, severity-tiered environmental vs informational; consumes the new `Summary/WarningsCodes` element; older runs without the element produce no false findings).
- **3.5.9.2026.07.02** - Three fixes.  (1) **Fresh statistics finally qualify** (gh-550, cf. Ola IndexOptimize #990): statistics that had never been computed (`last_updated IS NULL`, `modification_counter IS NULL`) over populated tables were silently never updated — the Phase 4 outer gate `effective_counter > 0` excluded them from every qualification branch, including the `@StaleHours` rescue.  Fresh candidates now get a targeted `sys.partitions` physical row count (fresh stats only — no blanket join) and a NEVER_UPDATED rescue branch in Phase 4; `QualifyReason` reports `NEVER_UPDATED`.  Fresh stats on empty tables still do not qualify.  (2) **New `@AbortOnIntegrityError bit = 1`** (gh-549): default 1 preserves the existing abort-on-823/824/825 behavior; 0 emits the CHECKDB advisory + `IO_CORRUPTION` warning code (deduped), counts the stat as failed, and continues (`@FailFast = 1` still aborts regardless).  TOCTOU CATCH skips (208/15009/2767) now close their pre-inserted CommandLog row (EndTime + `TOCTOU_SKIP` message; ErrorNumber intentionally NULL so diag failure aggregations ignore them) instead of leaving it dangling like a killed stat.  (3) **`@QueryStore = WAITS` category fix** (gh-544): the wait filter used `wait_category IN (3, 12, 15)` = Lock/Preemptive/Network IO — classes statistics updates cannot influence — while its comment claimed Buffer IO/Memory/SortAndTempDb.  Corrected to `5, 6, 17, 21` (Buffer Latch, Buffer IO, Memory, Other Disk IO) and centralized into one documented `@qs_wait_categories` variable so the three consumer sites can no longer drift.
- **3.5.8.2026.06.12** - Telemetry bug fix: four mid-run early returns (`NO_QUALIFYING_STATS`, `FINGERPRINT_CONFLICT`, `MAX_WORKERS`, `QUEUE_INIT_ERROR`) exited after the `SP_STATUPDATE_START` insert without ever writing an `SP_STATUPDATE_END` row.  Healthy runs were orphaned: the diag's C1 check raised false KILLED criticals, orphan cleanup later fabricated END rows with `StopReason=KILLED` (permanently recording healthy no-op runs as kills), and the RELIABILITY grade was poisoned.  All four paths now write a real END row with full Summary XML and the true StopReason.  Also fixed: the `FINGERPRINT_CONFLICT` refusal was dead code -- its severity-16 RAISERROR fired inside a TRY whose CATCH was empty, so the documented refuse-the-join semantics (`RETURN 50001`, ERROR result set) never executed and conflicting workers silently joined the queue anyway.  The conflict now flags inside the TRY and exits via a label outside all TRY scopes, so the error surfaces to the caller AND the bookkeeping runs (sp_StatUpdate-rse2).
- **Diag 2026.06.12.1** - Silent no-op parallel run detection, validated against the v31y zombie-queue incident signature.  New CRITICAL check **C6 SILENT_NOOP_RUN**: flags runs that self-report normal completion (`PARALLEL_COMPLETE`/`COMPLETED`) with StatsFound > 0 but zero stats processed across the worker window -- no overlapping worker processed anything and no `UPDATE_STATISTICS` activity exists in the window.  W3 STALE_BACKLOG is un-suppressed when the C6 signature is present, which also closes the COMPLETION grade-A fast path for zombie joiners reporting StatsRemaining = 0.  `CompletionPct` no longer trusts `PARALLEL_COMPLETE` blindly: with StatsRemaining > 0 it cross-checks the worker group (overlapping parallel runs) and scores SUM(processed)/MAX(found) instead of a constant 100.  Genuinely-drained fleets and healthy late joiners keep their 100% (no 2026.05.28.1/3j5l regression) (sp_StatUpdate-kzx0).
- **3.5.7.2026.06.12** - Parallel bug fix: queue leadership re-claim no longer blocked by recycled SPIDs or idle pooled leader sessions.  The claim UPDATE judged leader/worker liveness by bare `session_id` in `sys.dm_exec_sessions`, so once a drained queue's old SessionID was reused by any live session, every new worker with the same `@parameters_string` joined the completed queue, claimed nothing, and exited `PARALLEL_COMPLETE` with StatsFound > 0 but StatsProcessed = 0 -- a silent no-op maintenance run.  Liveness is now login_time-qualified (a genuine claimer logged in before claiming; a recycled SPID logged in after), same-SPID rows are unconditionally stale, a fully drained queue is re-claimable regardless of leader session state, and the active-worker guard is scoped to open claims (`TableEndTime IS NULL`).  Found by the ConcurrentParallel P3 gate test on a fresh container (sp_StatUpdate-v31y).
- **3.5.6.2026.06.10** - Code-review batch.  (1) Parallel fingerprint conflict check hoisted out of the queue-creator branch so queue JOINERS are validated too -- previously a worker that found an existing queue bypassed the `@stored_fingerprint` comparison entirely, letting fingerprint-only params (critical-table, mop-up) differ silently within one queue (sp_StatUpdate-r77t).  (2) CONTEXT_INFO restored on all 7 early-return paths so pooled sessions no longer keep a stale `sp_StatUpdate|...` tag after early exits (sp_StatUpdate-orec).  (3) LOW batch: dead `/StatInfo` XPath limb removed from CommandLog readers, `@FirstTimeFullScanCapRows` now also intercepts the RESAMPLE-of-persisted-100 path, re-entrancy guard comment corrected (sp_StatUpdate-mc03).  Test gate expanded to 6 suites (parallel-mode + partitioned-incremental promoted); diag suite de-tautologized; V3Fixes rewritten to behavioral assertions (sp_StatUpdate-8gzw).
- **Diag 2026.06.10.1** - @Help checks catalog completed with the 10 emitted-but-undocumented checks: W7 HIGH_IMPACT_STATS_DEPRIORITIZED, W8 MOPUP_INEFFECTIVE, W9 LOCK_TIMEOUT_INEFFECTIVE, W10 PARAMETER_CHURN, C5 SAMPLE_RATE_DEGRADATION, I9 WORKLOAD_CONCENTRATION, I11 FAILURE_CLUSTERING, I12 QS_COVERAGE_DRIFT, I13 PARALLEL_OPPORTUNITY, I14 MOPUP_MISSING_PAGECOUNT.  No behavior change to the checks themselves (sp_StatUpdate-8gzw).
- **3.5.5.2026.06.08** - Two fixes: (1) false `ALREADY_RUNNING` on consecutive same-session calls after a validation-error exit.  Caller TRY/CATCH catches the proc's severity-16 validation RAISERROR before cleanup, leaving a stale lock row.  Three-part re-entrancy guard: same-SPID is unconditionally stale, idle > 60s without active request is dead, AcquiredAt > 8h is dead (sp_StatUpdate-i7by).  (2) Diag registers `QS_PARALLEL_UNRELIABLE` in the valid-categories catalog as INFO I9b (sp_StatUpdate-g4c0).
- **Diag 2026.06.08.2** - Four W5 / I10 refinements following v3.5.4 parallel priority fix.  W5 now parallel-aware: emits softer INFO `QS_PARALLEL_UNRELIABLE` instead of WARNING when ALL QS runs use `@StatsInParallel='Y'` but QS CPU data IS observed.  I10 W5-conditional: keeps `@SortOrder=QUERY_STORE` and `@QueryStore=CPU` when W5 fired as parallel artifact, not genuine QS absence.  TimeLimit jitter normalization rounds <300s variance to nearest minute for clean recommended values.  `@CriticalTables` hint appended to I10 when W9 `LOCK_TIMEOUT_INEFFECTIVE` has fired (sp_StatUpdate-6x80).
- **3.5.4.2026.06.08** - Two fixes: (1) Parallel TablePriority population had LPT (`COALESCE(est_total_seconds, 0) DESC`) as PRIMARY sort, silently overriding user-intent min_priority rank.  Swapped ORDER BY so min_priority is PRIMARY and LPT is the tiebreaker -- QS-top stats now land at ProcessingPosition 1-10 instead of 285-296 on mature fleets (sp_StatUpdate-mhje).  (2) New `@FirstTimeFullScanCapRows bigint = NULL` parameter: stats with row count above threshold AND no successful CommandLog entry within retention get FULLSCAN capped to sampled rate using adaptive sampling formula (min 1%).  Critical-table override still wins.  Addresses _WA_Sys stats on 770M-15.9B row tables with ~3085s FULLSCANs on first encounter (sp_StatUpdate-mknv).
- **Diag 2026.06.08.1** - Four W5 / I10 refinements following v3.5.4 mhje fix.  W5 parallel-aware: when ALL QS-priority runs use `@StatsInParallel='Y'` and QS CPU data IS observed in stat updates, emits softer INFO `QS_PARALLEL_UNRELIABLE` instead of WARNING.  Per-worker ProcessingPosition counters made the original signal unreliable in parallel mode.  I10 W5-conditional: only reverts `@SortOrder=MODIFICATION_COUNTER` and clears `@QueryStore` when W5 fired due to genuine QS absence, not parallel artifact.  TimeLimit jitter normalization: when MAX-MIN of TimeLimit across recent non-killed runs is <300s, rounds to nearest minute for clean values like 21600.  `@CriticalTables` hint: appends commented-out suggestion with top 3 contended tables when W9 `LOCK_TIMEOUT_INEFFECTIVE` fires (sp_StatUpdate-6x80).
- **3.5.3.2026.0529** - Bug fix: `@total_stats` was assigned by a CASE inside a multi-variable DECLARE whose parallel branch reads the optional `dbo.QueueStatistic` table.  A CASE in a DECLARE is a single always-compiled statement, so SQL Server name-resolved the QueueStatistic branch even on non-parallel runs and failed with Msg 208 when the table was absent.  `@total_stats` is now assigned via IF/ELSE so the reference resolves only in parallel mode where the table exists (sp_StatUpdate-isa2).
- **3.5.2.2026.0528** - Bug fix: orphaned-run cleanup now bounds the orphan START candidate hunt to the same window as `#orphan_end_labels`, so a long-completed parallel run whose real END predated the window no longer receives a fabricated KILLED end -- which had been falsely flagging healthy runs as killed (sp_StatUpdate-trla).
- **Diag 2026.05.28.1** - Two parallel-mode false-positive fixes: the run dedup tiebreaker now keeps the real END over a synthetic KILLED end (sp_StatUpdate-772k), and completion percentage recognizes the PARALLEL_COMPLETE stop reason instead of pinning parallel runs at 100/workers % (sp_StatUpdate-3j5l).  Together these had produced fleet-wide false RELIABILITY F grades.
- **3.5.1.2026.0507** - Failed UPDATE STATISTICS rows now write ExtendedInfo with RunLabel + full discovery context to CommandLog, so sp_StatUpdate_Diag's C2 (REPEATED_FAILURES) check no longer silently misses failures.  TOCTOU rows (errors 208, 15009, 2767) keep NULL ExtendedInfo by design (gh-515).
- **3.5.0.2026.0423** - `@CriticalTables` feature: per-table sample-rate override with optional priority boost.  Three new params -- `@CriticalTables` (comma-delimited patterns with `%` wildcards), `@CriticalSamplePercent` (forces sample rate on critical tables only), and `@CriticalTablesFirst` (processes them before everything else regardless of `@SortOrder`).  PERSIST_SAMPLE_PERCENT auto-enabled for critical tables (gh-508).
- **3.4.1.2026.0423** - `@LockTimeout` promoted to a public parameter, overriding preset `@i_lock_timeout` (-1 = wait forever, 0 = no wait, N > 0 = seconds).  Addresses Error 1222 lock-timeout failures (gh-508).
- **3.4.0.2026.0422** - CommandLog intelligence: Phase 3B qualifies on ModificationCounter delta instead of the raw counter; delta=0 stats skip qualification.  New MODIFICATION_VELOCITY sort order ranks by mods/hour.  Phase 5B caches QS scores from CommandLog so Phase 6 skips expensive QS DMV joins for stats with fresh cached scores (gh-502, 503, 507).
- **3.3.5.2026.0422** - QS forced plan check replaces the CHARINDEX scan with a sql_expression_dependencies integer join for compiled objects (ad-hoc fallback retained).  Parallel lead-worker gate prevents N-worker redundancy.  Parallel progress now shows the global QueueStatistic total as denominator + GlobalStatsDone in ExtendedInfo (gh-498, 499, 500).
- **3.3.4.2026.0420** - Bug fix: removed the server-level AG-secondary hard-error from parallel pre-flight that regressed v2 behavior of silently skipping unreadable AG databases and proceeding against the remainder.  Kept the orphan QueueStatistic backlog warning (gh-428 follow-up).
- **3.3.1.2026.0420** - Deploy fix: a pre-ALTER-PROCEDURE migration adds `ClaimLoginTime` + `LastStatCompletedAt` to an existing `dbo.QueueStatistic`, so upgrades from v2.26 or earlier no longer fail compilation with "Invalid column name".  Idempotent; no-op on fresh installs.
- **3.3.0.2026.0418** - Additive enhancements (5 issues): `@JobName` context tag written to CONTEXT_INFO (gh-423), `@DeadWorkerTimeoutMinutes` NULL coercion + stale-row sweep (gh-425), `@WarningsCodesOut` OUTPUT param with stable code tokens (gh-426), `@SkipTablesWithNCCI` / `@SkipTablesWithCCI` per-type columnstore controls (gh-427), and parallel pre-flight validation (gh-428).
- **3.2.2.2026.0417** - Internal refactors (behavior-preserving): `@parameters_string` built once in region 07B (gh-461), mop-up WHERE filter extracted to `@mop_up_where_sql` (gh-462), and the empty-schema SELECT extracted to `@empty_disc_select` for all 6 staged-discovery bailout paths (gh-465).
- **3.2.1.2026.0417** - Phase 5 bug fix: DIRECT_STRING discovery path (`@Statistics` param) now honors `@TargetNorecompute` and `@ExcludeStatistics` filters, matching staged discovery Phase 1 behavior (gh-492).
- **3.2.2026.0417** - Phase 4 quality/perf batch (8 issues): Phase 6 plan feedback bounded by time + top-N, 5 COUNT_BIG scans collapsed to one SUM(CASE), orphan cleanup materializes END labels, per-table sys.partitions cache, per-database warning block skipped when DB has zero stats, MAX_GRANT_PERCENT token-substituted, 6 empty CATCH blocks surface to `@WarningsOut`, threshold-logic explanation gated behind `@Debug = 1` (gh-460, 463, 464, 466-470).
- **3.1.2026.0417** - Phase 1 correctness batch (9 issues): parallel early-return paths set OUTPUT params + summary result set, LOCK_TIMEOUT restored after forced-plan check, `@QueryStore = AVG_CPU` sorts by average (not total), `@parameter_fingerprint` + additional correctness fixes (gh-451..459). Also Phase 2/3 diag correctness (10 issues) in sp_StatUpdate_Diag 2026.04.17.1 (gh-471..480).
- **3.0.2026.0407** - v3 architecture: preset-first API.  33 input params (was 58 in v2) + 10 OUTPUT params.  25 params absorbed into `@i_` internal variables controlled by presets (DEFAULT, NIGHTLY, WEEKLY_FULL, OLTP_LIGHT, WAREHOUSE).  New `@QueryStore` param replaces `@QueryStorePriority` + `@QueryStoreMetric`.  `@StaleHours` replaces `@DaysStaleThreshold` + `@HoursStaleThreshold`.  Table-driven validation, 6-phase staged discovery only (no legacy fallback), unified mop-up filters.  Full behavioral parity with v2.37.
- **2.37.2026.0327** - WAITS enrichment unbounded XML parsing fix (metric gate + TopPlans limit), Phase 6 debug gate.
- **2.35.2026.0327** - @QueryStoreMetric WAITS + diag memory grant trending.
- **2.34.2026.0326** - QS discovery metric gaps: 6 issues resolved.
- **2.29.2026.0325** - Em dashes, AG sync-only redo, mop-up safety + 8 new diag checks (W8-W10, C5, I11-I14).
- **2.24.2026.0324** - Staged discovery hardening, legacy QS consistency, @MopUpPass, @MopUpMinRemainingSeconds.
- **2.23.2026.0324** - @QueryStoreTopPlans (Phase 6 XML parsing limit), early bail-out.
- **2.22.2026.0320** - AscendingKeyBoost, CE QUERY_OPTIMIZER_HOTFIXES, APC awareness, cursor-to-set-based.
- **Diag 2026.03.23.1** - I10 RECOMMENDED_CONFIG, @ExpertMode, Executive Dashboard A-F grades, persistent history.
- **2.16.2026.0308** - QS Efficacy Trending, ProcessingPosition, diag RS 9-13, I6/I7/I8.
- **2.14.2026.0304** - Bulk issue resolution (42 issues). @OrphanedRunThresholdHours. AG/safety guards.
- **2.8.2026.0302** - Comprehensive sweep (31 issues). @IncludeIndexedViews, @LogSkippedToCommandLog, QS forced plan warning.
- **2.7.2026.0302** - AG redo queue pause, tempdb pressure check.
- **2.4.2026.0302** - Region markers, collation-aware comparisons, per-phase timing, 12-issue bug-fix sprint.
- **2.0.2026.0212** - Environment Intelligence, staged discovery, diagnostic tool (sp_StatUpdate_Diag).
- **1.9.2026.0206** - Status/StatusMessage columns, batch QS enrichment.
- **1.5.2026.0120** - CRITICAL: Fixed @ExcludeStatistics filter, incremental partition targeting.
- **1.4.2026.0119** - Query Store prioritization, filtered stats handling.
- **1.3.2026.0119** - Multi-database support, OUTPUT parameters, return codes.
- **1.0.2026.0117** - Initial public release.

## When to Use This (vs IndexOptimize)

**IndexOptimize** is battle-tested and handles indexes + stats together. Use it for general maintenance.

**sp_StatUpdate** is for when you need:
- Priority ordering (worst stats first)
- Time-limited runs with graceful stops
- NORECOMPUTE targeting
- Query Store-driven prioritization (10 metrics, tunable plan parsing)
- Adaptive sampling for problematic stats
- AG-safe maintenance with redo queue awareness
- Mop-up pass for thorough coverage
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
