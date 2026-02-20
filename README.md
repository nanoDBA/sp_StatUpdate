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
| Azure DTU/vCore concerns | Auto-detects Azure SQL, warns about resource impact |

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
| **Azure SQL** | Database, Managed Instance, and Edge supported (v2.1+) |
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

### High-Churn Tables

```sql
-- Target specific tables with lock timeout
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @Tables = N'Sales.Orders, Sales.OrderDetails',
    @ModificationThreshold = 100000,
    @LockTimeout = 5,                     -- Skip if blocked after 5 sec
    @TimeLimit = 600;
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

## Parameter Reference

Run `EXEC sp_StatUpdate @Help = 1` for complete documentation.

### Database & Table Selection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@Databases` | Current DB | `USER_DATABASES`, `SYSTEM_DATABASES`, `ALL_DATABASES`, `AVAILABILITY_GROUP_DATABASES`, wildcards (`%Prod%`), exclusions (`-DevDB`) |
| `@Tables` | All | Table filter (comma-separated `Schema.Table`) |
| `@ExcludeTables` | `NULL` | Exclude tables by pattern (`%Archive%`) |
| `@ExcludeStatistics` | `NULL` | Exclude stats by pattern (`_WA_Sys%`) |
| `@IncludeSystemObjects` | `'N'` | Include stats on system objects |

### Threshold Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@TargetNorecompute` | `'BOTH'` | `'Y'`=NORECOMPUTE only, `'N'`=regular only, `'BOTH'`=all |
| `@ModificationThreshold` | `5000` | Minimum modifications to qualify |
| `@TieredThresholds` | `1` | Use Tiger Toolbox 5-tier adaptive formula |
| `@ThresholdLogic` | `'OR'` | `'OR'`=any threshold, `'AND'`=all must be met |
| `@DaysStaleThreshold` | `NULL` | Minimum days since last update |
| `@MinPageCount` | `0` | Minimum pages (125000 = ~1GB) |

### Execution Control

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@TimeLimit` | `3600` | Max seconds (1 hour) |
| `@BatchLimit` | `NULL` | Max stats per run |
| `@MaxConsecutiveFailures` | `NULL` | Stop after N consecutive failures (v2.1+) |
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

### Query Store Integration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@QueryStorePriority` | `'N'` | Prioritize stats used by Query Store plans |
| `@QueryStoreMetric` | `'CPU'` | `CPU`, `DURATION`, `READS`, or `EXECUTIONS` |
| `@QueryStoreMinExecutions` | `100` | Minimum plan executions to boost |
| `@QueryStoreRecentHours` | `168` | Only consider plans from last N hours |
| `@GroupByJoinPattern` | `'Y'` | Update joined tables together (prevents optimization cliffs) |

### Adaptive Sampling

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@LongRunningThresholdMinutes` | `NULL` | Stats that took longer get forced sample rate |
| `@LongRunningSamplePercent` | `10` | Sample percent for long-running stats |
| `@StatisticsSample` | `NULL` | `NULL`=SQL Server decides, `100`=FULLSCAN |
| `@PersistSamplePercent` | `'N'` | Add PERSIST_SAMPLE_PERCENT (SQL 2016 SP1 CU4+) |

### Logging & Output

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@LogToTable` | `'Y'` | Log to dbo.CommandLog |
| `@ProgressLogInterval` | `NULL` | Log progress every N stats |
| `@Debug` | `0` | `1` = verbose diagnostic output |

### OUTPUT Parameters (v2.1+)

| Parameter | Description |
|-----------|-------------|
| `@StatsFoundOut` | Total qualifying stats discovered |
| `@StatsProcessedOut` | Stats attempted (succeeded + failed) |
| `@StatsSucceededOut` | Stats updated successfully |
| `@StatsFailedOut` | Stats that failed to update |
| `@StatsRemainingOut` | Stats not processed (time/batch limit) |
| `@DurationSecondsOut` | Total run duration in seconds |
| `@WarningsOut` | Collected warnings (LOW_UPTIME, BACKUP_RUNNING, AZURE_SQL) |
| `@StopReasonOut` | Why execution stopped (COMPLETED, TIME_LIMIT, CONSECUTIVE_FAILURES, etc.) |

## Monitoring

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

### Programmatic Access (v2.1+)

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
    PRINT 'Alert: Statistics maintenance had failures';

IF @Warnings LIKE '%BACKUP_RUNNING%'
    PRINT 'Note: Backups were running during maintenance';
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

**sp_StatUpdate_Diag** analyzes CommandLog history and produces actionable recommendations.

```sql
-- Basic diagnostic (last 30 days)
EXEC dbo.sp_StatUpdate_Diag;

-- Obfuscated for external sharing (hashes names)
EXEC dbo.sp_StatUpdate_Diag @Obfuscate = 1;
```

### Multi-Server (PowerShell)

```powershell
.\Invoke-StatUpdateDiag.ps1 `
    -Servers "Server1", "Server2,2500", "Server3" `
    -CommandLogDatabase "Maintenance" `
    -OutputPath ".\diag_output"
```

### Diagnostic Checks

| Severity | Checks |
|----------|--------|
| CRITICAL | Killed runs, repeated stat failures, time limit exhaustion, degrading throughput |
| WARNING | Suboptimal parameters, long-running stats, stale-stats backlog, overlapping runs |
| INFO | Run health trends, parameter history, top tables by cost, version history |

## Extended Events

An XE session is included for runtime troubleshooting:

```sql
-- Create and start (see sp_StatUpdate_XE_Session.sql)
ALTER EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER STATE = START;
```

Captures UPDATE STATISTICS commands, errors, lock waits, lock escalation, and long-running statements.

## Version History

- **2.1.2026.0219** - Phase 2: Extended Awareness. Azure SQL detection with DTU/vCore warning. Hardware context (CPU/memory/NUMA/uptime). RCSI awareness. Replication/CDC/Temporal table detection (progress shows REPLICATED, CDC, TEMPORAL flags). Backup activity warning. New parameters: @MaxConsecutiveFailures, @WarningsOut OUTPUT, @StopReasonOut OUTPUT. SYSDATETIME() consistency throughout.
- **2.0.2026.0212** - Phase 1: Environment Intelligence. CE version/compat level, trace flag detection, DB-scoped config detection. Staged discovery auto-fallback. Truncated partition handling. XE session overhaul.
- **1.9.2026.0206** - Status/StatusMessage columns for Agent alerting. Batch Query Store enrichment (O(n) to O(1)).
- **1.9.2026.0128** - @Preset parameter, @GroupByJoinPattern, ##sp_StatUpdate_Progress, @CleanupOrphanedRuns default Y.
- **1.8.2026.0128** - Code review fixes, XE troubleshooting session.
- **1.7.2026.0127** - BREAKING: @ModificationThreshold default 1000 → 5000.
- **1.6.2026.0128** - Staged discovery, adaptive sampling, @ExcludeTables, @WhatIfOutputTable.
- **1.5.2026.0120** - CRITICAL: Fixed @ExcludeStatistics filter, incremental partition targeting.
- **1.4.2026.0119** - Query Store prioritization, filtered stats handling.
- **1.3.2026.0119** - Multi-database support, OUTPUT parameters, return codes.
- **1.2.2026.0117** - Tiger Toolbox tiered thresholds, AND/OR logic, PERSIST_SAMPLE_PERCENT.
- **1.1.2026.0117** - Erik Darling style refactor, @Help, parallel mode.
- **1.0.2026.0117** - Initial public release.

## When to Use This (vs IndexOptimize)

**IndexOptimize** is battle-tested and handles indexes + stats together. Use it for general maintenance.

**sp_StatUpdate** is for when you need:
- Priority ordering (worst stats first)
- Time-limited runs with graceful stops
- NORECOMPUTE targeting
- Query Store-driven prioritization
- Adaptive sampling for problematic stats
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
