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

## Common Usage Patterns

### Nightly Maintenance (All Stats)

```sql
EXEC dbo.sp_StatUpdate
    @Databases = N'USER_DATABASES',     -- All user databases
    @TargetNorecompute = N'BOTH',
    @TimeLimit = 18000,                  -- 5 hours
    @SortOrder = N'MODIFICATION_COUNTER';
```

### Multi-Database with Exclusions

```sql
-- All user databases except dev/test
EXEC dbo.sp_StatUpdate
    @Databases = N'USER_DATABASES, -DevDB, -TestDB',
    @TargetNorecompute = N'BOTH',
    @TimeLimit = 14400;

-- Databases matching pattern
EXEC dbo.sp_StatUpdate
    @Databases = N'%Prod%',
    @TimeLimit = 7200;
```

### Query Store Prioritization (v1.4+)

```sql
-- Prioritize stats on tables with highest CPU consumption
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @QueryStorePriority = N'Y',
    @QueryStoreMetric = N'CPU',          -- CPU, DURATION, READS, or EXECUTIONS
    @QueryStoreRecentHours = 48,         -- Last 48 hours of query activity
    @SortOrder = N'QUERY_STORE',
    @TimeLimit = 3600;
```

### Adaptive Sampling for Large Tables (v1.6+)

```sql
-- Stats that historically took >4 hours get 5% sample instead
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @LongRunningThresholdMinutes = 240,
    @LongRunningSamplePercent = 5,
    @TimeLimit = 18000;
```

### Target NORECOMPUTE Orphans Only

```sql
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @TargetNorecompute = N'Y',
    @ModificationThreshold = 100000,
    @BatchLimit = 50,
    @TimeLimit = 1800;
```

### Skip Auto-Created Stats

```sql
-- Focus on user-defined stats, skip _WA_Sys% auto-created
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @ExcludeStatistics = N'_WA_Sys%',
    @SortOrder = N'AUTO_CREATED',        -- User-created first
    @TimeLimit = 3600;
```

### Dry Run with Command Output

```sql
-- Preview commands and save to table
EXEC dbo.sp_StatUpdate
    @Databases = N'Production',
    @Execute = N'N',
    @WhatIfOutputTable = N'#WhatIfCommands',
    @Debug = 1;

SELECT * FROM #WhatIfCommands ORDER BY SequenceNum;
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
| `@ProgressLogInterval` | `NULL` | Log progress every N stats |
| `@Debug` | `0` | `1` = verbose diagnostic output |

### Parallel Mode

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@StatsInParallel` | `'N'` | Use queue-based parallel processing |
| `@DeadWorkerTimeoutMinutes` | `15` | Consider worker dead if no progress |

### Maintenance

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@CleanupOrphanedRuns` | `'N'` | Mark killed runs with END markers |
| `@Help` | `0` | `1` = show parameter documentation |

Run `EXEC sp_StatUpdate @Help = 1` for complete parameter documentation.

## Monitoring

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

## Troubleshooting

An Extended Events session is included for runtime troubleshooting:

```sql
-- Create the XE session (see tools/sp_StatUpdate_XE_Session.sql)
-- Start monitoring before running sp_StatUpdate
ALTER EVENT SESSION [sp_StatUpdate_Monitor] ON SERVER STATE = START;

-- Run sp_StatUpdate...

-- View captured events (queries included in the XE script)
```

The session captures UPDATE STATISTICS commands, errors, lock waits, and long-running statements.

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
