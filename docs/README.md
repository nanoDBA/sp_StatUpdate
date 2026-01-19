# sp_StatUpdate

**Priority-based statistics maintenance for SQL Server 2016+**

*Updates worst stats first.  Stops when you tell it to.  Tells you if it got killed.*

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![SQL Server 2016+](https://img.shields.io/badge/SQL%20Server-2016%2B-blue.svg)](https://www.microsoft.com/sql-server)

## Why This Exists

Alphabetical ordering means `Archive_2019` gets fresh stats while `Orders` (47M modifications) times out.  Job got killed at 5 AM? Good luck figuring out what actually ran.  Someone set `NORECOMPUTE` in 2019 and forgot? SQL Server will never auto-update those.

**sp_StatUpdate**: Worst stats first.  Time limits that work.  START/END markers so you know if it finished.

| Problem | Fix |
|---------|-----|
| Alphabetical ordering | `@SortOrder = 'MODIFICATION_COUNTER'` - worst first |
| 10-hour jobs killed at 5 AM | `@TimeLimit` - stops gracefully, logs what's left |
| "Did it finish or get killed?" | START/END markers in CommandLog |
| NORECOMPUTE orphans | `@TargetNorecompute = 'Y'` - finds and refreshes them |
| No idea what needs work | Queries `sys.dm_db_stats_properties` for modification_counter |

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
    @ModificationThreshold = 1000;     -- Min modifications to qualify
    -- Defaults: @TimeLimit=3600 (1hr), @TieredThresholds=1, @LogToTable='Y'
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

| Object      | Source                                                                     |
|-------------|----------------------------------------------------------------------------|
| `dbo.Queue` | [Queue.sql](https://ola.hallengren.com/scripts/Queue.sql) (Ola Hallengren) |

**Note**: `dbo.QueueStatistic` is auto-created on first parallel run if `dbo.Queue` exists.

**Note**: `dbo.CommandExecute` is NOT required.  sp_StatUpdate handles its own command execution.

## Common Usage Patterns

### Nightly Maintenance (All Stats)

```sql
EXEC dbo.sp_StatUpdate
    @Databases = 'Production',
    @TargetNorecompute = 'BOTH',
    @ModificationThreshold = 1000,
    @TimeLimit = 18000,
    @SortOrder = 'MODIFICATION_COUNTER';
```

### Target NORECOMPUTE Orphans Only

```sql
EXEC dbo.sp_StatUpdate
    @Databases = 'Production',
    @TargetNorecompute = 'Y',
    @ModificationThreshold = 100000,
    @BatchLimit = 50,
    @TimeLimit = 1800;
```

### High-Churn Tables (More Frequent)

```sql
-- Run every 4 hours for tables with ascending keys
EXEC dbo.sp_StatUpdate
    @Databases = 'Production',
    @Tables = 'dbo.Orders, dbo.Transactions',
    @TargetNorecompute = 'BOTH',
    @ModificationThreshold = 100,
    @TimeLimit = 900;
```

### Dry Run (Preview Only)

```sql
EXEC dbo.sp_StatUpdate
    @Databases = 'Production',
    @TargetNorecompute = 'BOTH',
    @Execute = 'N',
    @Debug = 'Y';
```

### Tiered Thresholds (v1.2+)

```sql
-- Use Tiger Toolbox 5-tier adaptive formula
-- More conservative for large tables than single SQRT threshold
EXEC dbo.sp_StatUpdate
    @Databases = 'Production',
    @TargetNorecompute = 'BOTH',
    @TieredThresholds = 1,
    @ModificationThreshold = 500,  -- Floor for very small tables
    @TimeLimit = 18000;
```

### Conservative Mode (AND Logic)

```sql
-- Only update stats that meet ALL criteria
-- Use when you want to minimize I/O on large databases
EXEC dbo.sp_StatUpdate
    @Databases = 'Production',
    @TargetNorecompute = 'BOTH',
    @ModificationThreshold = 10000,
    @DaysStaleThreshold = 7,
    @ThresholdLogic = 'AND',       -- Both conditions must be true
    @TimeLimit = 18000;
```

## Parameter Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@Databases` | Current DB | Database to process |
| `@Tables` | All | Table filter (comma-separated) |
| `@TargetNorecompute` | `'BOTH'` | `'Y'`=NORECOMPUTE only, `'N'`=regular only, `'BOTH'`=all |
| `@ModificationThreshold` | `1000` | Minimum modifications to qualify |
| `@ModificationPercent` | `NULL` | SQRT-based threshold (see below) |
| `@TieredThresholds` | `1` | Use Tiger Toolbox 5-tier adaptive formula |
| `@ThresholdLogic` | `'OR'` | `'OR'`=any threshold qualifies, `'AND'`=all must be met |
| `@DaysStaleThreshold` | `NULL` | Minimum days since last update |
| `@MinPageCount` | `0` | Minimum pages (125000 = ~1GB) |
| `@StatisticsSample` | `NULL` | `NULL`=SQL Server decides, `100`=FULLSCAN |
| `@TimeLimit` | `3600` | Max seconds (default: 1 hour) |
| `@BatchLimit` | `NULL` | Max stats per run |
| `@SortOrder` | `'MODIFICATION_COUNTER'` | Priority: `MODIFICATION_COUNTER`, `DAYS_STALE`, `PAGE_COUNT`, `RANDOM` |
| `@LogToTable` | `'Y'` | Log to dbo.CommandLog (set `'N'` if table not installed) |
| `@Execute` | `'Y'` | `'N'` for dry run |
| `@FailFast` | `0` | `1` = abort on first error |

Run `EXEC sp_StatUpdate @Help = 1` for complete parameter documentation.

### @ModificationPercent (SQRT Scaling)

Scales threshold with table size: `modification_counter >= @ModificationPercent * SQRT(row_count)`

```sql
-- 10M row table needs 31,623 modifications (not flat 1000)
EXEC dbo.sp_StatUpdate
    @Databases = 'Production',
    @ModificationPercent = 10,      -- Scales with table size
    @ModificationThreshold = 1000;  -- Floor for tiny tables
```

## Monitoring

```sql
-- Recent runs: did they finish or get killed?
SELECT
    CASE WHEN e.ID IS NOT NULL THEN 'Completed' ELSE 'KILLED' END AS Status,
    s.StartTime,
    e.ExtendedInfo.value('(/Summary/StopReason)[1]', 'nvarchar(50)') AS StopReason,
    e.ExtendedInfo.value('(/Summary/StatsProcessed)[1]', 'int') AS Processed
FROM dbo.CommandLog s
LEFT JOIN dbo.CommandLog e
    ON e.CommandType = 'SP_STATUPDATE_END'
    AND e.ExtendedInfo.value('(/Summary/RunLabel)[1]', 'nvarchar(100)') =
        s.ExtendedInfo.value('(/Parameters/RunLabel)[1]', 'nvarchar(100)')
WHERE s.CommandType = 'SP_STATUPDATE_START'
ORDER BY s.StartTime DESC;
```

## When to Use This (vs IndexOptimize)

**IndexOptimize** is battle-tested and handles indexes + stats together.  Use it for general maintenance.

**sp_StatUpdate** is for when you need priority ordering, time-limited runs, or NORECOMPUTE targeting.

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

- **1.3.2026.0119** - Code review hardening: deterministic database parsing, respects PERSIST_SAMPLE_PERCENT via RESAMPLE, TRY/CATCH around CommandLog writes, OUTPUT parameters, Agent job return codes, severity 16 for time/batch limits
- **1.2.2026.0117** - Tiger Toolbox tiered thresholds (`@TieredThresholds`), AND/OR threshold logic (`@ThresholdLogic`), version-aware PERSIST_SAMPLE_PERCENT and MAXDOP
- **1.1.2026.0117** - Erik Darling style refactor, `@Help` parameter, incremental stats, RESAMPLE, AG awareness, parallel mode, heap handling fixes
- **1.0.2026.0117** - Saner defaults: `@StatisticsSample=NULL`, `@TimeLimit=18000`
- **1.0.2026.0113** - Initial public release with versioning
- **1.0.2026.0112** - RAISERROR uniqueidentifier fix

## Acknowledgments

- [Ola Hallengren](https://ola.hallengren.com) - sp_StatUpdate wouldn't exist without his SQL Server Maintenance Solution.  We use his CommandLog table, Queue patterns, and database selection syntax.  If you're not already using his tools, start there.
- [Brent Ozar](https://www.brentozar.com) - years of Office Hours audio and videos emphasizing stats and proper fillfactor over index reorgs/rebuilds, First Responder Kit, and SQL Saturdays / many events.
- [Erik Darling](https://www.erikdarling.com) - T-SQL coding style ([CLAUDE.md](https://github.com/erikdarlingdata/DarlingData/blob/main/CLAUDE.md)) and performance insights with glorious snark.  I also love sp_LogHunter and sp_QuickieStore.
- [Colleen Morrow](https://www.sqlservercentral.com/blogs/better-living-thru-powershell-update-statistics-in-parallel) - parallel statistics maintenance concept
- [Tiger Team's AdaptiveIndexDefrag](https://github.com/microsoft/tigertoolbox) - the 5-tier adaptive threshold formula comes directly from their work
- [Claude Code](https://claude.com/product/claude-code) - AI is an amazing tool.
