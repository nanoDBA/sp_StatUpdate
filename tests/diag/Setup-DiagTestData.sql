/*
    Setup-DiagTestData.sql
    ======================
    Creates deterministic test data for sp_StatUpdate_Diag testing.
    Anchors dates to Monday of the current server week so the synthetic time
    windows remain stable without depending on DATEFIRST or the client clock.

    Scenarios seeded:
      - Healthy runs (COMPLETED, with and without time limits)
      - Killed runs (START without END)
      - Killed runs cleaned up by orphan cleanup (StopReason=KILLED)
      - Repeated failures (same stat failing across runs)
      - Degrading throughput (recent runs slower than older)
      - Overlapping runs (two runs active simultaneously)
      - Multiple versions (version skew within history)
      - Missing parameters (no TimeLimit, TieredThresholds off)
      - Long-running stats
      - Stats with Query Store data
      - Parallel runs (PARALLEL_COMPLETE, StatsInParallel=Y, with MopUp)

    Usage:
      sqlcmd -S <server> -d <database> -i Setup-DiagTestData.sql
      -- then deploy sp_StatUpdate_Diag and run tests

    NOTE: This script TRUNCATES dbo.CommandLog before inserting.
          Only run on test/dev servers.
*/
SET NOCOUNT ON;

/*
    Safety: This script TRUNCATEs dbo.CommandLog.
    Only run on test/dev servers. To add a server name check,
    uncomment and edit the block below.

    DECLARE @server_name nvarchar(256) = CONVERT(nvarchar(256), SERVERPROPERTY('ServerName'));
    IF @server_name NOT LIKE N'%test%' AND @server_name NOT LIKE N'%dev%'
    BEGIN
        RAISERROR(N'Safety: Edit the safety check in this script for your server.', 16, 1);
        RETURN;
    END;
*/

/* Create CommandLog if it doesn't exist */
IF OBJECT_ID('dbo.CommandLog') IS NULL
BEGIN
    CREATE TABLE dbo.CommandLog (
        ID int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        DatabaseName sysname NULL,
        SchemaName sysname NULL,
        ObjectName sysname NULL,
        ObjectType char(2) NULL,
        IndexName sysname NULL,
        IndexType tinyint NULL,
        StatisticsName sysname NULL,
        PartitionNumber int NULL,
        ExtendedInfo xml NULL,
        Command nvarchar(max) NOT NULL,
        CommandType nvarchar(60) NOT NULL,
        StartTime datetime2(7) NOT NULL,
        EndTime datetime2(7) NULL,
        ErrorNumber int NULL,
        ErrorMessage nvarchar(max) NULL
    );
    RAISERROR(N'Created dbo.CommandLog table', 10, 1) WITH NOWAIT;
END;

/* tracking issue: ensure the CommandType-leading index exists even when CommandLog
   already existed (this script previously created the table with no indexes, so the proc
   emitted the CommandLog index advisory #31 against this test data).  Created idempotently
   OUTSIDE the table guard and matching the advisory's recommended shape. */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.CommandLog') AND name = N'IX_CommandLog_CommandType_StartTime')
    CREATE NONCLUSTERED INDEX IX_CommandLog_CommandType_StartTime ON dbo.CommandLog (CommandType, StartTime) INCLUDE (DatabaseName, SchemaName, ObjectName, StatisticsName, EndTime, ErrorNumber);
GO

TRUNCATE TABLE dbo.CommandLog;
RAISERROR(N'CommandLog truncated', 10, 1) WITH NOWAIT;

/*
============================================================================
SCENARIO SEED DATA
All dates are relative to Monday at midnight in the server's current week.
============================================================================
*/
DECLARE @server_today date = CONVERT(date, SYSDATETIME());
DECLARE @anchor datetime2(3) = DATEADD(
    DAY,
    -(DATEDIFF(DAY, CONVERT(date, '19000101'), @server_today) % 7),
    CONVERT(datetime2(3), @server_today)
);

/* Helper variables */
DECLARE @run_label nvarchar(100);
DECLARE @start datetime2(3);
DECLARE @end_time datetime2(3);

/* =========================================================================
   RUN 1: Healthy completed run, good parameters, 10 days ago (PRIOR window)
   ========================================================================= */
SET @start = DATEADD(DAY, -10, @anchor);
SET @run_label = N'test_run01_healthy';
SET @end_time = DATEADD(SECOND, 600, @start);  /* Fast: 600s / 100 stats = 6 sec/stat */

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>1.9.2026.0206</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <TimeLimit>3600</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <ThresholdLogic>OR</ThresholdLogic>
        <SortOrder>MODIFICATION_COUNTER</SortOrder>
        <QueryStorePriority>Y</QueryStorePriority>
        <StatsInParallel>N</StatsInParallel>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>1.9.2026.0206</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>100</StatsFound>
        <StatsProcessed>100</StatsProcessed>
        <StatsSucceeded>100</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>0</StatsRemaining>
        <DurationSeconds>600</DurationSeconds>
        <StopReason>COMPLETED</StopReason>
    </Summary>');

/* Stats for run 1 - 5 healthy updates with QS data + ProcessingPosition */
DECLARE @j int = 0;
WHILE @j < 5
BEGIN
    INSERT INTO dbo.CommandLog (
        CommandType, Command, StartTime, EndTime,
        DatabaseName, SchemaName, ObjectName, StatisticsName,
        ErrorNumber, ErrorMessage, ExtendedInfo
    ) VALUES (
        N'UPDATE_STATISTICS',
        N'UPDATE STATISTICS [TestDB].[dbo].[Orders] ([IX_Orders_Date]) WITH SAMPLE 25 PERCENT',
        DATEADD(SECOND, @j * 30, @start),
        DATEADD(MILLISECOND, 150 + @j * 50, DATEADD(SECOND, @j * 30, @start)),
        N'TestDB', N'dbo',
        CASE @j WHEN 0 THEN N'Orders' WHEN 1 THEN N'OrderItems' WHEN 2 THEN N'Customers' WHEN 3 THEN N'Products' ELSE N'Inventory' END,
        CASE @j WHEN 0 THEN N'IX_Orders_Date' WHEN 1 THEN N'IX_OrderItems_OrderID' WHEN 2 THEN N'IX_Customers_Email' WHEN 3 THEN N'PK_Products' ELSE N'_WA_Sys_00000003_Inventory' END,
        NULL, NULL,
        N'<ExtendedInfo>
            <RunLabel>' + @run_label + N'</RunLabel>
            <QualifyReason>QUERY_STORE_PRIORITY</QualifyReason>
            <RowCount>' + CONVERT(nvarchar(20), 500000 + @j * 100000) + N'</RowCount>
            <ModificationCounter>' + CONVERT(nvarchar(20), 10000 + @j * 5000) + N'</ModificationCounter>
            <ModificationPct>' + CONVERT(nvarchar(10), 2.0 + @j * 0.5) + N'</ModificationPct>
            <PageCount>' + CONVERT(nvarchar(20), 5000 + @j * 1000) + N'</PageCount>
            <SizeMB>' + CONVERT(nvarchar(10), 40 + @j * 8) + N'</SizeMB>
            <DaysStale>' + CONVERT(nvarchar(10), 1 + @j) + N'</DaysStale>
            <HasNorecompute>0</HasNorecompute>
            <IsHeap>0</IsHeap>
            <QSPlanCount>' + CONVERT(nvarchar(10), 5 + @j * 2) + N'</QSPlanCount>
            <QSTotalCpuMs>' + CONVERT(nvarchar(20), 50000 - @j * 8000) + N'</QSTotalCpuMs>
            <QSTotalExecutions>' + CONVERT(nvarchar(20), 10000 - @j * 1500) + N'</QSTotalExecutions>
            <QSTotalMemoryGrantKB>' + CONVERT(nvarchar(20), 8000 - @j * 1200) + N'</QSTotalMemoryGrantKB>
            <QSTotalTempdbPages>' + CONVERT(nvarchar(20), 2000 - @j * 300) + N'</QSTotalTempdbPages>
            <QSPriorityBoost>1</QSPriorityBoost>
            <ProcessingPosition>' + CONVERT(nvarchar(10), @j + 1) + N'</ProcessingPosition>
        </ExtendedInfo>'
    );
    SET @j = @j + 1;
END;

RAISERROR(N'  Run 1: Healthy completed run (7 days ago)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUN 2: Time-limited run, 9 days ago (PRIOR window)
   ========================================================================= */
SET @start = DATEADD(DAY, -9, @anchor);
SET @run_label = N'test_run02_timelimited';
SET @end_time = DATEADD(SECOND, 3600, @start);

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>1.9.2026.0206</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <TimeLimit>3600</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <ThresholdLogic>OR</ThresholdLogic>
        <SortOrder>MODIFICATION_COUNTER</SortOrder>
        <StatsInParallel>N</StatsInParallel>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>1.9.2026.0206</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>200</StatsFound>
        <StatsProcessed>120</StatsProcessed>
        <StatsSucceeded>120</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>80</StatsRemaining>
        <DurationSeconds>3600</DurationSeconds>
        <StopReason>TIME_LIMIT</StopReason>
    </Summary>');

RAISERROR(N'  Run 2: Time-limited run (6 days ago)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUN 3: Killed run (START only, no END), 5 days ago
   ========================================================================= */
SET @start = DATEADD(DAY, -5, @anchor);
SET @run_label = N'test_run03_killed';

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>1.9.2026.0206</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <TimeLimit>3600</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <SortOrder>MODIFICATION_COUNTER</SortOrder>
        <StatsInParallel>N</StatsInParallel>
    </Parameters>');

RAISERROR(N'  Run 3: Killed run - no END record (5 days ago)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUN 4: Killed run cleaned up by @CleanupOrphanedRuns, 4 days ago
   ========================================================================= */
SET @start = DATEADD(DAY, -4, @anchor);
SET @run_label = N'test_run04_orphan_cleanup';
/* Orphan cleanup set EndTime to a later run's start time */
SET @end_time = DATEADD(DAY, -3, @anchor);

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>1.9.2026.0206</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <TimeLimit>3600</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <SortOrder>MODIFICATION_COUNTER</SortOrder>
        <StatsInParallel>N</StatsInParallel>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>1.9.2026.0206</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>0</StatsFound>
        <StatsProcessed>0</StatsProcessed>
        <StatsSucceeded>0</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>0</StatsRemaining>
        <DurationSeconds>86400</DurationSeconds>
        <StopReason>KILLED</StopReason>
    </Summary>');

RAISERROR(N'  Run 4: Orphan-cleaned killed run (4 days ago)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUN 5: Run with repeated failures, 3 days ago
   ========================================================================= */
SET @start = DATEADD(DAY, -3, @anchor);
SET @run_label = N'test_run05';
SET @end_time = DATEADD(SECOND, 2400, @start);

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>1.9.2026.0206</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <TimeLimit>3600</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <SortOrder>MODIFICATION_COUNTER</SortOrder>
        <StatsInParallel>N</StatsInParallel>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>1.9.2026.0206</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>100</StatsFound>
        <StatsProcessed>100</StatsProcessed>
        <StatsSucceeded>97</StatsSucceeded>
        <StatsFailed>3</StatsFailed>
        <StatsRemaining>0</StatsRemaining>
        <DurationSeconds>2400</DurationSeconds>
        <StopReason>COMPLETED</StopReason>
    </Summary>');

/* Failed stats - same stat failing (for C2 repeated failures) */
SET @j = 0;
WHILE @j < 3
BEGIN
    INSERT INTO dbo.CommandLog (
        CommandType, Command, StartTime, EndTime,
        DatabaseName, SchemaName, ObjectName, StatisticsName,
        ErrorNumber, ErrorMessage, ExtendedInfo
    ) VALUES (
        N'UPDATE_STATISTICS',
        N'UPDATE STATISTICS [TestDB].[dbo].[BigTable] ([IX_BigTable_Status]) WITH SAMPLE 25 PERCENT',
        DATEADD(SECOND, @j * 60, @start),
        DATEADD(SECOND, @j * 60 + 30, @start),
        N'TestDB', N'dbo', N'BigTable', N'IX_BigTable_Status',
        1222, N'Lock request time out period exceeded.',
        N'<ExtendedInfo>
            <RunLabel>' + @run_label + N'</RunLabel>
            <QualifyReason>MODIFICATION_COUNTER</QualifyReason>
            <RowCount>5000000</RowCount>
            <ModificationCounter>100000</ModificationCounter>
            <PageCount>50000</PageCount>
            <SizeMB>400</SizeMB>
            <DaysStale>3</DaysStale>
            <HasNorecompute>0</HasNorecompute>
            <IsHeap>0</IsHeap>
        </ExtendedInfo>'
    );
    SET @j = @j + 1;
END;

RAISERROR(N'  Run 5: Run with failures (3 days ago)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUNS 6-7: Repeated failures in additional runs (for C2 threshold of 3)
   ========================================================================= */
DECLARE @day_offset int = -2;
WHILE @day_offset <= -1
BEGIN
    SET @start = DATEADD(DAY, @day_offset, @anchor);
    SET @run_label = N'test_' + CONVERT(varchar(8), @start, 112) + N'_000000';
    SET @end_time = DATEADD(SECOND, 1900, @start);

    INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
    VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
        N'<Parameters>
            <Version>1.9.2026.0206</Version>
            <RunLabel>' + @run_label + N'</RunLabel>
            <Databases>USER_DATABASES</Databases>
            <TimeLimit>3600</TimeLimit>
            <ModificationThreshold>5000</ModificationThreshold>
            <TieredThresholds>1</TieredThresholds>
            <SortOrder>MODIFICATION_COUNTER</SortOrder>
            <StatsInParallel>N</StatsInParallel>
        </Parameters>');

    INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
    VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
        N'<Summary>
            <Version>1.9.2026.0206</Version>
            <RunLabel>' + @run_label + N'</RunLabel>
            <StatsFound>100</StatsFound>
            <StatsProcessed>100</StatsProcessed>
            <StatsSucceeded>99</StatsSucceeded>
            <StatsFailed>1</StatsFailed>
            <StatsRemaining>0</StatsRemaining>
            <DurationSeconds>1900</DurationSeconds>
            <StopReason>COMPLETED</StopReason>
        </Summary>');

    /* Same stat failing - IX_BigTable_Status */
    INSERT INTO dbo.CommandLog (
        CommandType, Command, StartTime, EndTime,
        DatabaseName, SchemaName, ObjectName, StatisticsName,
        ErrorNumber, ErrorMessage, ExtendedInfo
    ) VALUES (
        N'UPDATE_STATISTICS',
        N'UPDATE STATISTICS [TestDB].[dbo].[BigTable] ([IX_BigTable_Status]) WITH SAMPLE 25 PERCENT',
        @start, DATEADD(SECOND, 30, @start),
        N'TestDB', N'dbo', N'BigTable', N'IX_BigTable_Status',
        1222, N'Lock request time out period exceeded.',
        N'<ExtendedInfo>
            <RunLabel>' + @run_label + N'</RunLabel>
            <QualifyReason>MODIFICATION_COUNTER</QualifyReason>
            <RowCount>5000000</RowCount>
            <ModificationCounter>100000</ModificationCounter>
            <PageCount>50000</PageCount>
            <SizeMB>400</SizeMB>
            <DaysStale>3</DaysStale>
            <HasNorecompute>0</HasNorecompute>
            <IsHeap>0</IsHeap>
        </ExtendedInfo>'
    );

    SET @day_offset = @day_offset + 1;
END;

RAISERROR(N'  Runs 6-7: Additional failures for C2 threshold (2-1 days ago)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUNS 8-9: Old runs with FASTER throughput (for C4 degrading check)
   Using older version to also seed version history
   gh-558: C4's PRIOR window is now the 7 days immediately before the recent
   window (days 7-14 back from diag execution time).  The -14-day run falls
   just outside that lower bound (seeded from midnight, diag runs later), so
   only the -13-day run still feeds PRIOR -- alongside the other fast serial
   runs seeded in the 8-13 day range (runs 1, 15, 21, 24, 25).
   ========================================================================= */
SET @day_offset = -14;
WHILE @day_offset <= -13
BEGIN
    SET @start = DATEADD(DAY, @day_offset, @anchor);
    SET @run_label = N'test_' + CONVERT(varchar(8), @start, 112) + N'_000000';
    SET @end_time = DATEADD(SECOND, 600, @start);  /* Fast: 600s for 100 stats = 6 sec/stat */

    INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
    VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
        N'<Parameters>
            <Version>1.6.2026.0128</Version>
            <RunLabel>' + @run_label + N'</RunLabel>
            <Databases>USER_DATABASES</Databases>
            <TimeLimit>3600</TimeLimit>
            <ModificationThreshold>5000</ModificationThreshold>
            <TieredThresholds>1</TieredThresholds>
            <SortOrder>MODIFICATION_COUNTER</SortOrder>
            <StatsInParallel>N</StatsInParallel>
        </Parameters>');

    INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
    VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
        N'<Summary>
            <Version>1.6.2026.0128</Version>
            <RunLabel>' + @run_label + N'</RunLabel>
            <StatsFound>100</StatsFound>
            <StatsProcessed>100</StatsProcessed>
            <StatsSucceeded>100</StatsSucceeded>
            <StatsFailed>0</StatsFailed>
            <StatsRemaining>0</StatsRemaining>
            <DurationSeconds>600</DurationSeconds>
            <StopReason>COMPLETED</StopReason>
        </Summary>');

    /* Some stats for these runs */
    SET @j = 0;
    WHILE @j < 3
    BEGIN
        INSERT INTO dbo.CommandLog (
            CommandType, Command, StartTime, EndTime,
            DatabaseName, SchemaName, ObjectName, StatisticsName,
            ExtendedInfo
        ) VALUES (
            N'UPDATE_STATISTICS',
            N'UPDATE STATISTICS [TestDB].[dbo].[Orders] ([IX_Orders_Date])',
            DATEADD(SECOND, @j * 10, @start),
            DATEADD(MILLISECOND, 100, DATEADD(SECOND, @j * 10, @start)),
            N'TestDB', N'dbo', N'Orders', N'IX_Orders_Date',
            N'<ExtendedInfo>
                <RunLabel>' + @run_label + N'</RunLabel>
                <QualifyReason>MODIFICATION_COUNTER</QualifyReason>
                <RowCount>500000</RowCount>
                <ModificationCounter>10000</ModificationCounter>
                <PageCount>5000</PageCount>
                <SizeMB>40</SizeMB>
                <DaysStale>1</DaysStale>
                <HasNorecompute>0</HasNorecompute>
                <IsHeap>0</IsHeap>
            </ExtendedInfo>'
        );
        SET @j = @j + 1;
    END;

    SET @day_offset = @day_offset + 1;
END;

RAISERROR(N'  Runs 8-9: Fast old runs for throughput comparison (14-13 days ago)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUN 10: Overlapping run with Run 5 (for W4 detection)
   Starts 5 minutes into Run 5 and ends before Run 5
   ========================================================================= */
SET @start = DATEADD(MINUTE, 5, DATEADD(DAY, -3, @anchor));
SET @run_label = N'test_run10_overlap';
SET @end_time = DATEADD(MINUTE, 20, @start);

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>1.9.2026.0206</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>AnotherDB</Databases>
        <TimeLimit>3600</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <SortOrder>MODIFICATION_COUNTER</SortOrder>
        <StatsInParallel>N</StatsInParallel>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>1.9.2026.0206</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>30</StatsFound>
        <StatsProcessed>30</StatsProcessed>
        <StatsSucceeded>30</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>0</StatsRemaining>
        <DurationSeconds>1200</DurationSeconds>
        <StopReason>COMPLETED</StopReason>
    </Summary>');

RAISERROR(N'  Run 10: Overlapping run (concurrent with run 1)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUN 11: Run with no TimeLimit and TieredThresholds off (for W1).
   Version 2.24 (>= 2.15 full param logging) so 9ufm version-gating lets
   W1 reason about the ABSENT TimeLimit element (genuinely NULL).
   ========================================================================= */
SET @start = DATEADD(HOUR, -12, @anchor);
SET @run_label = N'test_run_12_hours_ago';
SET @end_time = DATEADD(SECOND, 5400, @start);

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>2.24.2026.03.24</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <ModificationThreshold>1000</ModificationThreshold>
        <TieredThresholds>0</TieredThresholds>
        <ThresholdLogic>OR</ThresholdLogic>
        <SortOrder>MODIFICATION_COUNTER</SortOrder>
        <StatsInParallel>N</StatsInParallel>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>2.24.2026.03.24</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>150</StatsFound>
        <StatsProcessed>150</StatsProcessed>
        <StatsSucceeded>150</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>0</StatsRemaining>
        <DurationSeconds>5400</DurationSeconds>
        <StopReason>COMPLETED</StopReason>
    </Summary>');

/* Long-running stat for this run */
INSERT INTO dbo.CommandLog (
    CommandType, Command, StartTime, EndTime,
    DatabaseName, SchemaName, ObjectName, StatisticsName,
    ExtendedInfo
) VALUES (
    N'UPDATE_STATISTICS',
    N'UPDATE STATISTICS [TestDB].[dbo].[HugeTable] ([IX_HugeTable_Created]) WITH SAMPLE 100 PERCENT',
    @start, DATEADD(MINUTE, 25, @start),
    N'TestDB', N'dbo', N'HugeTable', N'IX_HugeTable_Created',
    N'<ExtendedInfo>
        <RunLabel>' + @run_label + N'</RunLabel>
        <QualifyReason>MODIFICATION_COUNTER</QualifyReason>
        <RowCount>50000000</RowCount>
        <ModificationCounter>1000000</ModificationCounter>
        <PageCount>500000</PageCount>
        <SizeMB>4000</SizeMB>
        <DaysStale>7</DaysStale>
        <HasNorecompute>0</HasNorecompute>
        <IsHeap>0</IsHeap>
    </ExtendedInfo>'
);

RAISERROR(N'  Run 11: No TimeLimit, TieredThresholds off, with long-running stat (12 hours ago)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUNS 12-14: Additional QS-priority runs for I6/I7 inflection detection
   Need >= 3 QS runs + >= 3 non-QS runs for I7 to fire.
   Run 1 is already QS (10 days ago). Add 12 (8 days), 13 (7 days), 14 (6 days).
   Combined with runs 8-9-2 (non-QS, 14-13-9 days ago) = 3+ pre-QS.
   ========================================================================= */
SET @day_offset = -8;
WHILE @day_offset <= -6
BEGIN
    SET @start = DATEADD(DAY, @day_offset, @anchor);
    SET @run_label = N'test_qs_' + CONVERT(varchar(8), @start, 112);
    SET @end_time = DATEADD(SECOND, 500, @start);

    INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
    VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
        N'<Parameters>
            <Version>2.15.2026.03.05</Version>
            <RunLabel>' + @run_label + N'</RunLabel>
            <Databases>USER_DATABASES</Databases>
            <TimeLimit>3600</TimeLimit>
            <ModificationThreshold>5000</ModificationThreshold>
            <TieredThresholds>1</TieredThresholds>
            <ThresholdLogic>OR</ThresholdLogic>
            <SortOrder>QUERY_STORE</SortOrder>
            <QueryStorePriority>Y</QueryStorePriority>
            <StatsInParallel>N</StatsInParallel>
        </Parameters>');

    INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
    VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
        N'<Summary>
            <Version>2.15.2026.03.05</Version>
            <RunLabel>' + @run_label + N'</RunLabel>
            <StatsFound>100</StatsFound>
            <StatsProcessed>100</StatsProcessed>
            <StatsSucceeded>100</StatsSucceeded>
            <StatsFailed>0</StatsFailed>
            <StatsRemaining>0</StatsRemaining>
            <DurationSeconds>500</DurationSeconds>
            <StopReason>COMPLETED</StopReason>
        </Summary>');

    /* QS-prioritized stats with ProcessingPosition and QS metrics */
    SET @j = 0;
    WHILE @j < 5
    BEGIN
        INSERT INTO dbo.CommandLog (
            CommandType, Command, StartTime, EndTime,
            DatabaseName, SchemaName, ObjectName, StatisticsName,
            ExtendedInfo
        ) VALUES (
            N'UPDATE_STATISTICS',
            N'UPDATE STATISTICS [TestDB].[dbo].[Orders] ([IX_Orders_Date])',
            DATEADD(SECOND, @j * 10, @start),
            DATEADD(MILLISECOND, 200, DATEADD(SECOND, @j * 10, @start)),
            N'TestDB', N'dbo',
            CASE @j WHEN 0 THEN N'Orders' WHEN 1 THEN N'OrderItems' WHEN 2 THEN N'Customers' WHEN 3 THEN N'Products' ELSE N'Inventory' END,
            CASE @j WHEN 0 THEN N'IX_Orders_Date' WHEN 1 THEN N'IX_OrderItems_OrderID' WHEN 2 THEN N'IX_Customers_Email' WHEN 3 THEN N'PK_Products' ELSE N'_WA_Sys_00000003_Inventory' END,
            N'<ExtendedInfo>
                <RunLabel>' + @run_label + N'</RunLabel>
                <QualifyReason>QUERY_STORE_PRIORITY</QualifyReason>
                <RowCount>' + CONVERT(nvarchar(20), 500000 + @j * 100000) + N'</RowCount>
                <ModificationCounter>' + CONVERT(nvarchar(20), 8000 + @j * 3000) + N'</ModificationCounter>
                <PageCount>' + CONVERT(nvarchar(20), 5000 + @j * 1000) + N'</PageCount>
                <SizeMB>' + CONVERT(nvarchar(10), 40 + @j * 8) + N'</SizeMB>
                <DaysStale>' + CONVERT(nvarchar(10), 1 + @j) + N'</DaysStale>
                <HasNorecompute>0</HasNorecompute>
                <IsHeap>0</IsHeap>
                <QSPlanCount>' + CONVERT(nvarchar(10), 4 + @j * 3) + N'</QSPlanCount>
                <QSTotalCpuMs>' + CONVERT(nvarchar(20), 45000 - @j * 7000) + N'</QSTotalCpuMs>
                <QSTotalExecutions>' + CONVERT(nvarchar(20), 8000 - @j * 1200 + (ABS(@day_offset) - 8) * 2000) + N'</QSTotalExecutions>
                <QSTotalMemoryGrantKB>' + CONVERT(nvarchar(20), 10000 - @j * 1500 - (ABS(@day_offset) - 8) * 1000) + N'</QSTotalMemoryGrantKB>
                <QSTotalTempdbPages>' + CONVERT(nvarchar(20), 3000 - @j * 400 - (ABS(@day_offset) - 8) * 500) + N'</QSTotalTempdbPages>
                <ProcessingPosition>' + CONVERT(nvarchar(10), @j + 1) + N'</ProcessingPosition>
            </ExtendedInfo>'
        );
        SET @j = @j + 1;
    END;

    SET @day_offset = @day_offset + 1;
END;

RAISERROR(N'  Runs 12-14: QS-priority runs with ProcessingPosition (8-6 days ago)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUN 15: Additional old non-QS run (12 days ago) for I7 inflection
   Need 3 non-QS runs before the first QS run (10 days ago)
   Already have runs 8-9 at 14-13 days ago. This adds a third.
   ========================================================================= */
SET @start = DATEADD(DAY, -12, @anchor);
SET @run_label = N'test_old_nonqs_12d';
SET @end_time = DATEADD(SECOND, 700, @start);

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>1.9.2026.0206</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <TimeLimit>3600</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <SortOrder>MODIFICATION_COUNTER</SortOrder>
        <StatsInParallel>N</StatsInParallel>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>1.9.2026.0206</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>100</StatsFound>
        <StatsProcessed>100</StatsProcessed>
        <StatsSucceeded>100</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>0</StatsRemaining>
        <DurationSeconds>700</DurationSeconds>
        <StopReason>COMPLETED</StopReason>
    </Summary>');

/* Non-QS stats (no QSTotalCpuMs, no ProcessingPosition — uses older version) */
SET @j = 0;
WHILE @j < 3
BEGIN
    INSERT INTO dbo.CommandLog (
        CommandType, Command, StartTime, EndTime,
        DatabaseName, SchemaName, ObjectName, StatisticsName,
        ExtendedInfo
    ) VALUES (
        N'UPDATE_STATISTICS',
        N'UPDATE STATISTICS [TestDB].[dbo].[Orders] ([IX_Orders_Date])',
        DATEADD(SECOND, @j * 10, @start),
        DATEADD(MILLISECOND, 100, DATEADD(SECOND, @j * 10, @start)),
        N'TestDB', N'dbo', N'Orders', N'IX_Orders_Date',
        N'<ExtendedInfo>
            <RunLabel>' + @run_label + N'</RunLabel>
            <QualifyReason>MODIFICATION_COUNTER</QualifyReason>
            <RowCount>500000</RowCount>
            <ModificationCounter>10000</ModificationCounter>
            <PageCount>5000</PageCount>
            <SizeMB>40</SizeMB>
            <DaysStale>1</DaysStale>
            <HasNorecompute>0</HasNorecompute>
            <IsHeap>0</IsHeap>
        </ExtendedInfo>'
    );
    SET @j = @j + 1;
END;

RAISERROR(N'  Run 15: Old non-QS run for I7 inflection (12 days ago)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUN 16: Incomplete QS run (TIME_LIMIT) - for BUG-A WorkloadCoveragePct < 100%
   StatsFound > StatsProcessed to test honest coverage calculation
   ========================================================================= */
SET @start = DATEADD(DAY, -5, DATEADD(HOUR, 12, @anchor));
SET @run_label = N'test_qs_incomplete';
SET @end_time = DATEADD(SECOND, 1800, @start);

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>2.15.2026.03.05</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <TimeLimit>1800</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <ThresholdLogic>OR</ThresholdLogic>
        <SortOrder>QUERY_STORE</SortOrder>
        <QueryStorePriority>Y</QueryStorePriority>
        <StatsInParallel>N</StatsInParallel>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>2.15.2026.03.05</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>100</StatsFound>
        <StatsProcessed>60</StatsProcessed>
        <StatsSucceeded>60</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>40</StatsRemaining>
        <DurationSeconds>1800</DurationSeconds>
        <StopReason>TIME_LIMIT</StopReason>
    </Summary>');

/* QS stats for incomplete run — only 3 of 5 stats processed (reflecting incomplete) */
SET @j = 0;
WHILE @j < 3
BEGIN
    INSERT INTO dbo.CommandLog (
        CommandType, Command, StartTime, EndTime,
        DatabaseName, SchemaName, ObjectName, StatisticsName,
        ExtendedInfo
    ) VALUES (
        N'UPDATE_STATISTICS',
        N'UPDATE STATISTICS [TestDB].[dbo].[Orders] ([IX_Orders_Date])',
        DATEADD(SECOND, @j * 20, @start),
        DATEADD(MILLISECOND, 300, DATEADD(SECOND, @j * 20, @start)),
        N'TestDB', N'dbo',
        CASE @j WHEN 0 THEN N'Orders' WHEN 1 THEN N'OrderItems' ELSE N'Customers' END,
        CASE @j WHEN 0 THEN N'IX_Orders_Date' WHEN 1 THEN N'IX_OrderItems_OrderID' ELSE N'IX_Customers_Email' END,
        N'<ExtendedInfo>
            <RunLabel>' + @run_label + N'</RunLabel>
            <QualifyReason>QUERY_STORE_PRIORITY</QualifyReason>
            <RowCount>' + CONVERT(nvarchar(20), 500000 + @j * 100000) + N'</RowCount>
            <ModificationCounter>' + CONVERT(nvarchar(20), 9000 + @j * 4000) + N'</ModificationCounter>
            <PageCount>' + CONVERT(nvarchar(20), 5000 + @j * 1000) + N'</PageCount>
            <SizeMB>' + CONVERT(nvarchar(10), 40 + @j * 8) + N'</SizeMB>
            <DaysStale>' + CONVERT(nvarchar(10), 2 + @j) + N'</DaysStale>
            <HasNorecompute>0</HasNorecompute>
            <IsHeap>0</IsHeap>
            <QSPlanCount>' + CONVERT(nvarchar(10), 6 + @j * 2) + N'</QSPlanCount>
            <QSTotalCpuMs>' + CONVERT(nvarchar(20), 42000 - @j * 6000) + N'</QSTotalCpuMs>
            <QSTotalExecutions>' + CONVERT(nvarchar(20), 14000 - @j * 1000) + N'</QSTotalExecutions>
            <QSTotalMemoryGrantKB>' + CONVERT(nvarchar(20), 5000 - @j * 600) + N'</QSTotalMemoryGrantKB>
            <QSTotalTempdbPages>' + CONVERT(nvarchar(20), 800 - @j * 100) + N'</QSTotalTempdbPages>
            <ProcessingPosition>' + CONVERT(nvarchar(10), @j + 1) + N'</ProcessingPosition>
        </ExtendedInfo>'
    );
    SET @j = @j + 1;
END;

RAISERROR(N'  Run 16: Incomplete QS run - TIME_LIMIT (5 days ago, 60/100 processed)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUN 17: High-overhead run (W6 detection) - 2 days ago
   Wall clock: 300s, but actual stat updates total only ~5s.
   This simulates excessive discovery/environment check overhead.
   ========================================================================= */
SET @start = DATEADD(DAY, -2, @anchor);
SET @run_label = N'test_run17_overhead';
SET @end_time = DATEADD(SECOND, 300, @start);  /* 300s wall clock */

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>2.15.2026.03.05</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <TimeLimit>3600</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <ThresholdLogic>OR</ThresholdLogic>
        <SortOrder>MODIFICATION_COUNTER</SortOrder>
        <QueryStorePriority>Y</QueryStorePriority>
        <StatsInParallel>N</StatsInParallel>
        <CollectHeapForwarding>Y</CollectHeapForwarding>
        <GroupByJoinPattern>Y</GroupByJoinPattern>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>2.15.2026.03.05</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>10</StatsFound>
        <StatsProcessed>10</StatsProcessed>
        <StatsSucceeded>10</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>0</StatsRemaining>
        <DurationSeconds>300</DurationSeconds>
        <StopReason>COMPLETED</StopReason>
    </Summary>');

/* Stats for run 17 — 10 fast stats (500ms each = 5s total vs 300s wall clock) */
SET @j = 0;
WHILE @j < 10
BEGIN
    INSERT INTO dbo.CommandLog (
        CommandType, Command, StartTime, EndTime,
        DatabaseName, SchemaName, ObjectName, StatisticsName,
        ErrorNumber, ErrorMessage, ExtendedInfo
    ) VALUES (
        N'UPDATE_STATISTICS',
        N'UPDATE STATISTICS [TestDB].[dbo].[SmallTable] ([IX_SmallTable_Col' + CONVERT(nvarchar(2), @j) + N'])',
        /* Stats start late (after 200s of overhead), then run fast */
        DATEADD(SECOND, 200 + @j, @start),
        DATEADD(MILLISECOND, 500, DATEADD(SECOND, 200 + @j, @start)),
        N'TestDB', N'dbo', N'SmallTable',
        N'IX_SmallTable_Col' + CONVERT(nvarchar(2), @j),
        NULL, NULL,
        N'<ExtendedInfo>
            <RunLabel>' + @run_label + N'</RunLabel>
            <QualifyReason>' +
                CASE @j WHEN 0 THEN N'ASCENDING_KEY'
                        WHEN 1 THEN N'QUERY_STORE_PRIORITY'
                        ELSE N'TIERED_THRESHOLD' END +
            N'</QualifyReason>
            <RowCount>1000</RowCount>
            <ModificationCounter>600</ModificationCounter>
            <PageCount>50</PageCount>
            <SizeMB>0</SizeMB>
            <DaysStale>3</DaysStale>
            <HasNorecompute>0</HasNorecompute>
            <IsHeap>0</IsHeap>
            <ProcessingPosition>' + CONVERT(nvarchar(10), @j + 1) + N'</ProcessingPosition>
        </ExtendedInfo>'
    );
    SET @j = @j + 1;
END;

RAISERROR(N'  Run 17: High-overhead run (300s wall clock, ~5s actual stat work)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUNS 18-19: Parallel coordinator + follower (20:15 job, 16 days ago)
   These represent a two-worker parallel run from the 20:15 Agent job.
   Both workers use StatsInParallel='Y' and end with PARALLEL_COMPLETE.
   They overlap intentionally -- W4 must NOT fire for parallel pairs.
   Combined they process all 2500 stats (1400 + 1100), StatsRemaining=0.
   Placed at -16 days so Run 11 (-12h) remains the most recent serial run
   and W1 (suboptimal params in latest run) continues to fire correctly.
   ========================================================================= */

/* Run 18: Parallel coordinator */
SET @start = DATEADD(MINUTE, (20 * 60) + 15, DATEADD(DAY, -16, @anchor));  /* 20:15:01, 16 days ago */
SET @start = DATEADD(SECOND, 1, @start);
SET @run_label = N'test_run18_parallel_coord';
SET @end_time = DATEADD(SECOND, 2100, @start);  /* 2100s duration */

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>2.24.2026.03.24</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <TimeLimit>31499</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <ThresholdLogic>OR</ThresholdLogic>
        <SortOrder>QUERY_STORE</SortOrder>
        <QueryStorePriority>Y</QueryStorePriority>
        <StatsInParallel>Y</StatsInParallel>
        <MopUpPass>Y</MopUpPass>
        <MopUpMinRemainingSeconds>60</MopUpMinRemainingSeconds>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>2.24.2026.03.24</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>2500</StatsFound>
        <StatsProcessed>1400</StatsProcessed>
        <StatsSucceeded>1400</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>0</StatsRemaining>
        <DurationSeconds>2100</DurationSeconds>
        <StopReason>PARALLEL_COMPLETE</StopReason>
        <MopUpTriggered>1</MopUpTriggered>
        <MopUpFound>1400</MopUpFound>
        <MopUpProcessed>1400</MopUpProcessed>
    </Summary>');

/* Stats for run 18 -- 5 representative parallel stat updates */
SET @j = 0;
WHILE @j < 5
BEGIN
    INSERT INTO dbo.CommandLog (
        CommandType, Command, StartTime, EndTime,
        DatabaseName, SchemaName, ObjectName, StatisticsName,
        ErrorNumber, ErrorMessage, ExtendedInfo
    ) VALUES (
        N'UPDATE_STATISTICS',
        N'UPDATE STATISTICS [ProdDB].[dbo].[SalesOrders] ([IX_SalesOrders_Date]) WITH SAMPLE 25 PERCENT',
        DATEADD(SECOND, @j * 100, @start),
        DATEADD(SECOND, @j * 100 + 90, @start),
        N'ProdDB', N'dbo',
        CASE @j WHEN 0 THEN N'SalesOrders' WHEN 1 THEN N'SalesItems' WHEN 2 THEN N'Customers' WHEN 3 THEN N'Products' ELSE N'Inventory' END,
        CASE @j WHEN 0 THEN N'IX_SalesOrders_Date' WHEN 1 THEN N'IX_SalesItems_OrderID' WHEN 2 THEN N'IX_Customers_Region' WHEN 3 THEN N'PK_Products' ELSE N'IX_Inventory_SKU' END,
        NULL, NULL,
        N'<ExtendedInfo>
            <RunLabel>' + @run_label + N'</RunLabel>
            <QualifyReason>QUERY_STORE_PRIORITY</QualifyReason>
            <RowCount>' + CONVERT(nvarchar(20), 1000000 + @j * 200000) + N'</RowCount>
            <ModificationCounter>' + CONVERT(nvarchar(20), 20000 + @j * 5000) + N'</ModificationCounter>
            <PageCount>' + CONVERT(nvarchar(20), 10000 + @j * 2000) + N'</PageCount>
            <SizeMB>' + CONVERT(nvarchar(10), 80 + @j * 16) + N'</SizeMB>
            <DaysStale>' + CONVERT(nvarchar(10), 1 + @j) + N'</DaysStale>
            <HasNorecompute>0</HasNorecompute>
            <IsHeap>0</IsHeap>
            <QSPlanCount>' + CONVERT(nvarchar(10), 8 + @j * 2) + N'</QSPlanCount>
            <QSTotalCpuMs>' + CONVERT(nvarchar(20), 80000 - @j * 10000) + N'</QSTotalCpuMs>
            <QSTotalExecutions>' + CONVERT(nvarchar(20), 15000 - @j * 2000) + N'</QSTotalExecutions>
            <QSTotalMemoryGrantKB>' + CONVERT(nvarchar(20), 12000 - @j * 1500) + N'</QSTotalMemoryGrantKB>
            <QSTotalTempdbPages>' + CONVERT(nvarchar(20), 3000 - @j * 400) + N'</QSTotalTempdbPages>
            <QSPriorityBoost>1</QSPriorityBoost>
            <ProcessingPosition>' + CONVERT(nvarchar(10), @j + 1) + N'</ProcessingPosition>
        </ExtendedInfo>'
    );
    SET @j = @j + 1;
END;

RAISERROR(N'  Run 18: Parallel coordinator -- PARALLEL_COMPLETE, 1400 stats (16 days ago 20:15)', 10, 1) WITH NOWAIT;

/* Run 19: Parallel follower (starts 2s after coordinator, same job window) */
SET @start = DATEADD(MINUTE, (20 * 60) + 15, DATEADD(DAY, -16, @anchor));  /* 20:15:03, 16 days ago */
SET @start = DATEADD(SECOND, 3, @start);
SET @run_label = N'test_run19_parallel_follower';
SET @end_time = DATEADD(SECOND, 2050, @start);  /* 2050s duration */

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>2.24.2026.03.24</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <TimeLimit>31499</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <ThresholdLogic>OR</ThresholdLogic>
        <SortOrder>QUERY_STORE</SortOrder>
        <QueryStorePriority>Y</QueryStorePriority>
        <StatsInParallel>Y</StatsInParallel>
        <MopUpPass>Y</MopUpPass>
        <MopUpMinRemainingSeconds>60</MopUpMinRemainingSeconds>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>2.24.2026.03.24</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>2500</StatsFound>
        <StatsProcessed>1100</StatsProcessed>
        <StatsSucceeded>1100</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>0</StatsRemaining>
        <DurationSeconds>2050</DurationSeconds>
        <StopReason>PARALLEL_COMPLETE</StopReason>
        <MopUpTriggered>1</MopUpTriggered>
        <MopUpFound>1100</MopUpFound>
        <MopUpProcessed>1100</MopUpProcessed>
    </Summary>');

/* Stats for run 19 -- 4 representative parallel stat updates (different tables from run 18) */
SET @j = 0;
WHILE @j < 4
BEGIN
    INSERT INTO dbo.CommandLog (
        CommandType, Command, StartTime, EndTime,
        DatabaseName, SchemaName, ObjectName, StatisticsName,
        ErrorNumber, ErrorMessage, ExtendedInfo
    ) VALUES (
        N'UPDATE_STATISTICS',
        N'UPDATE STATISTICS [ProdDB].[dbo].[Shipments] ([IX_Shipments_Status]) WITH SAMPLE 25 PERCENT',
        DATEADD(SECOND, @j * 110, @start),
        DATEADD(SECOND, @j * 110 + 95, @start),
        N'ProdDB', N'dbo',
        CASE @j WHEN 0 THEN N'Shipments' WHEN 1 THEN N'ShipmentItems' WHEN 2 THEN N'Warehouses' ELSE N'PickLists' END,
        CASE @j WHEN 0 THEN N'IX_Shipments_Status' WHEN 1 THEN N'IX_ShipmentItems_ShipID' WHEN 2 THEN N'IX_Warehouses_Region' ELSE N'IX_PickLists_Date' END,
        NULL, NULL,
        N'<ExtendedInfo>
            <RunLabel>' + @run_label + N'</RunLabel>
            <QualifyReason>QUERY_STORE_PRIORITY</QualifyReason>
            <RowCount>' + CONVERT(nvarchar(20), 800000 + @j * 150000) + N'</RowCount>
            <ModificationCounter>' + CONVERT(nvarchar(20), 18000 + @j * 4000) + N'</ModificationCounter>
            <PageCount>' + CONVERT(nvarchar(20), 8000 + @j * 1500) + N'</PageCount>
            <SizeMB>' + CONVERT(nvarchar(10), 64 + @j * 12) + N'</SizeMB>
            <DaysStale>' + CONVERT(nvarchar(10), 1 + @j) + N'</DaysStale>
            <HasNorecompute>0</HasNorecompute>
            <IsHeap>0</IsHeap>
            <QSPlanCount>' + CONVERT(nvarchar(10), 6 + @j * 2) + N'</QSPlanCount>
            <QSTotalCpuMs>' + CONVERT(nvarchar(20), 60000 - @j * 8000) + N'</QSTotalCpuMs>
            <QSTotalExecutions>' + CONVERT(nvarchar(20), 12000 - @j * 1500) + N'</QSTotalExecutions>
            <QSTotalMemoryGrantKB>' + CONVERT(nvarchar(20), 9000 - @j * 1200) + N'</QSTotalMemoryGrantKB>
            <QSTotalTempdbPages>' + CONVERT(nvarchar(20), 2200 - @j * 300) + N'</QSTotalTempdbPages>
            <QSPriorityBoost>1</QSPriorityBoost>
            <ProcessingPosition>' + CONVERT(nvarchar(10), @j + 1) + N'</ProcessingPosition>
        </ExtendedInfo>'
    );
    SET @j = @j + 1;
END;

RAISERROR(N'  Run 19: Parallel follower -- PARALLEL_COMPLETE, 1100 stats (16 days ago 20:15, overlaps Run 18)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUN 20: Second parallel job (23:00, shorter TimeLimit, 16 days ago)
   Represents a second Agent job that fires later in the evening.
   Does NOT overlap with Runs 18-19 (those ended ~20:50, this starts 23:00).
   ========================================================================= */
SET @start = DATEADD(MINUTE, 23 * 60, DATEADD(DAY, -16, @anchor));  /* 23:00:01, 16 days ago */
SET @start = DATEADD(SECOND, 1, @start);
SET @run_label = N'test_run20_parallel_23h';
SET @end_time = DATEADD(SECOND, 1600, @start);  /* 1600s duration */

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>2.24.2026.03.24</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <TimeLimit>21599</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <ThresholdLogic>OR</ThresholdLogic>
        <SortOrder>QUERY_STORE</SortOrder>
        <QueryStorePriority>Y</QueryStorePriority>
        <StatsInParallel>Y</StatsInParallel>
        <MopUpPass>Y</MopUpPass>
        <MopUpMinRemainingSeconds>60</MopUpMinRemainingSeconds>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>2.24.2026.03.24</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>900</StatsFound>
        <StatsProcessed>650</StatsProcessed>
        <StatsSucceeded>650</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>0</StatsRemaining>
        <DurationSeconds>1600</DurationSeconds>
        <StopReason>PARALLEL_COMPLETE</StopReason>
        <MopUpTriggered>1</MopUpTriggered>
        <MopUpFound>650</MopUpFound>
        <MopUpProcessed>650</MopUpProcessed>
    </Summary>');

/* Stats for run 20 -- 3 representative parallel stat updates */
SET @j = 0;
WHILE @j < 3
BEGIN
    INSERT INTO dbo.CommandLog (
        CommandType, Command, StartTime, EndTime,
        DatabaseName, SchemaName, ObjectName, StatisticsName,
        ErrorNumber, ErrorMessage, ExtendedInfo
    ) VALUES (
        N'UPDATE_STATISTICS',
        N'UPDATE STATISTICS [ProdDB].[dbo].[Reports] ([IX_Reports_Created]) WITH SAMPLE 25 PERCENT',
        DATEADD(SECOND, @j * 120, @start),
        DATEADD(SECOND, @j * 120 + 100, @start),
        N'ProdDB', N'dbo',
        CASE @j WHEN 0 THEN N'Reports' WHEN 1 THEN N'ReportLines' ELSE N'Schedules' END,
        CASE @j WHEN 0 THEN N'IX_Reports_Created' WHEN 1 THEN N'IX_ReportLines_ReportID' ELSE N'IX_Schedules_NextRun' END,
        NULL, NULL,
        N'<ExtendedInfo>
            <RunLabel>' + @run_label + N'</RunLabel>
            <QualifyReason>QUERY_STORE_PRIORITY</QualifyReason>
            <RowCount>' + CONVERT(nvarchar(20), 600000 + @j * 100000) + N'</RowCount>
            <ModificationCounter>' + CONVERT(nvarchar(20), 15000 + @j * 3000) + N'</ModificationCounter>
            <PageCount>' + CONVERT(nvarchar(20), 6000 + @j * 1000) + N'</PageCount>
            <SizeMB>' + CONVERT(nvarchar(10), 48 + @j * 8) + N'</SizeMB>
            <DaysStale>' + CONVERT(nvarchar(10), 1 + @j) + N'</DaysStale>
            <HasNorecompute>0</HasNorecompute>
            <IsHeap>0</IsHeap>
            <QSPlanCount>' + CONVERT(nvarchar(10), 5 + @j * 2) + N'</QSPlanCount>
            <QSTotalCpuMs>' + CONVERT(nvarchar(20), 55000 - @j * 9000) + N'</QSTotalCpuMs>
            <QSTotalExecutions>' + CONVERT(nvarchar(20), 11000 - @j * 1800) + N'</QSTotalExecutions>
            <QSTotalMemoryGrantKB>' + CONVERT(nvarchar(20), 7000 - @j * 1000) + N'</QSTotalMemoryGrantKB>
            <QSTotalTempdbPages>' + CONVERT(nvarchar(20), 1800 - @j * 250) + N'</QSTotalTempdbPages>
            <QSPriorityBoost>1</QSPriorityBoost>
            <ProcessingPosition>' + CONVERT(nvarchar(10), @j + 1) + N'</ProcessingPosition>
        </ExtendedInfo>'
    );
    SET @j = @j + 1;
END;

RAISERROR(N'  Run 20: Parallel 23:00 job -- PARALLEL_COMPLETE, 650 stats (16 days ago 23:00)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUNS 21-23: WarningsCodes recurring-warning test (t8lj / W15 / I16)
   Three runs with WarningsCodes containing BACKUP_RUNNING (environmental ->
   W15) and TOCTOU_SKIPS (informational -> I16).  Also one run WITHOUT
   WarningsCodes to verify that old rows produce zero false findings.
   ========================================================================= */
/* -13..-15: OUTSIDE the C4 7-day recent-throughput window (these healthy runs
   would otherwise dilute the seeded degradation signal) but inside the 30-day
   analysis window that W15/I16 aggregate over. */
DECLARE @day21 int = -13;
WHILE @day21 >= -15
BEGIN
    SET @start = DATEADD(DAY, @day21, @anchor);
    SET @run_label = N'test_warncode_' + CONVERT(varchar(8), @start, 112);
    SET @end_time = DATEADD(SECOND, 450, @start);

    INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
    VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
        N'<Parameters>
            <Version>3.5.9.2026.06.25</Version>
            <RunLabel>' + @run_label + N'</RunLabel>
            <Databases>USER_DATABASES</Databases>
            <TimeLimit>3600</TimeLimit>
            <ModificationThreshold>5000</ModificationThreshold>
            <TieredThresholds>1</TieredThresholds>
            <SortOrder>MODIFICATION_COUNTER</SortOrder>
            <StatsInParallel>N</StatsInParallel>
            <Preset>NIGHTLY</Preset>
        </Parameters>');

    INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
    VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
        N'<Summary>
            <Version>3.5.9.2026.06.25</Version>
            <RunLabel>' + @run_label + N'</RunLabel>
            <StatsFound>50</StatsFound>
            <StatsProcessed>50</StatsProcessed>
            <StatsSucceeded>50</StatsSucceeded>
            <StatsFailed>0</StatsFailed>
            <StatsRemaining>0</StatsRemaining>
            <DurationSeconds>450</DurationSeconds>
            <StopReason>COMPLETED</StopReason>
            <StatsToctou>2</StatsToctou>
            <QSEnrichmentSkipped>0</QSEnrichmentSkipped>
            <WarningsCodes>BACKUP_RUNNING|TOCTOU_SKIPS</WarningsCodes>
        </Summary>');

    SET @day21 = @day21 - 1;
END;

RAISERROR(N'  Runs 21-23: WarningsCodes recurring (BACKUP_RUNNING+TOCTOU_SKIPS, 3 runs)', 10, 1) WITH NOWAIT;

/* RUN 24: TEMPDB_PRESSURE stop -- no WarningsCodes element (old-format run) */
SET @start = DATEADD(DAY, -12, @anchor);
SET @run_label = N'test_tempdb_press_1';
SET @end_time = DATEADD(SECOND, 90, @start);

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>3.5.9.2026.06.25</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <TimeLimit>3600</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <SortOrder>MODIFICATION_COUNTER</SortOrder>
        <StatsInParallel>N</StatsInParallel>
        <Preset>NIGHTLY</Preset>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>3.5.9.2026.06.25</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>50</StatsFound>
        <StatsProcessed>10</StatsProcessed>
        <StatsSucceeded>10</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>40</StatsRemaining>
        <DurationSeconds>90</DurationSeconds>
        <StopReason>TEMPDB_PRESSURE</StopReason>
    </Summary>');

RAISERROR(N'  Run 24: TEMPDB_PRESSURE stop (no WarningsCodes -- old format)', 10, 1) WITH NOWAIT;

/* RUN 25: Second TEMPDB_PRESSURE stop -- triggers W14 */
SET @start = DATEADD(DAY, -11, @anchor);
SET @run_label = N'test_tempdb_press_2';
SET @end_time = DATEADD(SECOND, 45, @start);

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>3.5.9.2026.06.25</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <TimeLimit>3600</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <SortOrder>MODIFICATION_COUNTER</SortOrder>
        <StatsInParallel>N</StatsInParallel>
        <Preset>NIGHTLY</Preset>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>3.5.9.2026.06.25</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>50</StatsFound>
        <StatsProcessed>5</StatsProcessed>
        <StatsSucceeded>5</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>45</StatsRemaining>
        <DurationSeconds>45</DurationSeconds>
        <StopReason>TEMPDB_PRESSURE</StopReason>
    </Summary>');

RAISERROR(N'  Run 25: Second TEMPDB_PRESSURE stop -- W14 should fire', 10, 1) WITH NOWAIT;

/* SKIP MARKERS: SKIPPED: Command and TOCTOU_SKIP ErrorMessage rows
   These should be excluded from #stat_updates entirely (70hh).
   Two skip markers attached to run 21's RunLabel. */
SET @start = DATEADD(DAY, -6, @anchor);
SET @run_label = N'test_warncode_' + CONVERT(varchar(8), @start, 112);

/* Pre-check SKIPPED: path (Command begins SKIPPED:) */
INSERT INTO dbo.CommandLog (
    CommandType, Command, StartTime, EndTime,
    DatabaseName, SchemaName, ObjectName, StatisticsName,
    ErrorNumber, ErrorMessage, ExtendedInfo
) VALUES (
    N'UPDATE_STATISTICS',
    N'SKIPPED: [TestDB].[dbo].[Orders] ([IX_Orders_Date]) -- concurrent update detected',
    @start,
    @start,  /* 0ms duration -- skip marker */
    N'TestDB', N'dbo', N'Orders', N'IX_Orders_Skip1',
    NULL, NULL,
    N'<ExtendedInfo>
        <RunLabel>' + @run_label + N'</RunLabel>
        <QualifyReason>MODIFICATION_COUNTER</QualifyReason>
        <RowCount>500000</RowCount>
        <ModificationCounter>10000</ModificationCounter>
        <PageCount>5000</PageCount>
    </ExtendedInfo>'
);

/* TOCTOU_SKIP CATCH-path (ErrorMessage begins TOCTOU_SKIP) */
INSERT INTO dbo.CommandLog (
    CommandType, Command, StartTime, EndTime,
    DatabaseName, SchemaName, ObjectName, StatisticsName,
    ErrorNumber, ErrorMessage, ExtendedInfo
) VALUES (
    N'UPDATE_STATISTICS',
    N'UPDATE STATISTICS [TestDB].[dbo].[Orders] ([IX_Orders_Status]) WITH SAMPLE 25 PERCENT',
    @start,
    DATEADD(MILLISECOND, 1, @start),  /* 1ms -- near-zero, skip marker */
    N'TestDB', N'dbo', N'Orders', N'IX_Orders_Skip2',
    NULL, N'TOCTOU_SKIP (error 0): stat claimed by another session between check and execution',
    N'<ExtendedInfo>
        <RunLabel>' + @run_label + N'</RunLabel>
        <QualifyReason>MODIFICATION_COUNTER</QualifyReason>
        <RowCount>500000</RowCount>
        <ModificationCounter>8000</ModificationCounter>
        <PageCount>5000</PageCount>
    </ExtendedInfo>'
);

RAISERROR(N'  Skip markers: 2 rows (SKIPPED: + TOCTOU_SKIP) -- should be excluded from #stat_updates', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUN 26: CriticalTables configured -- WITH matching IsCritical=1 stats
   Drives I17 CRITICAL_TABLES_COVERAGE.
   Date: -17 days (outside C4 7-day window; sec/stat = 450/50 = 9).
   ========================================================================= */
SET @start = DATEADD(DAY, -17, @anchor);
SET @run_label = N'test_critical_coverage';
SET @end_time = DATEADD(SECOND, 450, @start);   /* 450s / 50 stats = 9 sec/stat */

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>3.5.5.2026.07.01</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <TimeLimit>3600</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <SortOrder>MODIFICATION_COUNTER</SortOrder>
        <StatsInParallel>N</StatsInParallel>
        <Preset>NIGHTLY</Preset>
        <CriticalTables>dbo.Orders,dbo.OrderItems</CriticalTables>
        <CriticalSamplePercent>100</CriticalSamplePercent>
        <CriticalTablesFirst>Y</CriticalTablesFirst>
        <FirstTimeFullScanCapRows>10000000</FirstTimeFullScanCapRows>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>3.5.5.2026.07.01</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>50</StatsFound>
        <StatsProcessed>50</StatsProcessed>
        <StatsSucceeded>50</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>0</StatsRemaining>
        <DurationSeconds>450</DurationSeconds>
        <StopReason>COMPLETED</StopReason>
    </Summary>');

/* Per-stat rows: first 2 with IsCritical=1 + CriticalSampleOverride=1, rest normal */
SET @j = 0;
WHILE @j < 4
BEGIN
    INSERT INTO dbo.CommandLog (
        CommandType, Command, StartTime, EndTime,
        DatabaseName, SchemaName, ObjectName, StatisticsName,
        ErrorNumber, ErrorMessage, ExtendedInfo
    ) VALUES (
        N'UPDATE_STATISTICS',
        N'UPDATE STATISTICS [TestDB].[dbo].[' + CASE @j WHEN 0 THEN N'Orders' WHEN 1 THEN N'OrderItems' WHEN 2 THEN N'Customers' ELSE N'Products' END + N'] WITH SAMPLE 25 PERCENT',
        DATEADD(SECOND, @j * 40, @start),
        DATEADD(SECOND, @j * 40 + 35, @start),
        N'TestDB', N'dbo',
        CASE @j WHEN 0 THEN N'Orders' WHEN 1 THEN N'OrderItems' WHEN 2 THEN N'Customers' ELSE N'Products' END,
        CASE @j WHEN 0 THEN N'IX_Orders_Date' WHEN 1 THEN N'IX_OrderItems_OrderID' WHEN 2 THEN N'IX_Customers_Email' ELSE N'PK_Products' END,
        NULL, NULL,
        N'<ExtendedInfo>
            <RunLabel>' + @run_label + N'</RunLabel>
            <QualifyReason>MODIFICATION_COUNTER</QualifyReason>
            <RowCount>' + CONVERT(nvarchar(20), 200000 + @j * 50000) + N'</RowCount>
            <ModificationCounter>' + CONVERT(nvarchar(20), 6000 + @j * 2000) + N'</ModificationCounter>
            <PageCount>' + CONVERT(nvarchar(20), 3000 + @j * 500) + N'</PageCount>
            <SizeMB>' + CONVERT(nvarchar(10), 24 + @j * 4) + N'</SizeMB>
            <DaysStale>2</DaysStale>
            <HasNorecompute>0</HasNorecompute>
            <IsHeap>0</IsHeap>
            <ProcessingPosition>' + CONVERT(nvarchar(10), @j + 1) + N'</ProcessingPosition>
            <IsCritical>' + CASE WHEN @j < 2 THEN N'1' ELSE N'0' END + N'</IsCritical>
            <CriticalSampleOverride>' + CASE WHEN @j < 2 THEN N'1' ELSE N'0' END + N'</CriticalSampleOverride>
        </ExtendedInfo>'
    );
    SET @j = @j + 1;
END;

RAISERROR(N'  Run 26: CriticalTables run with IsCritical=1 stats (I17 coverage, -17 days)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUN 27: CriticalTables configured -- ZERO IsCritical=1 stats
   Drives W16 CRITICAL_TABLES_NO_MATCH (pattern matched nothing).
   Date: -18 days (outside C4 window; sec/stat = 450/50 = 9).
   ========================================================================= */
SET @start = DATEADD(DAY, -18, @anchor);
SET @run_label = N'test_critical_nomatch';
SET @end_time = DATEADD(SECOND, 450, @start);   /* 450s / 50 stats = 9 sec/stat */

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>3.5.5.2026.07.01</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <TimeLimit>3600</TimeLimit>
        <ModificationThreshold>5000</ModificationThreshold>
        <TieredThresholds>1</TieredThresholds>
        <SortOrder>MODIFICATION_COUNTER</SortOrder>
        <StatsInParallel>N</StatsInParallel>
        <Preset>NIGHTLY</Preset>
        <CriticalTables>dbo.NonExistentTable</CriticalTables>
        <CriticalSamplePercent>100</CriticalSamplePercent>
        <CriticalTablesFirst>Y</CriticalTablesFirst>
        <FirstTimeFullScanCapRows>10000000</FirstTimeFullScanCapRows>
    </Parameters>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>3.5.5.2026.07.01</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>50</StatsFound>
        <StatsProcessed>50</StatsProcessed>
        <StatsSucceeded>50</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>0</StatsRemaining>
        <DurationSeconds>450</DurationSeconds>
        <StopReason>COMPLETED</StopReason>
    </Summary>');

/* Per-stat rows: ALL have IsCritical=0 -- pattern matched nothing */
SET @j = 0;
WHILE @j < 3
BEGIN
    INSERT INTO dbo.CommandLog (
        CommandType, Command, StartTime, EndTime,
        DatabaseName, SchemaName, ObjectName, StatisticsName,
        ErrorNumber, ErrorMessage, ExtendedInfo
    ) VALUES (
        N'UPDATE_STATISTICS',
        N'UPDATE STATISTICS [TestDB].[dbo].[SomeTable] WITH SAMPLE 25 PERCENT',
        DATEADD(SECOND, @j * 50, @start),
        DATEADD(SECOND, @j * 50 + 45, @start),
        N'TestDB', N'dbo', N'SomeTable',
        N'IX_SomeTable_Col' + CONVERT(nvarchar(2), @j),
        NULL, NULL,
        N'<ExtendedInfo>
            <RunLabel>' + @run_label + N'</RunLabel>
            <QualifyReason>MODIFICATION_COUNTER</QualifyReason>
            <RowCount>100000</RowCount>
            <ModificationCounter>5500</ModificationCounter>
            <PageCount>1000</PageCount>
            <SizeMB>8</SizeMB>
            <DaysStale>3</DaysStale>
            <HasNorecompute>0</HasNorecompute>
            <IsHeap>0</IsHeap>
            <ProcessingPosition>' + CONVERT(nvarchar(10), @j + 1) + N'</ProcessingPosition>
            <IsCritical>0</IsCritical>
            <CriticalSampleOverride>0</CriticalSampleOverride>
        </ExtendedInfo>'
    );
    SET @j = @j + 1;
END;

RAISERROR(N'  Run 27: CriticalTables run with ZERO IsCritical stats (W16 no-match, -18 days)', 10, 1) WITH NOWAIT;

/* =========================================================================
   RUN 28: Pre-v3 style run -- NO Parameters element for gated params
   Tests 9ufm: W1/I4 checks must NOT fire for this run even though
   TieredThresholds, TimeLimit, Preset, CriticalTables values are NULL.
   Date: -19 days (outside C4 window; sec/stat = 450/50 = 9).
   Note: This run is v1-style; Parameters element present but minimal
   (no TieredThresholds, no TimeLimit, no Preset, no CriticalTables elements).
   ========================================================================= */
SET @start = DATEADD(DAY, -19, @anchor);
SET @run_label = N'test_pre_v3_element_gate';
SET @end_time = DATEADD(SECOND, 450, @start);

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
    N'<Parameters>
        <Version>1.5.0</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <Databases>USER_DATABASES</Databases>
        <StatsInParallel>N</StatsInParallel>
    </Parameters>');
/* NOTE: No TimeLimit, TieredThresholds, Preset, CriticalTables, FirstTimeFullScanCapRows elements.
   The .exist() flags will all be 0 so W1a, W1b, I4a, I4c, I4d must NOT fire for this run. */

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
    N'<Summary>
        <Version>1.5.0</Version>
        <RunLabel>' + @run_label + N'</RunLabel>
        <StatsFound>50</StatsFound>
        <StatsProcessed>50</StatsProcessed>
        <StatsSucceeded>50</StatsSucceeded>
        <StatsFailed>0</StatsFailed>
        <StatsRemaining>0</StatsRemaining>
        <DurationSeconds>450</DurationSeconds>
        <StopReason>COMPLETED</StopReason>
    </Summary>');

RAISERROR(N'  Run 28: Pre-v3 minimal Parameters element (9ufm gate test, -19 days)', 10, 1) WITH NOWAIT;

/* =========================================================================
   SUMMARY
   ========================================================================= */
RAISERROR(N'', 10, 1) WITH NOWAIT;
RAISERROR(N'Test data setup complete:', 10, 1) WITH NOWAIT;

DECLARE @msg nvarchar(200);
SET @msg = N'  Total CommandLog rows: ' + CONVERT(nvarchar(10), (SELECT COUNT(*) FROM dbo.CommandLog));
RAISERROR(@msg, 10, 1) WITH NOWAIT;
SET @msg = N'  START records: ' + CONVERT(nvarchar(10), (SELECT COUNT(*) FROM dbo.CommandLog WHERE CommandType = 'SP_STATUPDATE_START'));
RAISERROR(@msg, 10, 1) WITH NOWAIT;
SET @msg = N'  END records: ' + CONVERT(nvarchar(10), (SELECT COUNT(*) FROM dbo.CommandLog WHERE CommandType = 'SP_STATUPDATE_END'));
RAISERROR(@msg, 10, 1) WITH NOWAIT;
SET @msg = N'  UPDATE_STATISTICS: ' + CONVERT(nvarchar(10), (SELECT COUNT(*) FROM dbo.CommandLog WHERE CommandType = 'UPDATE_STATISTICS'));
RAISERROR(@msg, 10, 1) WITH NOWAIT;

RAISERROR(N'', 10, 1) WITH NOWAIT;
RAISERROR(N'Expected diagnostics:', 10, 1) WITH NOWAIT;
RAISERROR(N'  C1: Killed runs (2 - one true kill, one orphan cleanup)', 10, 1) WITH NOWAIT;
RAISERROR(N'  C2: Repeated failures (IX_BigTable_Status in 3+ runs)', 10, 1) WITH NOWAIT;
RAISERROR(N'  C4: Degrading throughput (recent ~19 sec/stat vs prior ~6 sec/stat)', 10, 1) WITH NOWAIT;
RAISERROR(N'  W1: TieredThresholds off, no TimeLimit (run 11)', 10, 1) WITH NOWAIT;
RAISERROR(N'  W2: Long-running stats (HugeTable - 25 min)', 10, 1) WITH NOWAIT;
RAISERROR(N'  W4: Overlapping runs (run 5 + run 10) -- NOT runs 18+19 (parallel pair suppressed)', 10, 1) WITH NOWAIT;
RAISERROR(N'  I4: Presets not in use', 10, 1) WITH NOWAIT;
RAISERROR(N'  I5: Version history (1.6 + 1.9 + 2.15 + 2.24)', 10, 1) WITH NOWAIT;
RAISERROR(N'  I6: QS efficacy (5 QS-priority runs with ProcessingPosition)', 10, 1) WITH NOWAIT;
RAISERROR(N'  I7: QS inflection (3 non-QS runs before, 5 QS runs after)', 10, 1) WITH NOWAIT;
RAISERROR(N'  W6: Excessive overhead (run 17 — 300s wall clock, ~5s stats)', 10, 1) WITH NOWAIT;
RAISERROR(N'  I8: QS performance trend (per-execution CPU across runs)', 10, 1) WITH NOWAIT;
RAISERROR(N'  Parallel runs 18-20 (PARALLEL_COMPLETE, StatsInParallel=Y):', 10, 1) WITH NOWAIT;
RAISERROR(N'    - W4 suppressed for runs 18+19 (both parallel, same job window)', 10, 1) WITH NOWAIT;
RAISERROR(N'    - W3 suppressed (all PARALLEL_COMPLETE with StatsRemaining=0)', 10, 1) WITH NOWAIT;
RAISERROR(N'    - Completion grade honors PARALLEL_COMPLETE aggregate = done', 10, 1) WITH NOWAIT;
RAISERROR(N'  Runs 21-23: WarningsCodes=BACKUP_RUNNING|TOCTOU_SKIPS (W15/I16 should fire)', 10, 1) WITH NOWAIT;
RAISERROR(N'  Run 24-25: TEMPDB_PRESSURE x2 (W14 should fire)', 10, 1) WITH NOWAIT;
RAISERROR(N'  Skip markers: 2 UPDATE_STATISTICS rows (SKIPPED: + TOCTOU_SKIP) excluded from #stat_updates (70hh)', 10, 1) WITH NOWAIT;
RAISERROR(N'  Run 26: CriticalTables run with IsCritical=1 stats -> I17 CRITICAL_TABLES_COVERAGE (sw82)', 10, 1) WITH NOWAIT;
RAISERROR(N'  Run 27: CriticalTables=dbo.NonExistentTable, zero IsCritical -> W16 CRITICAL_TABLES_NO_MATCH (sw82)', 10, 1) WITH NOWAIT;
RAISERROR(N'  Run 28: Pre-v3 minimal Parameters (no TimeLimit/TieredThresholds/Preset elements) -> 9ufm gates hold W1/I4 findings', 10, 1) WITH NOWAIT;
GO
