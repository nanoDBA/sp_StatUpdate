/*
   Contract-only synthetic history. Keep these rows outside the ordinary test
   windows; the source checker reads this file and the SQL runner can execute it
   on a disposable database to validate the same shape against the Diag reader.
*/
SET NOCOUNT ON;

DECLARE @server_today date = CONVERT(date, SYSDATETIME());
DECLARE @week_anchor datetime2(3) = DATEADD(
    DAY,
    -(DATEDIFF(DAY, CONVERT(date, '19000101'), @server_today) % 7),
    CONVERT(datetime2(3), @server_today)
);
DECLARE @reasons TABLE (StopReason nvarchar(50) NOT NULL);
INSERT INTO @reasons (StopReason)
VALUES
    (N'AG_REDO_QUEUE'), (N'AG_SECONDARY'), (N'BATCH_LIMIT'),
    (N'CONSECUTIVE_FAILURES'), (N'FAIL_FAST'), (N'IO_CORRUPTION'),
    (N'LOG_SPACE_HIGH'), (N'NATURAL_END');

DECLARE @i int = 0;
DECLARE @stop_reason nvarchar(50);
DECLARE @run_label nvarchar(100);
DECLARE @start datetime2(3);
DECLARE reason_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT StopReason FROM @reasons ORDER BY StopReason;
OPEN reason_cursor;
FETCH NEXT FROM reason_cursor INTO @stop_reason;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @i += 1;
    SET @run_label = N'diag_contract_' + CONVERT(nvarchar(10), @i);
    SET @start = DATEADD(HOUR, -5 * @i, DATEADD(DAY, -196, @week_anchor));

    INSERT INTO dbo.CommandLog
        (CommandType, Command, StartTime, EndTime, ExtendedInfo)
    VALUES
        (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
         N'<Parameters>
            <Version>contract</Version><RunLabel>' + @run_label + N'</RunLabel>
            <Databases>USER_DATABASES</Databases><TimeLimit>3600</TimeLimit>
            <ModificationThreshold>5000</ModificationThreshold><TieredThresholds>1</TieredThresholds>
            <ThresholdLogic>OR</ThresholdLogic><SortOrder>MODIFICATION_COUNTER</SortOrder>
            <QueryStorePriority>N</QueryStorePriority><StatsInParallel>N</StatsInParallel>
            <AbortOnIntegrityError>1</AbortOnIntegrityError><BatchLimit>0</BatchLimit>
            <FailFast>0</FailFast><FilteredStatsMode>EXCLUDE</FilteredStatsMode>
            <LongRunningSamplePercent>100</LongRunningSamplePercent>
            <LongRunningThresholdMinutes>30</LongRunningThresholdMinutes>
            <StatisticsSample>0</StatisticsSample><CriticalSamplePercent>25</CriticalSamplePercent>
            <CriticalTables></CriticalTables><CriticalTablesFirst>0</CriticalTablesFirst>
            <FirstTimeFullScanCapRows>0</FirstTimeFullScanCapRows>
         </Parameters>');

    INSERT INTO dbo.CommandLog
        (CommandType, Command, StartTime, EndTime, ExtendedInfo)
    VALUES
        (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, DATEADD(SECOND, 600, @start),
         N'<Summary>
            <Version>contract</Version><RunLabel>' + @run_label + N'</RunLabel>
            <StatsFound>100</StatsFound><StatsProcessed>90</StatsProcessed>
            <StatsSucceeded>90</StatsSucceeded><StatsFailed>0</StatsFailed>
            <StatsRemaining>10</StatsRemaining><DurationSeconds>600</DurationSeconds>
            <StopReason>' + @stop_reason + N'</StopReason><MopUpTriggered>1</MopUpTriggered>
            <MopUpFound>10</MopUpFound><MopUpProcessed>9</MopUpProcessed>
            <StatsToctou>0</StatsToctou><QSEnrichmentSkipped>0</QSEnrichmentSkipped>
            <WarningsCodes></WarningsCodes><DelayWaitMs>0</DelayWaitMs><AgRedoWaitMs>0</AgRedoWaitMs>
            <RetryBackoffMs>0</RetryBackoffMs><QueueClaimMs>0</QueueClaimMs>
         </Summary>');

    FETCH NEXT FROM reason_cursor INTO @stop_reason;
END;
CLOSE reason_cursor;
DEALLOCATE reason_cursor;

INSERT INTO dbo.CommandLog
    (CommandType, Command, StartTime, EndTime, DatabaseName, SchemaName, ObjectName,
     StatisticsName, ErrorNumber, ErrorMessage, ExtendedInfo)
VALUES
    (N'UPDATE_STATISTICS', N'UPDATE STATISTICS [contract]', @week_anchor, @week_anchor,
     N'ContractDb', N'dbo', N'ContractTable', N'ContractStat', NULL, NULL,
     N'<ExtendedInfo>
        <RunLabel>diag_contract_stat</RunLabel><RowCount>1000</RowCount>
        <ModificationCounter>100</ModificationCounter><PageCount>10</PageCount><SizeMB>1</SizeMB>
        <DaysStale>2</DaysStale><QualifyReason>CONTRACT</QualifyReason>
        <HasNorecompute>0</HasNorecompute><IsHeap>0</IsHeap><AutoCreated>0</AutoCreated>
        <CriticalSampleOverride>0</CriticalSampleOverride><EffectiveSamplePct>25</EffectiveSamplePct>
        <QSPlanCount>1</QSPlanCount><QSTotalExecutions>1</QSTotalExecutions>
        <QSTotalCpuMs>10</QSTotalCpuMs><QSTotalMemoryGrantKB>0</QSTotalMemoryGrantKB>
        <QSTotalTempdbPages>0</QSTotalTempdbPages><ProcessingPosition>1</ProcessingPosition>
        <FilteredDriftRatio>0</FilteredDriftRatio><FromMopUp>0</FromMopUp>
        <HasFilter>0</HasFilter><IsIncremental>0</IsIncremental><Kind>STAT</Kind>
        <ObjectId>1</ObjectId><SampleSource>CONTRACT</SampleSource>
        <StatsId>1</StatsId><Truncated>0</Truncated>
     </ExtendedInfo>');

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES
    (N'SP_STATUPDATE_REMAINING', N'contract remaining work', @week_anchor, @week_anchor,
     N'<Remaining><RunLabel>diag_contract_remaining</RunLabel><Remaining>10</Remaining></Remaining>');
