[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Server,
    [Parameter(Mandatory)][int]$Port,
    [string]$Password = $env:SPSTATUPDATE_TEST_SQL_PASSWORD,
    [string]$Database = 'master',
    [Parameter(Mandatory)][ValidateSet('2019','2022','2025')][string]$SqlVersion,
    [Parameter(Mandatory)][string]$Commit,
    [Parameter(Mandatory)][string]$ResultsPath
)

$ErrorActionPreference = 'Stop'
$startedAt = [DateTime]::UtcNow
if (-not $Password) { throw 'Release-gate SQL credential is unavailable.' }
$connectionString = "Server=$Server,$Port;Database=$Database;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
$results = [System.Collections.Generic.List[object]]::new()

function Add-Result([string]$Name, [string]$Result, [string]$Detail = '') {
    $results.Add([pscustomobject]@{ Test = $Name; Result = $Result; Detail = $Detail })
    Write-Host "[$Result] $Name $Detail"
}

function Invoke-Query([string]$Sql) {
    $connection = [System.Data.SqlClient.SqlConnection]::new($connectionString)
    try {
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandTimeout = 120
        $command.CommandText = $Sql
        $reader = $command.ExecuteReader()
        $tables = [System.Collections.Generic.List[System.Data.DataTable]]::new()
        do {
            $table = [System.Data.DataTable]::new()
            $null = $table.Load($reader)
            $tables.Add($table)
        } while (-not $reader.IsClosed)
        $reader.Dispose()
        return ,$tables
    } finally {
        $connection.Dispose()
    }
}

function Invoke-NonQuery([string]$Sql) {
    $connection = [System.Data.SqlClient.SqlConnection]::new($connectionString)
    try {
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandTimeout = 120
        $command.CommandText = $Sql
        [void]$command.ExecuteNonQuery()
    } finally {
        $connection.Dispose()
    }
}

try {
    $status = Invoke-Query "SELECT CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')) AS ProductVersion, CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS ProductMajorVersion;"
    $serverBuild = [string]$status[0].Rows[0]['ProductVersion']
    $majorToVersion = @{ 15 = '2019'; 16 = '2022'; 17 = '2025' }
    if ($majorToVersion[[int]$status[0].Rows[0]['ProductMajorVersion']] -ne $SqlVersion) {
        throw 'Connected SQL version did not match the requested release-gate version.'
    }

    # A fresh release-gate base must not contain prior CommandLog rows. Create
    # the fixture table only when absent; refuse to clear any existing records.
    Invoke-NonQuery @"
IF OBJECT_ID(N'dbo.CommandLog') IS NULL
BEGIN
    CREATE TABLE dbo.CommandLog
    (
        ID int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        DatabaseName sysname NULL, SchemaName sysname NULL, ObjectName sysname NULL,
        ObjectType char(2) NULL, IndexName sysname NULL, IndexType tinyint NULL,
        StatisticsName sysname NULL, PartitionNumber int NULL, ExtendedInfo xml NULL,
        Command nvarchar(max) NOT NULL, CommandType nvarchar(60) NOT NULL,
        StartTime datetime2(7) NOT NULL, EndTime datetime2(7) NULL,
        ErrorNumber int NULL, ErrorMessage nvarchar(max) NULL
    );
END;
IF EXISTS (SELECT 1 FROM dbo.CommandLog)
    THROW 51000, 'Target-check suite requires a fresh empty CommandLog.', 1;
"@

    # Produce actual writer rows on the disposable release-gate clone. A dry run
    # intentionally emits no start/end history, so execute against the isolated
    # StatUpdateTest database only; the clone is removed by the labeled wrapper.
    $null = Invoke-Query "EXECUTE dbo.sp_StatUpdate @Databases=N'StatUpdateTest', @LogToTable=N'Y', @Execute=N'Y';"
    $realRows = Invoke-Query "SELECT StartRows=COALESCE(SUM(CASE WHEN CommandType=N'SP_STATUPDATE_START' THEN 1 ELSE 0 END),0), EndRows=COALESCE(SUM(CASE WHEN CommandType=N'SP_STATUPDATE_END' THEN 1 ELSE 0 END),0) FROM dbo.CommandLog;"
    $hasRealStartEnd = ([int]$realRows[0].Rows[0]['StartRows'] -gt 0 -and [int]$realRows[0].Rows[0]['EndRows'] -gt 0)
    $liveTables = Invoke-Query 'EXECUTE dbo.sp_StatUpdate_Diag @DaysBack=30, @ExpertMode=0, @SkipHistory=0;'
    $dashboardColumns = if ($liveTables.Count -gt 0) { @($liveTables[0].Columns | ForEach-Object { $_.ColumnName }) } else { @() }
    $recommendationColumns = if ($liveTables.Count -gt 1) { @($liveTables[1].Columns | ForEach-Object { $_.ColumnName }) } else { @() }
    $missingDashboard = @('Grade','Score','Headline') | Where-Object { $_ -notin $dashboardColumns }
    $missingRecommendations = @('Severity','Finding','Recommendation') | Where-Object { $_ -notin $recommendationColumns }
    $documentedShape = $liveTables.Count -eq 2 -and $missingDashboard.Count -eq 0 -and $missingRecommendations.Count -eq 0
    Add-Result "Live smoke: Diag reads actual dry-run CommandLog rows with documented result shapes" `
        $(if ($hasRealStartEnd -and $documentedShape) {'PASS'} else {'FAIL'}) `
        "Start/END rows=$hasRealStartEnd; two-set dashboard/recommendations shape=$documentedShape; sets=$($liveTables.Count)."

    Invoke-NonQuery @"
DECLARE @runs TABLE
(
    RunLabel nvarchar(100), OffsetDays int, StopReason nvarchar(50),
    StatsFound int, StatsProcessed int NULL, StatsSucceeded int,
    MopUpTriggered bit, MopUpFound int, MopUpProcessed int
);
INSERT INTO @runs VALUES
    (N'target_c6', -12, N'COMPLETED', 100, 0, 0, 0, 0, 0),
    (N'target_i18', -10, N'COMPLETED', 100, NULL, 0, 0, 0, 0),
    (N'target_w11_1', -6, N'COMPLETED', 10, 5, 5, 1, 10, 4),
    (N'target_w11_2', -4, N'COMPLETED', 10, 5, 5, 1, 10, 4),
    (N'target_w11_3', -2, N'COMPLETED', 10, 5, 5, 1, 10, 4);

DECLARE @label nvarchar(100), @days int, @reason nvarchar(50),
        @found int, @processed int, @succeeded int,
        @mopup bit, @mopup_found int, @mopup_processed int,
        @start datetime2(3), @end datetime2(3);
DECLARE run_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT RunLabel, OffsetDays, StopReason, StatsFound, StatsProcessed,
           StatsSucceeded, MopUpTriggered, MopUpFound, MopUpProcessed
    FROM @runs;
OPEN run_cursor;
FETCH NEXT FROM run_cursor INTO @label, @days, @reason, @found, @processed,
    @succeeded, @mopup, @mopup_found, @mopup_processed;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @start = DATEADD(DAY, @days, SYSDATETIME());
    SET @end = DATEADD(SECOND, 600, @start);
    INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
    VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
        N'<Parameters><Version>3.11.3.2026.11.06</Version><RunLabel>' + @label +
        N'</RunLabel><Databases>USER_DATABASES</Databases><TimeLimit>3600</TimeLimit>' +
        N'<ModificationThreshold>5000</ModificationThreshold><TieredThresholds>1</TieredThresholds>' +
        N'<ThresholdLogic>OR</ThresholdLogic><SortOrder>MODIFICATION_COUNTER</SortOrder>' +
        N'<QueryStorePriority>N</QueryStorePriority><StatsInParallel>N</StatsInParallel></Parameters>');
    INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
    VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end,
        N'<Summary><Version>3.11.3.2026.11.06</Version><RunLabel>' + @label + N'</RunLabel>' +
        N'<StatsFound>' + CONVERT(nvarchar(20), @found) + N'</StatsFound>' +
        CASE WHEN @processed IS NULL THEN N'' ELSE N'<StatsProcessed>' + CONVERT(nvarchar(20), @processed) + N'</StatsProcessed>' END +
        N'<StatsSucceeded>' + CONVERT(nvarchar(20), @succeeded) + N'</StatsSucceeded><StatsFailed>0</StatsFailed>' +
        N'<StatsRemaining>0</StatsRemaining><DurationSeconds>600</DurationSeconds><StopReason>' + @reason + N'</StopReason>' +
        N'<MopUpTriggered>' + CONVERT(nvarchar(1), @mopup) + N'</MopUpTriggered>' +
        N'<MopUpFound>' + CONVERT(nvarchar(20), @mopup_found) + N'</MopUpFound>' +
        N'<MopUpProcessed>' + CONVERT(nvarchar(20), @mopup_processed) + N'</MopUpProcessed></Summary>');
    FETCH NEXT FROM run_cursor INTO @label, @days, @reason, @found, @processed,
        @succeeded, @mopup, @mopup_found, @mopup_processed;
END;
CLOSE run_cursor;
DEALLOCATE run_cursor;

INSERT INTO dbo.CommandLog
    (CommandType, Command, StartTime, EndTime, DatabaseName, SchemaName,
     ObjectName, StatisticsName, ErrorNumber, ExtendedInfo)
VALUES
    (N'UPDATE_STATISTICS', N'UPDATE STATISTICS [heap]', DATEADD(DAY, -5, SYSDATETIME()), DATEADD(SECOND, 90, DATEADD(DAY, -5, SYSDATETIME())),
     N'DiagDb', N'dbo', N'HeapTable', N'HeapStat', 0,
     N'<ExtendedInfo><RunLabel>target_w11_1</RunLabel><RowCount>100</RowCount><ModificationCounter>10</ModificationCounter>' +
     N'<PageCount>10</PageCount><SizeMB>1</SizeMB><DaysStale>1</DaysStale><HasNorecompute>0</HasNorecompute>' +
     N'<IsHeap>1</IsHeap><DurationMs>90000</DurationMs><QSPlanCount>0</QSPlanCount></ExtendedInfo>'),
    (N'UPDATE_STATISTICS', N'UPDATE STATISTICS [clustered]', DATEADD(DAY, -5, SYSDATETIME()), DATEADD(SECOND, 1, DATEADD(DAY, -5, SYSDATETIME())),
     N'DiagDb', N'dbo', N'ClusteredTable', N'ClusteredStat', 0,
     N'<ExtendedInfo><RunLabel>target_w11_1</RunLabel><RowCount>100</RowCount><ModificationCounter>10</ModificationCounter>' +
     N'<PageCount>10</PageCount><SizeMB>1</SizeMB><DaysStale>1</DaysStale><HasNorecompute>0</HasNorecompute>' +
     N'<IsHeap>0</IsHeap><DurationMs>1000</DurationMs><QSPlanCount>0</QSPlanCount></ExtendedInfo>');
"@

    $tables = Invoke-Query 'EXECUTE dbo.sp_StatUpdate_Diag @DaysBack=30, @ExpertMode=1, @SkipHistory=1;'
    $recommendations = $null
    foreach ($table in $tables) {
        if ($table.Columns.Contains('Category') -and $table.Columns.Contains('Finding')) {
            $recommendations = $table
            break
        }
    }
    if (-not $recommendations) { throw 'Diag did not return its Recommendations result set.' }
    $categories = @($recommendations.Rows | ForEach-Object { [string]$_.Category })
    Add-Result "C6 SILENT_NOOP_RUN fires for confirmed zero processing" $(if ($categories -contains 'SILENT_NOOP_RUN') {'PASS'} else {'FAIL'}) "Synthetic completed run has StatsFound>0 and StatsProcessed=0."
    Add-Result "I18 NOOP_UNCONFIRMED fires when END omits StatsProcessed" $(if ($categories -contains 'NOOP_UNCONFIRMED') {'PASS'} else {'FAIL'}) "Synthetic completed run omits StatsProcessed and has no nearby worker rows."
    Add-Result "W11 MOPUP_LOW_YIELD fires across three low-yield runs" $(if ($categories -contains 'MOPUP_LOW_YIELD') {'PASS'} else {'FAIL'}) "Three runs each process 4 of 10 mop-up stats."
    Add-Result "W12 PRIORITY_PASS_EMPTY fires when mop-up does the work" $(if ($categories -contains 'PRIORITY_PASS_EMPTY') {'PASS'} else {'FAIL'}) "Three runs average one priority-pass stat and four mop-up stats."
    Add-Result "I15 HEAP_TIME_BUDGET fires when heap duration exceeds half" $(if ($categories -contains 'HEAP_TIME_BUDGET') {'PASS'} else {'FAIL'}) "Heap updates consume 90 of 91 measured seconds."
    Add-Result "I19 AG_SECONDARY_BY_DESIGN" 'NOT_APPLICABLE' "Standalone SQL test instances cannot simulate an AG secondary replica."
} finally {
    if ($ResultsPath -and $results.Count -gt 0) {
        Import-Module (Join-Path $PSScriptRoot '..\StructuredLegacyResults.psm1') -Force
        Write-StructuredLegacyResults -Path $ResultsPath -Commit $Commit -SqlVersion $SqlVersion `
            -SqlBuild $serverBuild -ServerAliveAtEnd $true -Suite 'DiagTargetChecks' `
            -ExpectedCount $results.Count -Results @($results) `
            -StartedAt $startedAt -EndedAt ([DateTime]::UtcNow)
    }
}

if (@($results | Where-Object Result -eq 'FAIL').Count -gt 0) { exit 1 }
