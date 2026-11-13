[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Server,
    [string[]]$Versions = @('2022'),
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [string]$ResultsPath = '',
    [string]$Database = 'StatUpdateTest'
)

$ErrorActionPreference = 'Stop'
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }
if ($Versions.Count -ne 1) { throw 'Run one supported SQL version at a time.' }
$version = $Versions[0]
$port = @{ '2019' = 1434; '2022' = 1432; '2025' = 1431 }[$version]
if (-not $port) { throw 'Unsupported SQL Server version.' }
$run = [guid]::NewGuid().ToString('N').Substring(0, 10)
$persistTable = "AUP_P_$run"
$autoTable = "AUP_A_$run"
$firstTable = "AUP_F_$run"
$changedTable = "AUP_C_$run"
$preflightTable = "AUP_W_$run"
$evidenceTable = "AUP_E_$run"
$triggerName = "AUP_T_$run"
$repoRoot = Split-Path -Parent $PSScriptRoot
$started = Get-Date
$lastCheck = $started
$checks = [System.Collections.Generic.List[object]]::new()
$aliveAtEnd = $false
$sqlBuild = 'unavailable'

function Invoke-Sql {
    param([string]$Query, [string]$Db = 'master', [int]$Timeout = 120, [switch]$Rows)
    $cs = "Server=$Server,$port;Database=$Db;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Connection Timeout=15;Pooling=false"
    $c = [System.Data.SqlClient.SqlConnection]::new($cs)
    try {
        $c.Open()
        $cmd = $c.CreateCommand(); $cmd.CommandText = $Query; $cmd.CommandTimeout = $Timeout
        if ($Rows) {
            $reader = $cmd.ExecuteReader()
            $table = [System.Data.DataTable]::new(); $table.Load($reader)
            return ,$table
        }
        [void]$cmd.ExecuteNonQuery()
    } finally { $c.Dispose() }
}

function Add-Check([string]$Name, [string]$Result, [string]$Reason = '') {
    $now = Get-Date
    $item = [ordered]@{ check = $Name; result = $Result; duration_ms = [long][Math]::Max(0, ($now - $script:lastCheck).TotalMilliseconds) }
    if ($Result -ne 'PASS') { $item.reason = $Reason }
    $script:checks.Add([pscustomobject]$item)
    $script:lastCheck = $now
}

try {
    $dbOptions = Invoke-Sql "SELECT is_auto_create_stats_on, is_auto_update_stats_on, is_auto_update_stats_async_on FROM sys.databases WHERE name=N'$Database';" 'master' 30 -Rows
    if ($dbOptions.Rows.Count -ne 1) { throw "Required test database '$Database' is missing." }
    $optionsReady = [int]$dbOptions.Rows[0].is_auto_create_stats_on -eq 1 -and
        [int]$dbOptions.Rows[0].is_auto_update_stats_on -eq 1 -and
        [int]$dbOptions.Rows[0].is_auto_update_stats_async_on -eq 0

    $setup = @"
CREATE TABLE dbo.[$persistTable](id int IDENTITY, persist_key int NOT NULL, payload char(100) NOT NULL);
INSERT dbo.[$persistTable](persist_key,payload)
SELECT TOP (100000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 1000, REPLICATE('P',100)
FROM sys.all_objects a CROSS JOIN sys.all_objects b;
CREATE STATISTICS [AUP_PersistStat] ON dbo.[$persistTable](persist_key) WITH FULLSCAN;
INSERT dbo.[$persistTable](persist_key,payload) SELECT TOP (1000) 7, REPLICATE('M',100) FROM sys.all_objects;

CREATE TABLE dbo.[$autoTable](id int IDENTITY, auto_key int NOT NULL);
INSERT dbo.[$autoTable](auto_key) SELECT TOP (5000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 100 FROM sys.all_objects a CROSS JOIN sys.all_objects b;
"@
    Invoke-Sql $setup $Database 180

    Invoke-Sql "EXEC master.dbo.sp_StatUpdate @Databases=N'$Database', @Tables=N'dbo.$persistTable', @StatisticsSample=20, @ModificationThreshold=1, @StaleHours=0, @Execute=N'Y', @LogToTable=N'Y';" 'master' 180
    $before = Invoke-Sql "SELECT p.last_updated, p.persisted_sample_percent FROM sys.dm_db_stats_properties(OBJECT_ID(N'dbo.[$persistTable]'), (SELECT stats_id FROM sys.stats WHERE object_id=OBJECT_ID(N'dbo.[$persistTable]') AND name=N'AUP_PersistStat')) AS p;" $Database 30 -Rows
    if ($before.Rows.Count -ne 1) { throw 'The explicit persisted-sample statistic was not found after sp_StatUpdate.' }

    Start-Sleep -Milliseconds 1200
    Invoke-Sql "INSERT dbo.[$persistTable](persist_key,payload) SELECT TOP (30000) 7, REPLICATE('A',100) FROM sys.all_objects a CROSS JOIN sys.all_objects b; SELECT COUNT_BIG(*) FROM dbo.[$persistTable] WHERE persist_key=7 OPTION (RECOMPILE); SELECT COUNT_BIG(*) FROM dbo.[$autoTable] WHERE auto_key=17 OPTION (RECOMPILE);" $Database 180
    $after = Invoke-Sql "SELECT p.last_updated, p.persisted_sample_percent, (SELECT COUNT(*) FROM sys.stats WHERE object_id=OBJECT_ID(N'dbo.[$autoTable]') AND auto_created=1) AS AutoCreated FROM sys.dm_db_stats_properties(OBJECT_ID(N'dbo.[$persistTable]'), (SELECT stats_id FROM sys.stats WHERE object_id=OBJECT_ID(N'dbo.[$persistTable]') AND name=N'AUP_PersistStat')) AS p;" $Database 30 -Rows
    if ($after.Rows.Count -ne 1) { throw 'The persisted-sample statistic was not found after automatic update.' }
    $updatedAfterAuto = [datetime]$after.Rows[0].last_updated -gt [datetime]$before.Rows[0].last_updated
    $persistedPercent = [int][Math]::Round([double]$after.Rows[0].persisted_sample_percent, 0)
    $autoCreated = [int]$after.Rows[0].AutoCreated -gt 0
    if ($optionsReady -and $updatedAfterAuto -and $persistedPercent -eq 20 -and $autoCreated) {
        Add-Check 'persisted_sample_rate_after_auto_update' 'PASS'
    } else {
        Add-Check 'persisted_sample_rate_after_auto_update' 'FAIL' ('options_ready={0}; auto_update_occurred={1}; persisted_sample_percent={2}; auto_created_stat={3}.' -f $optionsReady, $updatedAfterAuto, $persistedPercent, $autoCreated)
    }

    $toctouSetup = @"
CREATE TABLE dbo.[$firstTable](id int IDENTITY, k int NOT NULL);
CREATE TABLE dbo.[$changedTable](id int IDENTITY, k int NOT NULL);
INSERT dbo.[$firstTable](k) SELECT TOP (100000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM sys.all_objects a CROSS JOIN sys.all_objects b;
INSERT dbo.[$changedTable](k) SELECT TOP (100000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM sys.all_objects a CROSS JOIN sys.all_objects b;
CREATE STATISTICS [AUP_Stat] ON dbo.[$firstTable](k) WITH FULLSCAN;
CREATE STATISTICS [AUP_Stat] ON dbo.[$changedTable](k) WITH FULLSCAN;
INSERT dbo.[$firstTable](k) SELECT TOP (5000) -1 FROM sys.all_objects a CROSS JOIN sys.all_objects b;
INSERT dbo.[$changedTable](k) SELECT TOP (5000) -1 FROM sys.all_objects a CROSS JOIN sys.all_objects b;
CREATE TABLE dbo.[$evidenceTable](DroppedTable sysname NOT NULL);
"@
    Invoke-Sql $toctouSetup $Database 120

    Invoke-Sql "EXEC master.dbo.sp_StatUpdate @Preset=N'OLTP_LIGHT', @Databases=N'$Database', @Tables=N'dbo.$firstTable,dbo.$changedTable', @ModificationThreshold=1, @StaleHours=0, @Execute=N'N', @LogToTable=N'N', @WhatIfOutputTable=N'dbo.$preflightTable';" 'master' 120
    $preflight = Invoke-Sql "SELECT TableName, SequenceNum FROM dbo.[$preflightTable] WHERE TableName IN (N'$firstTable',N'$changedTable') ORDER BY SequenceNum;" 'master' 20 -Rows
    $candidateCount = $preflight.Rows.Count
    $candidateOrder = @($preflight.Rows | ForEach-Object { [string]$_.TableName })
    $commandLogBaseline = Invoke-Sql 'SELECT ISNULL(MAX(ID),0) AS MaxId FROM dbo.CommandLog;' 'master' 20 -Rows
    $commandLogBaselineId = [long]$commandLogBaseline.Rows[0].MaxId

    if ($candidateCount -ne 2) { throw "TOCTOU preflight expected two candidate statistics; received $candidateCount." }
    $trigger = @"
CREATE TRIGGER [$triggerName] ON DATABASE FOR UPDATE_STATISTICS
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM dbo.[$evidenceTable]) RETURN;
    DECLARE @statement nvarchar(max) = EVENTDATA().value('(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]', 'nvarchar(max)');
    IF CHARINDEX(N'$firstTable', @statement) > 0
    BEGIN
        DROP STATISTICS dbo.[$changedTable].[AUP_Stat];
        INSERT dbo.[$evidenceTable](DroppedTable) VALUES (N'$changedTable');
    END
    ELSE IF CHARINDEX(N'$changedTable', @statement) > 0
    BEGIN
        DROP STATISTICS dbo.[$firstTable].[AUP_Stat];
        INSERT dbo.[$evidenceTable](DroppedTable) VALUES (N'$firstTable');
    END;
END;
"@
    Invoke-Sql $trigger $Database 30
    Invoke-Sql "EXEC master.dbo.sp_StatUpdate @Preset=N'OLTP_LIGHT', @Databases=N'$Database', @Tables=N'dbo.$firstTable,dbo.$changedTable', @ModificationThreshold=1, @StaleHours=0, @Execute=N'Y', @LogToTable=N'Y', @Debug=1;" 'master' 240
    $evidence = Invoke-Sql "SELECT DroppedTable FROM dbo.[$evidenceTable];" $Database 20 -Rows
    $targetTable = if ($evidence.Rows.Count -eq 1) { [string]$evidence.Rows[0].DroppedTable } else { $null }

    $targetSucceeded = $false
    $skipRows = [System.Data.DataTable]::new()
    $runLogRows = Invoke-Sql "SELECT TOP (10) CommandType, ObjectName, StatisticsName, ErrorNumber FROM dbo.CommandLog WHERE ID>$commandLogBaselineId ORDER BY ID;" 'master' 20 -Rows
    $runLogSummary = [string]::Join('|', [string[]]@($runLogRows.Rows | ForEach-Object { '{0}:{1}:{2}:{3}' -f $_.CommandType, $_.ObjectName, $_.StatisticsName, $_.ErrorNumber }))
    if ($targetTable) {
        $targetSucceededRows = Invoke-Sql "SELECT COUNT(*) AS SuccessCount FROM dbo.CommandLog WHERE DatabaseName=N'$Database' AND ObjectName=N'$targetTable' AND CommandType=N'UPDATE_STATISTICS' AND ErrorNumber IS NULL AND (ExtendedInfo IS NULL OR ExtendedInfo.value('(/ExtendedInfo/SkipReason/text())[1]','nvarchar(50)') IS NULL);" 'master' 30 -Rows
        $targetSucceeded = [int]$targetSucceededRows.Rows[0].SuccessCount -gt 0
        $skipRows = Invoke-Sql "SELECT COUNT(*) AS SkipCount FROM dbo.CommandLog WHERE DatabaseName=N'$Database' AND ObjectName=N'$targetTable' AND CommandType=N'UPDATE_STATISTICS' AND ExtendedInfo.value('(/ExtendedInfo/SkipReason/text())[1]','nvarchar(50)')=N'TOCTOU_SKIP';" 'master' 30 -Rows
    }
    $wasSkipped = $candidateCount -eq 2 -and $targetTable -and -not $targetSucceeded -and [int]$skipRows.Rows[0].SkipCount -eq 1
    if ($wasSkipped) { Add-Check 'skip_changed_after_discovery' 'PASS' }
    else { Add-Check 'skip_changed_after_discovery' 'FAIL' ('candidate_count={0}; target_dropped_by_ddl_trigger={1}; target_success={2}; skip_log_count={3}; command_log=[{4}].' -f $candidateCount, $targetTable, $targetSucceeded, $(if ($skipRows.Rows.Count) { $skipRows.Rows[0].SkipCount } else { 0 }), $runLogSummary) }
    if ($wasSkipped -and [int]$skipRows.Rows[0].SkipCount -eq 1) { Add-Check 'log_changed_stat_skip' 'PASS' }
    else { Add-Check 'log_changed_stat_skip' 'FAIL' 'The TOCTOU_SKIP CommandLog row was missing or duplicated.' }
} catch {
    $reason = $_.Exception.Message.Replace($Password, '[redacted]')
    foreach ($name in @('persisted_sample_rate_after_auto_update','skip_changed_after_discovery','log_changed_stat_skip')) {
        if (@($checks | Where-Object check -eq $name).Count -eq 0) { Add-Check $name 'FAIL' "Suite setup/execution failed ($reason)." }
    }
} finally {
    try { Invoke-Sql "IF EXISTS (SELECT 1 FROM sys.triggers WHERE name=N'$triggerName') DROP TRIGGER [$triggerName] ON DATABASE;" $Database 30 } catch { }
    try { Invoke-Sql "IF OBJECT_ID(N'dbo.[$preflightTable]',N'U') IS NOT NULL DROP TABLE dbo.[$preflightTable];" 'master' 30 } catch { }
    try { Invoke-Sql "IF OBJECT_ID(N'dbo.[$evidenceTable]',N'U') IS NOT NULL DROP TABLE dbo.[$evidenceTable]; IF OBJECT_ID(N'dbo.[$persistTable]',N'U') IS NOT NULL DROP TABLE dbo.[$persistTable]; IF OBJECT_ID(N'dbo.[$autoTable]',N'U') IS NOT NULL DROP TABLE dbo.[$autoTable]; IF OBJECT_ID(N'dbo.[$firstTable]',N'U') IS NOT NULL DROP TABLE dbo.[$firstTable]; IF OBJECT_ID(N'dbo.[$changedTable]',N'U') IS NOT NULL DROP TABLE dbo.[$changedTable];" $Database 60 } catch { }
    try { $status = Invoke-Sql "SELECT CONVERT(nvarchar(64), SERVERPROPERTY(N'ProductVersion')) AS Build;" 'master' 10 -Rows; $sqlBuild = [string]$status.Rows[0]['Build']; $aliveAtEnd = $true } catch { Write-Warning "Server liveness probe failed ($($_.Exception.GetType().Name))." }
}

if ($ResultsPath) {
    if ($checks.Count -ne 3) { throw "Expected three checks; received $($checks.Count)." }
    $commit = (& git -C $repoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Import-Module (Join-Path $PSScriptRoot 'ReleaseResults.psm1') -Force
    $ended = Get-Date
    [void](Write-ReleaseRunList -Path $ResultsPath -Commit $commit -SqlVersion $version -SqlBuild $sqlBuild `
        -ServerAliveAtEnd $aliveAtEnd -Suite 'AutoUpdatePersistence' -ExpectedCount 3 -Checks @($checks) `
        -StartedAt $started -EndedAt $ended -DurationMs ([long](($ended - $started).TotalMilliseconds)))
}
if (@($checks | Where-Object result -ne 'PASS').Count) { exit 1 }
exit 0
