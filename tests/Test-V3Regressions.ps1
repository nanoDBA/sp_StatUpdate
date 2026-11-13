[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Server,
    [string[]]$Versions = @('2022'),
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [Parameter(Mandatory)][string]$ResultsPath
)

$ErrorActionPreference = 'Stop'
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }
if ($Versions.Count -ne 1) { throw 'Run one supported SQL version at a time.' }
$version = $Versions[0]
$port = @{ '2019' = 1434; '2022' = 1432; '2025' = 1431 }[$version]
if (-not $port) { throw 'Unsupported SQL Server version.' }
$repoRoot = Split-Path -Parent $PSScriptRoot
$started = Get-Date
$lastCheck = $started
$checks = [System.Collections.Generic.List[object]]::new()
$sqlBuild = 'unavailable'
$aliveAtEnd = $false
$suffix = [guid]::NewGuid().ToString('N').Substring(0, 10)
$tinyName = "ST_TinyBaseline_$suffix"
$clampName = "ST_ClampMe_$suffix"
$orphanName = "ST_OrphanSpan_$suffix"

function Add-Check([string]$Name, [string]$Result, [string]$Reason = '') {
    $now = Get-Date
    $item = [ordered]@{ check = $Name; result = $Result; duration_ms = [long][Math]::Max(0, ($now - $script:lastCheck).TotalMilliseconds) }
    if ($Result -ne 'PASS') { $item.reason = $Reason }
    $script:checks.Add([pscustomobject]$item)
    $script:lastCheck = $now
}

function New-SqlConnection {
    $connectionString = "Server=$Server,$port;Database=master;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Connection Timeout=15;Pooling=false"
    return [System.Data.SqlClient.SqlConnection]::new($connectionString)
}

function Invoke-SqlDataSet([string]$Sql, [int]$TimeoutSec = 180) {
    $connection = New-SqlConnection
    try {
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandText = $Sql
        $command.CommandTimeout = $TimeoutSec
        $adapter = [System.Data.SqlClient.SqlDataAdapter]::new($command)
        $dataSet = [System.Data.DataSet]::new()
        [void]$adapter.Fill($dataSet)
        return $dataSet
    } finally {
        if ($adapter) { $adapter.Dispose() }
        if ($command) { $command.Dispose() }
        $connection.Dispose()
    }
}

function Invoke-SqlScalar([string]$Sql) {
    $connection = New-SqlConnection
    try {
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandText = $Sql
        return $command.ExecuteScalar()
    } finally { $connection.Dispose() }
}

function Get-DiagDataSet([int]$DaysBack, [int]$ThroughputWindowDays = 7) {
    $query = "EXEC dbo.sp_StatUpdate_Diag @DaysBack=$DaysBack, @ThroughputWindowDays=$ThroughputWindowDays, @ExpertMode=1, @SkipHistory=1;"
    return Invoke-SqlDataSet $query 240
}

function Add-RegressionFixtures {
    $sql = @"
DECLARE @now datetime2(3) = SYSDATETIME(), @i int = 1, @t0 datetime2(3), @lbl nvarchar(100), @cpu bigint;
WHILE @i <= 2
BEGIN
    SET @t0 = DATEADD(HOUR, -6 * @i, @now);
    SET @lbl = N'v3reg_$suffix' + N'_' + CONVERT(nvarchar(10), @i);
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, ExtendedInfo)
    VALUES (N'DiagTest', N'dbo', N'sp_StatUpdate', N'P', N'EXECUTE dbo.sp_StatUpdate', N'SP_STATUPDATE_START', @t0,
        CONVERT(xml, N'<Parameters><Version>3.11.2</Version><RunLabel>' + @lbl + N'</RunLabel><Databases>DiagTest</Databases></Parameters>'));
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ExtendedInfo)
    VALUES (N'DiagTest', N'dbo', N'sp_StatUpdate', N'P', N'sp_StatUpdate completed: COMPLETED', N'SP_STATUPDATE_END', @t0, DATEADD(MINUTE, 10, @t0),
        CONVERT(xml, N'<Summary><Version>3.11.2</Version><RunLabel>' + @lbl + N'</RunLabel><StatsFound>2</StatsFound><StatsProcessed>2</StatsProcessed><StatsSucceeded>2</StatsSucceeded><StatsFailed>0</StatsFailed><StatsRemaining>0</StatsRemaining><DurationSeconds>600</DurationSeconds><StopReason>COMPLETED</StopReason></Summary>'));
    SET @cpu = CASE WHEN @i = 2 THEN 2 ELSE 50000000000 END;
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ErrorNumber, StatisticsName, ExtendedInfo)
    VALUES (N'DiagTest', N'dbo', N'Orders', N'U', N'UPDATE STATISTICS x', N'UPDATE_STATISTICS', DATEADD(MINUTE, 2, @t0), DATEADD(MINUTE, 3, @t0), 0, N'$tinyName',
        CONVERT(xml, N'<ExtendedInfo><RunLabel>' + @lbl + N'</RunLabel><Version>3.11.2</Version><QSPlanCount>1</QSPlanCount><QSTotalExecutions>1</QSTotalExecutions><QSTotalCpuMs>' + CONVERT(nvarchar(20), @cpu) + N'</QSTotalCpuMs></ExtendedInfo>'));
    SET @cpu = CASE WHEN @i = 2 THEN 200 ELSE 100000 END;
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ErrorNumber, StatisticsName, ExtendedInfo)
    VALUES (N'DiagTest', N'dbo', N'Orders', N'U', N'UPDATE STATISTICS x', N'UPDATE_STATISTICS', DATEADD(MINUTE, 4, @t0), DATEADD(MINUTE, 5, @t0), 0, N'$clampName',
        CONVERT(xml, N'<ExtendedInfo><RunLabel>' + @lbl + N'</RunLabel><Version>3.11.2</Version><QSPlanCount>1</QSPlanCount><QSTotalExecutions>1</QSTotalExecutions><QSTotalCpuMs>' + CONVERT(nvarchar(20), @cpu) + N'</QSTotalCpuMs></ExtendedInfo>'));
    SET @i += 1;
END;
INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ErrorNumber, StatisticsName, ExtendedInfo)
VALUES (N'DiagTest', N'dbo', N'Orders', N'U', N'UPDATE STATISTICS x', N'UPDATE_STATISTICS', DATEADD(DAY, -26, @now), @now, 0, N'$orphanName',
    CONVERT(xml, N'<ExtendedInfo><RunLabel>v3reg_$suffix' + N'</RunLabel><Version>3.11.2</Version></ExtendedInfo>'));
"@
    [void](Invoke-SqlDataSet $sql 120)
}

try {
    $probe = New-SqlConnection
    $probe.Open()
    $command = $probe.CreateCommand()
    $command.CommandText = "SELECT CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion'));"
    $sqlBuild = [string]$command.ExecuteScalar()
    $aliveAtEnd = $true
    $command.Dispose()
    $probe.Dispose()

    Add-RegressionFixtures

    try {
        $diag30 = Get-DiagDataSet 30
        $hasRows = $diag30.Tables.Count -gt 0
        if ($hasRows) { Add-Check '0859.9' 'PASS' }
        else { Add-Check '0859.9' 'FAIL' 'Diag returned no result sets for a 26-day CommandLog span.' }
    } catch {
        $reason = $_.Exception.Message.Replace($Password, '[redacted]')
        Add-Check '0859.9' 'FAIL' "Diag failed on the 26-day CommandLog span ($reason)."
    }

    try {
        $diag1 = Get-DiagDataSet 2 1
        $rs13 = $null
        foreach ($table in $diag1.Tables) {
            if ($table.Columns.Contains('CpuChangePct')) { $rs13 = $table; break }
        }
        $tiny = $null
        $clamped = $null
        if ($rs13) {
            foreach ($row in $rs13.Rows) {
                if ([string]$row['StatisticsName'] -eq $tinyName) { $tiny = $row }
                if ([string]$row['StatisticsName'] -eq $clampName) { $clamped = $row }
            }
        }
        $tinyNull = $null -ne $tiny -and $tiny['CpuChangePct'] -is [System.DBNull]
        $clampAtLimit = $null -ne $clamped -and $clamped['CpuChangePct'] -isnot [System.DBNull] -and [decimal]$clamped['CpuChangePct'] -eq 1000.0
        if ($tinyNull -and $clampAtLimit) { Add-Check '0859.10' 'PASS' }
        else { Add-Check '0859.10' 'FAIL' "Expected tiny-baseline NULL and +1000.0 clamp (tiny_found=$($null -ne $tiny); tiny_null=$tinyNull; clamp_found=$($null -ne $clamped); clamp_ok=$clampAtLimit)." }
    } catch {
        $reason = $_.Exception.Message.Replace($Password, '[redacted]')
        Add-Check '0859.10' 'FAIL' "Diag failed the ChangePct floor/clamp probe ($reason)."
    }

    try {
        $definition = [string](Invoke-SqlScalar "SELECT OBJECT_DEFINITION(OBJECT_ID(N'dbo.sp_StatUpdate'));" )
        $filter = [regex]::Match($definition, '(?is)pf2\.state_desc\s+IN\s*\(([^)]*)\)')
        $required = @('PENDING_VALIDATION','IN_VALIDATION','VERIFICATION_PASSED','FEEDBACK_VALID')
        $missing = @($required | Where-Object { -not $filter.Success -or $filter.Groups[1].Value -notmatch [regex]::Escape($_) })
        $hasInvalidState = $filter.Success -and $filter.Groups[1].Value -match '(?i)ACTIVE'
        if ($filter.Success -and $missing.Count -eq 0 -and -not $hasInvalidState) { Add-Check 'Plan-feedback state filter uses documented states' 'PASS' }
        else { Add-Check 'Plan-feedback state filter uses documented states' 'FAIL' ('Plan-feedback filter missing documented states or includes nonexistent ACTIVE (missing={0}; active={1}).' -f ($missing -join ','),$hasInvalidState) }
    } catch {
        $reason = $_.Exception.Message.Replace($Password, '[redacted]')
        Add-Check 'Plan-feedback state filter uses documented states' 'FAIL' "Could not verify the plan-feedback state filter ($reason)."
    }
} catch {
    $reason = $_.Exception.Message.Replace($Password, '[redacted]')
    foreach ($name in @('0859.9','0859.10','Plan-feedback state filter uses documented states')) {
        if (@($checks | Where-Object check -eq $name).Count -eq 0) { Add-Check $name 'FAIL' "Regression fixture setup failed ($reason)." }
    }
} finally {
    if ($probe) { $probe.Dispose() }
    $aliveAtEnd = $false
    try {
        $probe = New-SqlConnection; $probe.Open()
        $command = $probe.CreateCommand(); $command.CommandText = "SELECT CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion'));"
        $sqlBuild = [string]$command.ExecuteScalar(); $aliveAtEnd = $true
    } catch { }
    finally { if ($command) { $command.Dispose() }; if ($probe) { $probe.Dispose() } }
}

if ($checks.Count -ne 3) { throw "Expected three V3 regression checks; received $($checks.Count)." }
$commit = (& git -C $repoRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
Import-Module (Join-Path $PSScriptRoot 'ReleaseResults.psm1') -Force
$ended = Get-Date
[void](Write-ReleaseRunList -Path $ResultsPath -Commit $commit -SqlVersion $version -SqlBuild $sqlBuild `
    -ServerAliveAtEnd $aliveAtEnd -Suite 'V3Regression' -ExpectedCount 3 -Checks @($checks) `
    -StartedAt $started -EndedAt $ended -DurationMs ([long](($ended - $started).TotalMilliseconds)))
if (@($checks | Where-Object result -ne 'PASS').Count) { exit 1 }
exit 0
