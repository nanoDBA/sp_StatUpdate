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
$repoRoot = Split-Path -Parent $PSScriptRoot
$started = Get-Date
$lastCheck = $started
$checks = [System.Collections.Generic.List[object]]::new()
$sqlBuild = 'unavailable'
$aliveAtEnd = $false

function Add-Check([string]$Name, [string]$Result, [string]$Reason = '') {
    $now = Get-Date
    $item = [ordered]@{ check = $Name; result = $Result; duration_ms = [long][Math]::Max(0, ($now - $script:lastCheck).TotalMilliseconds) }
    if ($Result -ne 'PASS') { $item.reason = $Reason }
    $script:checks.Add([pscustomobject]$item)
    $script:lastCheck = $now
}

function New-SqlConnection([string]$InitialCatalog = 'master') {
    $connectionString = "Server=$Server,$port;Database=$InitialCatalog;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Connection Timeout=15;Pooling=false"
    return [System.Data.SqlClient.SqlConnection]::new($connectionString)
}

function Invoke-DrainReader([System.Data.SqlClient.SqlCommand]$Command) {
    $reader = $null
    $hasEscape = $false
    try {
        $reader = $Command.ExecuteReader()
        do {
            $fieldCount = $reader.FieldCount
            while ($reader.Read()) {
                for ($field = 0; $field -lt $fieldCount; $field++) {
                    if (-not $reader.IsDBNull($field) -and ([string]$reader.GetValue($field)).Contains([string][char]27)) { $hasEscape = $true }
                }
            }
        } while ($reader.NextResult())
        return $hasEscape
    } finally {
        if ($reader) { $reader.Close() }
    }
}

function Invoke-StatUpdate([string]$Arguments, [int]$TimeoutSec = 180) {
    $connection = New-SqlConnection
    try {
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandText = "EXEC dbo.sp_StatUpdate $Arguments;"
        $command.CommandTimeout = $TimeoutSec
        [void](Invoke-DrainReader $command)
    } finally {
        $connection.Dispose()
    }
}

function Invoke-Scalar([string]$Sql, [hashtable]$Parameters = @{}) {
    $connection = New-SqlConnection
    try {
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandText = $Sql
        foreach ($name in $Parameters.Keys) {
            $parameter = $command.Parameters.Add("@$name", $Parameters[$name].Type)
            $parameter.Value = $Parameters[$name].Value
        }
        return $command.ExecuteScalar()
    } finally {
        $connection.Dispose()
    }
}

function Invoke-SetupSql([string]$Sql) {
    $connection = New-SqlConnection
    try {
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandText = $Sql
        $command.CommandTimeout = 180
        [void]$command.ExecuteNonQuery()
    } finally {
        $connection.Dispose()
    }
}

function Get-ServerTime {
    return [datetime](Invoke-Scalar 'SELECT SYSDATETIME();')
}

function Get-LoggedCommands([string]$Table, [datetime]$Since) {
    $connection = New-SqlConnection
    $items = [System.Collections.Generic.List[object]]::new()
    try {
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandText = @'
SELECT cl.Command, cl.ErrorNumber
FROM dbo.CommandLog AS cl
WHERE cl.DatabaseName = @database
  AND cl.ObjectName = @table
  AND cl.CommandType = N'UPDATE_STATISTICS'
  AND cl.StartTime >= @since
ORDER BY cl.ID;
'@
        [void]$command.Parameters.Add('@database', [System.Data.SqlDbType]::NVarChar, 128)
        [void]$command.Parameters.Add('@table', [System.Data.SqlDbType]::NVarChar, 128)
        [void]$command.Parameters.Add('@since', [System.Data.SqlDbType]::DateTime2)
        $command.Parameters['@database'].Value = $Database
        $command.Parameters['@table'].Value = $Table
        $command.Parameters['@since'].Value = $Since
        $reader = $command.ExecuteReader()
        try {
            while ($reader.Read()) {
                $null = $items.Add([pscustomobject]@{ Command = [string]$reader['Command']; ErrorNumber = [int]$reader['ErrorNumber'] })
            }
        }
        finally { $reader.Close() }
        return $items.ToArray()
    } finally {
        $connection.Dispose()
    }
}

function Get-UpdateCount([string]$Table, [datetime]$Since) {
    return @((Get-LoggedCommands $Table $Since) | Where-Object ErrorNumber -eq 0).Count
}

function New-BehaviorFixtures {
    $sql = @'
USE [StatUpdateTest];
IF OBJECT_ID(N'dbo.ParamBehaviorJob', N'U') IS NOT NULL DROP TABLE dbo.ParamBehaviorJob;
IF OBJECT_ID(N'dbo.ParamBehaviorCCI', N'U') IS NOT NULL DROP TABLE dbo.ParamBehaviorCCI;
IF OBJECT_ID(N'dbo.ParamBehaviorNCCI', N'U') IS NOT NULL DROP TABLE dbo.ParamBehaviorNCCI;
IF OBJECT_ID(N'dbo.ParamBehaviorCap', N'U') IS NOT NULL DROP TABLE dbo.ParamBehaviorCap;

CREATE TABLE dbo.ParamBehaviorJob (ID int NOT NULL, Value int NOT NULL);
CREATE TABLE dbo.ParamBehaviorCCI (ID int NOT NULL, Value int NOT NULL);
CREATE TABLE dbo.ParamBehaviorNCCI (ID int NOT NULL CONSTRAINT PK_ParamBehaviorNCCI PRIMARY KEY CLUSTERED, Value int NOT NULL);
CREATE TABLE dbo.ParamBehaviorCap (ID int NOT NULL, Value int NOT NULL);

;WITH n AS (
    SELECT TOP (20000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b
)
INSERT dbo.ParamBehaviorJob (ID, Value) SELECT n, n % 97 FROM n;
CREATE STATISTICS Stat_ParamBehaviorJob_Value ON dbo.ParamBehaviorJob (Value);
UPDATE dbo.ParamBehaviorJob SET Value = Value + 1 WHERE ID <= 100;

;WITH n AS (
    SELECT TOP (20000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b
)
INSERT dbo.ParamBehaviorCCI (ID, Value) SELECT n, n % 97 FROM n;
CREATE CLUSTERED COLUMNSTORE INDEX CCI_ParamBehaviorCCI ON dbo.ParamBehaviorCCI;
INSERT dbo.ParamBehaviorCCI (ID, Value)
SELECT 20000 + n, n % 97 FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20)) AS n(n);

;WITH n AS (
    SELECT TOP (20000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b
)
INSERT dbo.ParamBehaviorNCCI (ID, Value) SELECT n, n % 97 FROM n;
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_ParamBehaviorNCCI ON dbo.ParamBehaviorNCCI (Value);
UPDATE dbo.ParamBehaviorNCCI SET Value = Value + 1 WHERE ID <= 100;

;WITH n AS (
    SELECT TOP (20000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b
)
INSERT dbo.ParamBehaviorCap (ID, Value) SELECT n, n % 97 FROM n;
CREATE STATISTICS Stat_ParamBehaviorCap_Value ON dbo.ParamBehaviorCap (Value);
'@
    Invoke-SetupSql $sql
}

function Invoke-ColumnstoreProbe([string]$Table, [string]$Parameter, [string]$Value) {
    $since = Get-ServerTime
    $args = "@Databases=N'$Database', @Tables=N'dbo.$Table', @Execute=N'Y', @LogToTable=N'Y', @ModificationThreshold=0, @StatisticsSample=10, @TimeLimit=120, @$Parameter=N'$Value'"
    $expectedColumnstoreError = $false
    try { Invoke-StatUpdate $args }
    catch {
        if ($_.Exception.Message -match '35337|statistics cannot be updated on a columnstore index') { $expectedColumnstoreError = $true }
        else { throw }
    }
    $attempts = @(Get-LoggedCommands $Table $since)
    return [pscustomobject]@{
        Attempts = $attempts.Count
        ExpectedColumnstoreError = $expectedColumnstoreError -or @($attempts | Where-Object ErrorNumber -eq 35337).Count -gt 0
        HasUnexpectedErrors = @($attempts | Where-Object { $_.ErrorNumber -ne 0 -and $_.ErrorNumber -ne 35337 }).Count -gt 0
    }
}

function Invoke-JobNameProbe {
    $connection = New-SqlConnection
    $captureTable = "IF OBJECT_ID(N'dbo.ParamBehaviorJobContext', N'U') IS NOT NULL DROP TABLE dbo.ParamBehaviorJobContext; CREATE TABLE dbo.ParamBehaviorJobContext (ContextInfo varbinary(128) NULL);"
    $dropTrigger = "IF OBJECT_ID(N'dbo.trg_ParamBehaviorJobName', N'TR') IS NOT NULL DROP TRIGGER dbo.trg_ParamBehaviorJobName;"
    $trigger = @"
CREATE TRIGGER dbo.trg_ParamBehaviorJobName ON dbo.CommandLog
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    INSERT [$Database].dbo.ParamBehaviorJobContext (ContextInfo)
    SELECT CONTEXT_INFO()
    FROM inserted
    WHERE DatabaseName = N'$Database'
      AND ObjectName = N'sp_StatUpdate'
      AND CommandType = N'SP_STATUPDATE_START';
END;
"@
    try {
        Invoke-SetupSql "USE [$Database]; $captureTable"
        Invoke-SetupSql $dropTrigger
        Invoke-SetupSql $trigger

        $connection.Open()
        $jobCommand = $connection.CreateCommand()
        $jobCommand.CommandText = "EXEC dbo.sp_StatUpdate @Databases=N'$Database', @Tables=N'dbo.ParamBehaviorJob', @Execute=N'Y', @LogToTable=N'Y', @ModificationThreshold=0, @StatisticsSample=10, @TimeLimit=120, @JobName=N'ParameterBehaviorProof';"
        $jobCommand.CommandTimeout = 180
        [void](Invoke-DrainReader $jobCommand)
        $captureCommand = $connection.CreateCommand()
        $captureCommand.CommandText = "SELECT COUNT_BIG(*) FROM [$Database].dbo.ParamBehaviorJobContext WHERE CHARINDEX(CONVERT(varbinary(128), N'Job:ParameterBehaviorProof'), ContextInfo) > 0;"
        $observed = [long]$captureCommand.ExecuteScalar() -gt 0
        $restoreCommand = $connection.CreateCommand()
        $restoreCommand.CommandText = 'SELECT CONVERT(nvarchar(128), CONTEXT_INFO());'
        $restoredValue = $restoreCommand.ExecuteScalar()
        $restored = if ($null -eq $restoredValue -or $restoredValue -is [DBNull]) { '' } else { [string]$restoredValue }
        return [pscustomobject]@{ Observed = $observed; Restored = [string]::IsNullOrEmpty($restored) }
    } finally {
        try { Invoke-SetupSql $dropTrigger } catch { }
        $connection.Dispose()
    }
}

function Invoke-AnsiProbe([string]$Ansi) {
    $connection = New-SqlConnection
    $messages = [System.Collections.Generic.List[string]]::new()
    try {
        $connection.add_InfoMessage({ param($sender,$eventArgs) $null = $messages.Add([string]$eventArgs.Message) })
        $connection.FireInfoMessageEventOnUserErrors = $true
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandText = "EXEC dbo.sp_StatUpdate @Databases=N'$Database', @Tables=N'dbo.Orders', @Execute=N'N', @LogToTable=N'Y', @Ansi=N'$Ansi';"
        $command.CommandTimeout = 120
        $resultHasEscape = Invoke-DrainReader $command
        $versionCommand = $connection.CreateCommand()
        $versionCommand.CommandText = "SELECT CONVERT(nvarchar(128),SERVERPROPERTY(N'ProductVersion'));"
        $build = [string]$versionCommand.ExecuteScalar()
        return [pscustomobject]@{ Messages = @($messages); ResultHasEscape = $resultHasEscape; Build = $build; Error = $null }
    } catch {
        $detail = $_.Exception.Message.Replace($Password, '[redacted]')
        return [pscustomobject]@{ Messages = @($messages); ResultHasEscape = $false; Build = $null; Error = $detail }
    } finally { $connection.Dispose() }
}

try {
    $colored = Invoke-AnsiProbe 'Y'
    $plain = Invoke-AnsiProbe 'N'
    if ($colored.Error) { throw "ANSI-enabled call failed ($($colored.Error))." }
    if ($plain.Error) { throw "ANSI-disabled call failed ($($plain.Error))." }
    $escape = [string][char]27
    $ansiHasEscape = @($colored.Messages | Where-Object { $_.Contains($escape) }).Count -gt 0
    $plainHasEscape = @($plain.Messages | Where-Object { $_.Contains($escape) }).Count -gt 0
    $coloredHasEscapeInResults = [bool]$colored.ResultHasEscape
    $plainHasEscapeInResults = [bool]$plain.ResultHasEscape
    $sqlBuild = $colored.Build
    if ($ansiHasEscape -and -not $plainHasEscape -and -not $coloredHasEscapeInResults -and -not $plainHasEscapeInResults) { Add-Check '@ansi' 'PASS' }
    else { Add-Check '@ansi' 'FAIL' ("ansi_escape={0}; plain_escape={1}; ansi_result_escape={2}; plain_result_escape={3}; ansi_messages={4}; plain_messages={5}." -f $ansiHasEscape,$plainHasEscape,$coloredHasEscapeInResults,$plainHasEscapeInResults,$colored.Messages.Count,$plain.Messages.Count) }
} catch {
    $reason = $_.Exception.Message.Replace($Password, '[redacted]')
    if (@($checks | Where-Object check -eq '@ansi').Count -eq 0) { Add-Check '@ansi' 'FAIL' "Probe failed ($reason)." }
}

try {
    New-BehaviorFixtures
    $since = Get-ServerTime
    Invoke-StatUpdate "@Databases=N'$Database', @Statistics=N'dbo.ParamBehaviorCap.Stat_ParamBehaviorCap_Value', @Execute=N'Y', @LogToTable=N'Y', @StatisticsSample=100, @FirstTimeFullScanCapRows=1, @TimeLimit=120"
    $commands = Get-LoggedCommands 'ParamBehaviorCap' $since
    $capped = @($commands | Where-Object { $_.ErrorNumber -eq 0 -and $_.Command -match '(?i)\bSAMPLE\s+10\s+PERCENT\b' -and $_.Command -notmatch '(?i)\bFULLSCAN\b' }).Count -gt 0
    if ($capped) { Add-Check '@firsttimefullscancaprows' 'PASS' }
    else { Add-Check '@firsttimefullscancaprows' 'FAIL' 'No logged 10 percent sample command was found for a first-time stat above the configured row threshold.' }
} catch {
    $reason = $_.Exception.Message.Replace($Password, '[redacted]')
    Add-Check '@firsttimefullscancaprows' 'FAIL' "Probe failed ($reason)."
}

try {
    $skipped = Invoke-ColumnstoreProbe 'ParamBehaviorNCCI' 'SkipTablesWithNCCI' 'Y'
    $included = Invoke-ColumnstoreProbe 'ParamBehaviorNCCI' 'SkipTablesWithNCCI' 'N'
    if ($skipped.Attempts -eq 0 -and $included.Attempts -gt 0 -and $included.ExpectedColumnstoreError -and -not $included.HasUnexpectedErrors) { Add-Check '@skiptableswithncci' 'PASS' }
    else { Add-Check '@skiptableswithncci' 'FAIL' "NCCI behavior mismatch (skipped attempts=$($skipped.Attempts); included attempts=$($included.Attempts); expected columnstore error=$($included.ExpectedColumnstoreError); unexpected errors=$($included.HasUnexpectedErrors))." }
} catch {
    $reason = $_.Exception.Message.Replace($Password, '[redacted]')
    Add-Check '@skiptableswithncci' 'FAIL' "Probe failed ($reason)."
}

try {
    $observed = Invoke-JobNameProbe
    if ($observed.Observed -and $observed.Restored) { Add-Check '@jobname' 'PASS' }
    else { Add-Check '@jobname' 'FAIL' "Job context was not visible during execution or caller context was not restored (observed=$($observed.Observed); restored=$($observed.Restored))." }
} catch {
    $reason = $_.Exception.Message.Replace($Password, '[redacted]')
    Add-Check '@jobname' 'FAIL' "Probe failed ($reason)."
}

try {
    $skipped = Invoke-ColumnstoreProbe 'ParamBehaviorCCI' 'SkipTablesWithCCI' 'Y'
    $included = Invoke-ColumnstoreProbe 'ParamBehaviorCCI' 'SkipTablesWithCCI' 'N'
    if ($skipped.Attempts -eq 0 -and $included.Attempts -gt 0 -and $included.ExpectedColumnstoreError -and -not $included.HasUnexpectedErrors) { Add-Check '@skiptableswithcci' 'PASS' }
    else { Add-Check '@skiptableswithcci' 'FAIL' "CCI behavior mismatch (skipped attempts=$($skipped.Attempts); included attempts=$($included.Attempts); expected columnstore error=$($included.ExpectedColumnstoreError); unexpected errors=$($included.HasUnexpectedErrors))." }
} catch {
    $reason = $_.Exception.Message.Replace($Password, '[redacted]')
    Add-Check '@skiptableswithcci' 'FAIL' "Probe failed ($reason)."
}

try {
    $probe = New-SqlConnection
    $probe.Open()
    $cmd = $probe.CreateCommand(); $cmd.CommandText = 'SELECT CONVERT(nvarchar(128),SERVERPROPERTY(N''ProductVersion''));'
    $sqlBuild = [string]$cmd.ExecuteScalar(); $aliveAtEnd = $true
} catch { Write-Warning "SQL liveness probe failed ($($_.Exception.GetType().Name))." }
finally { if ($probe) { $probe.Dispose() } }

if ($ResultsPath) {
    if ($checks.Count -ne 5) { throw "Expected five parameter behavior checks; received $($checks.Count)." }
    $commit = (& git -C $repoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Import-Module (Join-Path $PSScriptRoot 'ReleaseResults.psm1') -Force
    $ended = Get-Date
    [void](Write-ReleaseRunList -Path $ResultsPath -Commit $commit -SqlVersion $version -SqlBuild $sqlBuild `
        -ServerAliveAtEnd $aliveAtEnd -Suite 'ParameterBehavior' -ExpectedCount 5 -Checks @($checks) `
        -StartedAt $started -EndedAt $ended -DurationMs ([long](($ended - $started).TotalMilliseconds)))
}
if (@($checks | Where-Object result -ne 'PASS').Count) { exit 1 }
exit 0
