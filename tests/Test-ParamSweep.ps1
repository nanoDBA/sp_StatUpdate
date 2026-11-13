<#
.SYNOPSIS
    Smoke-check every current input parameter on one supported SQL Server version.

.DESCRIPTION
    Builds a current-API case map, requires exact parity with the procedure's input
    signature, and invokes each input independently with safe test-database values.
    All calls use dry-run mode. A final check exercises all OUTPUT parameters.

.PARAMETER Server
    SQL Server host or tunnel endpoint.

.PARAMETER Password
    SQL password. Defaults to SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Versions
    One supported SQL version. Default: 2022.

.PARAMETER ResultsPath
    Optional path for the release-gate JSON result list.

.PARAMETER SkipSetup
    Skip deploying the procedure and preparing test data; for release-gate use.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Server,
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [ValidateSet('2019','2022','2025')][string[]]$Versions = @('2022'),
    [string]$ResultsPath = '',
    [switch]$SkipSetup
)

$ErrorActionPreference = 'Stop'
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }
if ($Versions.Count -ne 1) { throw 'Run one supported SQL version at a time.' }

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$procPath = Join-Path $repoRoot 'sp_StatUpdate.sql'
$setupPath = Join-Path $scriptDir 'setup_multiversion_db.sql'
$database = 'StatUpdateTest'
$databaseLiteral = "N'$database'"
$versionConfig = @{
    '2019' = @{ Port = 1434; Major = 15 }
    '2022' = @{ Port = 1432; Major = 16 }
    '2025' = @{ Port = 1431; Major = 17 }
}
$version = $Versions[0]
$port = $versionConfig[$version].Port
$startedAt = Get-Date
$procSource = [IO.File]::ReadAllText($procPath)

function Invoke-Sql {
    param(
        [string]$Query,
        [string]$Db = 'master',
        [int]$Timeout = 120
    )
    $connectionString = "Server=$Server,$port;Database=$Db;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=15"
    $messages = [System.Collections.Generic.List[string]]::new()
    $connection = [System.Data.SqlClient.SqlConnection]::new($connectionString)
    try {
        $connection.add_InfoMessage({ param($sender, $eventArgs) [void]$messages.Add($eventArgs.Message) })
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandText = $Query
        $command.CommandTimeout = $Timeout
        $adapter = [System.Data.SqlClient.SqlDataAdapter]::new($command)
        $dataSet = [System.Data.DataSet]::new()
        [void]$adapter.Fill($dataSet)
        return @{ ExitCode = 0; Messages = @($messages); DataSet = $dataSet }
    } catch {
        $message = if ($_.Exception.InnerException) { $_.Exception.InnerException.Message } else { $_.Exception.Message }
        return @{ ExitCode = 1; Messages = @($messages) + $message; DataSet = $null }
    } finally {
        $connection.Dispose()
    }
}

function Add-Result {
    param([string]$Check, [string]$Result, [string]$Reason = '', [long]$DurationMs = 0)
    $record = [pscustomobject]@{
        CheckName = $Check
        Outcome = $Result
        Detail = $Reason
        DurationMs = [Math]::Max(0, $DurationMs)
    }
    [void]$script:checks.Add($record)
    $color = if ($Result -eq 'PASS') { 'Green' } else { 'Red' }
    $suffix = if ($Reason) { " ($Reason)" } else { '' }
    Write-Host "  [$Result] $Check$suffix" -ForegroundColor $color
}

function Get-SqlFailureMessage([object]$Result) {
    $message = (@($Result.Messages | Where-Object { $_ -match '(?i)is not a parameter|invalid @|requires @|must be|cannot be|not supported|error|failed' } | Select-Object -First 2) -join '; ')
    if (-not $message) { $message = 'SQL call failed without a reported message.' }
    return $message
}

$checks = [System.Collections.Generic.List[object]]::new()
$allStarted = Get-Date
$connectionCheck = Invoke-Sql -Query "SELECT CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS MajorVer, CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')) AS ProductVersion;"
if ($connectionCheck.ExitCode -ne 0 -or [int]$connectionCheck.DataSet.Tables[0].Rows[0]['MajorVer'] -ne $versionConfig[$version].Major) {
    throw 'SQL version endpoint is unavailable or returned an unexpected engine version.'
}
$sqlBuild = [string]$connectionCheck.DataSet.Tables[0].Rows[0]['ProductVersion']

if (-not $SkipSetup) {
    $deploySql = [IO.File]::ReadAllText($procPath)
    foreach ($batch in [regex]::Split($deploySql, '(?m)^\s*GO\s*$')) {
        if (-not $batch.Trim()) { continue }
        $deployment = Invoke-Sql -Query $batch -Db 'master'
        if ($deployment.ExitCode -ne 0) { throw 'Procedure deployment failed.' }
    }
    $setupSql = [IO.File]::ReadAllText($setupPath)
    $currentDatabase = 'master'
    foreach ($batch in [regex]::Split($setupSql, '(?m)^\s*GO\s*$')) {
        if (-not $batch.Trim()) { continue }
        if ($batch -match '(?mi)(?:^|\n)\s*USE\s+\[?([A-Za-z0-9_]+)\]?') { $currentDatabase = $Matches[1] }
        $setup = Invoke-Sql -Query $batch -Db $currentDatabase
        if ($setup.ExitCode -ne 0) { throw 'Release-gate test database setup failed.' }
    }
}

$statSetup = Invoke-Sql -Db $database -Query @'
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE object_id = OBJECT_ID(N'dbo.Orders') AND name = N'PS_ParamSweep')
    CREATE STATISTICS PS_ParamSweep ON dbo.Orders(CustomerName);
'@
if ($statSetup.ExitCode -ne 0) { throw 'Could not prepare the isolated parameter-sweep statistic.' }
$whatIfName = 'ParamSweep_' + [guid]::NewGuid().ToString('N')
$whatIfQualified = "tempdb.dbo.$whatIfName"

# One deliberately safe value per current input parameter. Paired values appear
# only where the documented contract requires them (critical-table sampling and
# the mop-up time budget).
$cases = [ordered]@{
    '@JobName' = "N'ParamSweep'"
    '@Statistics' = "N'dbo.Orders.PS_ParamSweep'"
    '@Preset' = "N'DEFAULT'"
    '@Databases' = $databaseLiteral
    '@Tables' = "N'dbo.Orders'"
    '@ExcludeTables' = "N'dbo.NeverMatch%'"
    '@ExcludeStatistics' = "N'PS_NeverMatch%'"
    '@SkipTablesWithNCCI' = "N'Y'"
    '@SkipTablesWithCCI' = "N'N'"
    '@TargetNorecompute' = "N'BOTH'"
    '@StaleHours' = '1'
    '@QueryStore' = "N'OFF'"
    '@TimeLimit' = '60'
    '@StopByTime' = "N'23:59'"
    '@BatchLimit' = '1'
    '@SortOrder' = "N'ROWS'"
    '@FilteredStatsMode' = "N'INCLUDE'"
    '@FilteredStatsStaleFactor' = '2.0'
    '@MopUpPass' = "N'N'"
    '@StatisticsSample' = '10'
    '@MaxDOP' = '1'
    '@LockTimeout' = '0'
    '@ModificationThreshold' = '1'
    '@LongRunningThresholdMinutes' = '1'
    '@LongRunningSamplePercent' = '50'
    '@FirstTimeFullScanCapRows' = '100000'
    '@QueryStoreTopPlans' = '10'
    '@QueryStoreMinExecutions' = '1'
    '@QueryStoreRecentHours' = '24'
    '@MaxAGRedoQueueMB' = '1024'
    '@MaxAGWaitMinutes' = '1'
    '@MinTempdbFreeMB' = '1'
    '@MopUpMinRemainingSeconds' = '60'
    '@CriticalTables' = "N'dbo.Orders'"
    '@CriticalSamplePercent' = '50'
    '@CriticalTablesFirst' = "N'Y'"
    '@LogToTable' = "N'Y'"
    '@Execute' = "N'N'"
    '@WhatIfOutputTable' = "N'$whatIfQualified'"
    '@FailFast' = '0'
    '@AbortOnIntegrityError' = '1'
    '@Debug' = '0'
    '@Ansi' = "N'N'"
    '@StatsInParallel' = "N'N'"
    '@Help' = "N'0'"
}

$signatureNames = [regex]::Matches($procSource.Substring($procSource.IndexOf('@JobName sysname'), $procSource.IndexOf('WITH RECOMPILE') - $procSource.IndexOf('@JobName sysname')), '(?m)^\s*(@[A-Za-z][A-Za-z0-9_]*)\s+[A-Za-z]') |
    ForEach-Object { $_.Groups[1].Value } | Select-Object -Unique
$signatureInputs = @($signatureNames | Where-Object { $_ -notin @('@Version','@VersionDate','@StatsFoundOut','@StatsProcessedOut','@StatsSucceededOut','@StatsFailedOut','@StatsRemainingOut','@DurationSecondsOut','@WarningsOut','@WarningsCodesOut','@StopReasonOut') })
$missingCases = @($signatureInputs | Where-Object { -not $cases.Contains($_) })
$extraCases = @($cases.Keys | Where-Object { $_ -notin $signatureInputs })
if ($missingCases.Count -or $extraCases.Count -or $signatureInputs.Count -ne 45) {
    throw "Parameter case map mismatch (signature=$($signatureInputs.Count), missing=$($missingCases -join ','), extra=$($extraCases -join ','))."
}

Write-Host "ParamSweep SQL $version ($sqlBuild): checking $($signatureInputs.Count) current inputs and all OUTPUT parameters."
foreach ($parameter in $signatureInputs) {
    $timer = [Diagnostics.Stopwatch]::StartNew()
    $arguments = [System.Collections.Generic.List[string]]::new()
    if ($parameter -ne '@Databases') { $arguments.Add("@Databases = $databaseLiteral") }
    if ($parameter -ne '@Execute') { $arguments.Add("@Execute = N'N'") }
    if ($parameter -ne '@LogToTable') { $arguments.Add("@LogToTable = N'Y'") }
    if ($parameter -in @('@CriticalTables','@CriticalSamplePercent','@CriticalTablesFirst') -and $parameter -ne '@CriticalTables') {
        $arguments.Add("@CriticalTables = N'dbo.Orders'")
    }
    if ($parameter -in @('@CriticalTables','@CriticalSamplePercent','@CriticalTablesFirst') -and $parameter -ne '@CriticalSamplePercent') {
        $arguments.Add('@CriticalSamplePercent = 50')
    }
    if ($parameter -eq '@MopUpPass') { $arguments.Add('@TimeLimit = 60') }
    $arguments.Add("$parameter = $($cases[$parameter])")
    $query = 'EXEC master.dbo.sp_StatUpdate ' + ($arguments -join ', ') + ';'
    $call = Invoke-Sql -Db $database -Query $query
    $timer.Stop()
    if ($call.ExitCode -eq 0) {
        Add-Result -Check $parameter -Result 'PASS' -DurationMs $timer.ElapsedMilliseconds
    } else {
        Add-Result -Check $parameter -Result 'FAIL' -Reason (Get-SqlFailureMessage $call) -DurationMs $timer.ElapsedMilliseconds
    }
}

# Exercise each declared OUTPUT parameter in one call, then verify the returned
# variable values are present in the result set.
$outputQuery = @"
DECLARE @Version varchar(20), @VersionDate datetime,
    @StatsFoundOut int, @StatsProcessedOut int, @StatsSucceededOut int,
    @StatsFailedOut int, @StatsRemainingOut int, @DurationSecondsOut int,
    @WarningsOut nvarchar(max), @WarningsCodesOut nvarchar(max), @StopReasonOut nvarchar(50);
EXEC master.dbo.sp_StatUpdate @Databases = $databaseLiteral, @Execute = N'N', @LogToTable = N'N',
    @Version = @Version OUTPUT, @VersionDate = @VersionDate OUTPUT,
    @StatsFoundOut = @StatsFoundOut OUTPUT, @StatsProcessedOut = @StatsProcessedOut OUTPUT,
    @StatsSucceededOut = @StatsSucceededOut OUTPUT, @StatsFailedOut = @StatsFailedOut OUTPUT,
    @StatsRemainingOut = @StatsRemainingOut OUTPUT, @DurationSecondsOut = @DurationSecondsOut OUTPUT,
    @WarningsOut = @WarningsOut OUTPUT, @WarningsCodesOut = @WarningsCodesOut OUTPUT,
    @StopReasonOut = @StopReasonOut OUTPUT;
SELECT @Version AS Version, @VersionDate AS VersionDate, @StatsFoundOut AS StatsFoundOut,
    @StatsProcessedOut AS StatsProcessedOut, @StatsSucceededOut AS StatsSucceededOut,
    @StatsFailedOut AS StatsFailedOut, @StatsRemainingOut AS StatsRemainingOut,
    @DurationSecondsOut AS DurationSecondsOut, @WarningsOut AS WarningsOut,
    @WarningsCodesOut AS WarningsCodesOut, @StopReasonOut AS StopReasonOut;
"@
$outputTimer = [Diagnostics.Stopwatch]::StartNew()
$outputRun = Invoke-Sql -Db $database -Query $outputQuery
$outputTimer.Stop()
$expectedOutputColumns = @('Version','VersionDate','StatsFoundOut','StatsProcessedOut','StatsSucceededOut','StatsFailedOut','StatsRemainingOut','DurationSecondsOut','WarningsOut','WarningsCodesOut','StopReasonOut')
$outputTable = $null
if ($outputRun.DataSet) {
    foreach ($table in $outputRun.DataSet.Tables) {
        if (@($expectedOutputColumns | Where-Object { -not $table.Columns.Contains($_) }).Count -eq 0 -and $table.Rows.Count -gt 0) { $outputTable = $table; break }
    }
}
if ($outputRun.ExitCode -eq 0 -and $outputTable -and -not [string]::IsNullOrWhiteSpace([string]$outputTable.Rows[0]['Version'])) {
    Add-Result -Check 'OutputParamCoverage' -Result 'PASS' -DurationMs $outputTimer.ElapsedMilliseconds
} else {
    $reason = if ($outputRun.ExitCode -ne 0) { Get-SqlFailureMessage $outputRun } else { 'The full OUTPUT parameter result set was not returned.' }
    Add-Result -Check 'OutputParamCoverage' -Result 'FAIL' -Reason $reason -DurationMs $outputTimer.ElapsedMilliseconds
}

Invoke-Sql -Query "IF OBJECT_ID(N'$whatIfQualified') IS NOT NULL DROP TABLE $whatIfQualified;" -Db 'master' | Out-Null
$endLiveness = Invoke-Sql -Query "SELECT CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')) AS ProductVersion;"
$serverAliveAtEnd = $endLiveness.ExitCode -eq 0
$failed = @($checks | Where-Object { $_.Outcome -ne 'PASS' }).Count
Write-Host "ParamSweep result: $(@($checks).Count - $failed)/$(@($checks).Count) passed; $failed failed."

if ($ResultsPath) {
    if (@($checks).Count -ne 46) { throw "Expected 46 ParamSweep checks; received $(@($checks).Count)." }
    $commit = (& git -C $repoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Import-Module (Join-Path $scriptDir 'ReleaseResults.psm1') -Force
    $endedAt = Get-Date
    $releaseChecks = @($checks | ForEach-Object {
        $item = [ordered]@{ check = $_.CheckName; result = $_.Outcome; duration_ms = $_.DurationMs }
        if ($_.Outcome -ne 'PASS') { $item.reason = $_.Detail }
        [pscustomobject]$item
    })
    [void](Write-ReleaseRunList -Path $ResultsPath -Commit $commit -SqlVersion $version -SqlBuild $sqlBuild `
        -ServerAliveAtEnd $serverAliveAtEnd -Suite 'ParamSweep' -ExpectedCount 46 -Checks $releaseChecks `
        -StartedAt $startedAt -EndedAt $endedAt -DurationMs ([long]($endedAt - $startedAt).TotalMilliseconds))
}
if ($failed -gt 0 -or -not $serverAliveAtEnd) { exit 1 }
exit 0
