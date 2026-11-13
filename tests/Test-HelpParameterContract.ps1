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
$Version = $Versions[0]
if ($Version -notin @('2019','2022','2025')) { throw 'Unsupported SQL Server version.' }
$port = @{ '2019' = 1434; '2022' = 1432; '2025' = 1431 }[$Version]
$repoRoot = Split-Path -Parent $PSScriptRoot
$procFile = Join-Path $repoRoot 'sp_StatUpdate.sql'
$started = Get-Date
$lastCheck = $started
$checks = [System.Collections.Generic.List[object]]::new()
$aliveAtEnd = $false
$sqlBuild = 'unavailable'

function Add-Check([string]$Name, [string]$Result, [string]$Reason = '') {
    $now = Get-Date
    $item = [ordered]@{ check = $Name; result = $Result; duration_ms = [long][Math]::Max(0, ($now - $script:lastCheck).TotalMilliseconds) }
    if ($Result -ne 'PASS') { $item.reason = $Reason }
    $script:checks.Add([pscustomobject]$item)
    $script:lastCheck = $now
}

function Invoke-HelpQuery {
    $connectionString = "Server=$Server,$port;Database=master;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Connection Timeout=15;Pooling=false"
    $connection = [System.Data.SqlClient.SqlConnection]::new($connectionString)
    try {
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandText = 'EXEC dbo.sp_StatUpdate @Help = 1;'
        $command.CommandTimeout = 60
        $dataSet = [System.Data.DataSet]::new()
        $reader = $command.ExecuteReader()
        do {
            $table = [System.Data.DataTable]::new()
            $table.Load($reader)
            [void]$dataSet.Tables.Add($table)
        } while (-not $reader.IsClosed)
        $buildCommand = $connection.CreateCommand()
        $buildCommand.CommandText = "SELECT CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion'));"
        $build = [string]$buildCommand.ExecuteScalar()
        return [pscustomobject]@{ DataSet = $dataSet; Build = $build }
    } finally { $connection.Dispose() }
}

$expectedCheckNames = @(
    'HC0: @Help=1 proc call succeeds', 'HC0: Returns 4 result sets',
    'HC1: RS1 contains current @procedure_version', "HC2: RS1 contains 'Version:' keyword",
    'HC3: RS2 contains all 5 preset names', 'HC4: RS3 parameter names match input signature',
    'HC5: RS3 does NOT contain deprecated/v3-internal params', 'HC6: RS4 contains all documented help topics',
    'HC7: RS4 detail contains all 5 preset names', 'HC8: RS2 has correct column names',
    'HC9: RS3 has correct column names', 'HC10: RS4 has correct column names (topic, detail)',
    'HC11: RS1 has exactly 14 rows', 'HC12: RS2 has exactly 5 preset rows',
    'HC13: RS3 row count matches input signature', 'HC14: @Databases entry has type nvarchar(max)',
    'HC15: @Preset default is DEFAULT', 'HC16: @Execute default is Y', 'HC17: @LogToTable default is Y',
    'HC18: @StatsInParallel default is N', 'HC19: @ModificationThreshold type is bigint',
    'HC20: @TargetNorecompute default is BOTH', 'HC20: @TargetNorecompute entry exists with default BOTH',
    'HC21: @Help is listed in RS3'
)
function Add-Assertion([string]$Name, [bool]$Passed, [string]$Reason) {
    if ($Passed) { Add-Check $Name 'PASS' } else { Add-Check $Name 'FAIL' $Reason }
}
function Get-ParameterRow($Table, [string]$Name) {
    if ($null -eq $Table -or -not $Table.Columns.Contains('parameter')) { return $null }
    foreach ($row in $Table.Rows) { if ([string]$row.parameter -ceq $Name) { return $row } }
    return $null
}

try {
    $source = [IO.File]::ReadAllText($procFile)
    $signature = [regex]::Match($source, '(?is)\bALTER\s+PROCEDURE\s+(?:\[?dbo\]?\.)?\[?sp_StatUpdate\]?\b(.*?)\nAS\s*\n')
    if (-not $signature.Success) { throw 'Could not parse the current procedure signature.' }
    $signatureText = [regex]::Replace($signature.Groups[1].Value, '/\*.*?\*/|--[^\r\n]*', '', [Text.RegularExpressions.RegexOptions]::Singleline)
    $outputParameters = @([regex]::Matches($signatureText, '@([A-Za-z][A-Za-z0-9_]*)\s+[A-Za-z][A-Za-z0-9_]*(?:\s*\([^)]*\))?\s*=[^,\r\n]*\bOUTPUT\b', [Text.RegularExpressions.RegexOptions]::IgnoreCase) | ForEach-Object { '@' + $_.Groups[1].Value })
    $expected = @([regex]::Matches($signatureText, '(?im)^\s*(@[A-Za-z][A-Za-z0-9_]*)\s+') | ForEach-Object { $_.Groups[1].Value } | Where-Object { $outputParameters -notcontains $_ } | Sort-Object -Unique)
    if (-not $expected.Count) { throw 'The procedure signature yielded no input parameters.' }
    $output = Invoke-HelpQuery
    $script:sqlBuild = $output.Build
    $tables = $output.DataSet.Tables
    Add-Assertion 'HC0: @Help=1 proc call succeeds' ($tables.Count -gt 0) 'The call returned no result sets.'
    Add-Assertion 'HC0: Returns 4 result sets' ($tables.Count -eq 4) "@Help returned $($tables.Count) result sets; expected 4."
    if ($tables.Count -lt 4) { throw "@Help returned $($tables.Count) result sets; four are required." }
    $rs1, $rs2, $rs3, $rs4 = $tables[0], $tables[1], $tables[2], $tables[3]
    $intro = @($rs1.Rows | ForEach-Object { [string]$_.introduction })
    $versionMatch = [regex]::Match($source, "(?im)^\s*@procedure_version\s+varchar\(\d+\)\s*=\s*'([^']+)'")
    Add-Assertion 'HC1: RS1 contains current @procedure_version' ($versionMatch.Success -and @($intro | Where-Object { $_ -like "*$($versionMatch.Groups[1].Value)*" }).Count -gt 0) 'The current procedure version is absent from RS1.'
    Add-Assertion "HC2: RS1 contains 'Version:' keyword" (@($intro | Where-Object { $_ -like '*Version:*' }).Count -gt 0) 'RS1 does not contain the Version: line.'
    $presetNames = @('DEFAULT','NIGHTLY','WEEKLY_FULL','OLTP_LIGHT','WAREHOUSE')
    $presets = @($rs2.Rows | ForEach-Object { [string]$_.preset_name })
    Add-Assertion 'HC3: RS2 contains all 5 preset names' (@($presetNames | Where-Object { $presets -cnotcontains $_ }).Count -eq 0) 'One or more preset names are missing from RS2.'
    $rows = @($rs3.Rows | ForEach-Object { [string]$_.parameter })
    $actual = @($rows | Sort-Object -Unique)
    $missing = @($expected | Where-Object { $actual -notcontains $_ })
    $extra = @($actual | Where-Object { $expected -notcontains $_ })
    $duplicates = @($rows | Group-Object | Where-Object Count -gt 1)
    $namesMatch = $missing.Count -eq 0 -and $extra.Count -eq 0 -and $duplicates.Count -eq 0
    if ($namesMatch) { Add-Check 'HC4: RS3 parameter names match input signature' 'PASS' }
    else { Add-Check 'HC4: RS3 parameter names match input signature' 'FAIL' ('missing={0}; extra={1}; duplicates={2}.' -f ($missing -join ','), ($extra -join ','), (($duplicates | ForEach-Object Name) -join ',')) }
    if ($rows.Count -eq $expected.Count) { Add-Check 'HC13: RS3 row count matches input signature' 'PASS' }
    else { Add-Check 'HC13: RS3 row count matches input signature' 'FAIL' ("rows={0}; expected={1}." -f $rows.Count, $expected.Count) }

    $forbiddenParams = @('@DaysStaleThreshold','@HoursStaleThreshold','@MaxWorkers','@IsParallelWorker')
    $forbiddenFound = @($actual | Where-Object { $_ -match '^@i_' -or $forbiddenParams -contains $_ })
    Add-Assertion 'HC5: RS3 does NOT contain deprecated/v3-internal params' ($forbiddenFound.Count -eq 0) ('Found forbidden parameter names: ' + ($forbiddenFound -join ', '))
    $topics = @($rs4.Rows | ForEach-Object { [string]$_.topic })
    $expectedTopics = @('PRESETS','CONTEXT_INFO','COLUMNSTORE','WARNINGS','PARALLEL','QUERY STORE','SPILL PROOF','MOP-UP','DIRECT MODE','STALE HOURS','SCHEDULING','NORECOMPUTE','COMMANDLOG','DIAGNOSTICS','CRITICAL TABLES','INTEGRITY ABORT','ANSI COLOR','VERSION')
    $missingTopics = @($expectedTopics | Where-Object { $topics -cnotcontains $_ })
    Add-Assertion 'HC6: RS4 contains all documented help topics' ($missingTopics.Count -eq 0) ('Missing topics: ' + ($missingTopics -join ', '))
    $presetHelp = @($rs4.Rows | Where-Object { [string]$_.topic -ceq 'PRESETS' } | ForEach-Object { [string]$_.detail }) -join ' '
    $missingPresets = @($presetNames | Where-Object { $presetHelp -notmatch [regex]::Escape($_) })
    Add-Assertion 'HC7: RS4 detail contains all 5 preset names' ($missingPresets.Count -eq 0) ('Missing preset names from PRESETS help: ' + ($missingPresets -join ', '))
    $columns = { param($table) [string]::Join('|', @($table.Columns | ForEach-Object { $_.ColumnName })) }
    Add-Assertion 'HC8: RS2 has correct column names' ((& $columns $rs2) -ceq 'preset_name|time_limit|sort_order|query_store|mop_up|sample|description') 'RS2 columns do not match the documented preset schema.'
    Add-Assertion 'HC9: RS3 has correct column names' ((& $columns $rs3) -ceq 'parameter|type|default|description') 'RS3 columns do not match the documented parameter schema.'
    Add-Assertion 'HC10: RS4 has correct column names (topic, detail)' ((& $columns $rs4) -ceq 'topic|detail') 'RS4 columns do not match the documented topic schema.'
    Add-Assertion 'HC11: RS1 has exactly 14 rows' ($rs1.Rows.Count -eq 14) 'RS1 must contain 14 introduction lines.'
    Add-Assertion 'HC12: RS2 has exactly 5 preset rows' ($rs2.Rows.Count -eq 5) 'RS2 must contain five preset definitions.'
    $databases = Get-ParameterRow $rs3 '@Databases'
    Add-Assertion 'HC14: @Databases entry has type nvarchar(max)' ($null -ne $databases -and [string]$databases.type -ieq 'nvarchar(max)') 'The @Databases row is missing or has the wrong type.'
    $preset = Get-ParameterRow $rs3 '@Preset'
    Add-Assertion 'HC15: @Preset default is DEFAULT' ($null -ne $preset -and [string]$preset.default -ceq 'DEFAULT') 'The @Preset default is not DEFAULT.'
    $execute = Get-ParameterRow $rs3 '@Execute'
    Add-Assertion 'HC16: @Execute default is Y' ($null -ne $execute -and [string]$execute.default -ceq 'Y') 'The @Execute default is not Y.'
    $logToTable = Get-ParameterRow $rs3 '@LogToTable'
    Add-Assertion 'HC17: @LogToTable default is Y' ($null -ne $logToTable -and [string]$logToTable.default -ceq 'Y') 'The @LogToTable default is not Y.'
    $parallel = Get-ParameterRow $rs3 '@StatsInParallel'
    Add-Assertion 'HC18: @StatsInParallel default is N' ($null -ne $parallel -and [string]$parallel.default -ceq 'N') 'The @StatsInParallel default is not N.'
    $threshold = Get-ParameterRow $rs3 '@ModificationThreshold'
    Add-Assertion 'HC19: @ModificationThreshold type is bigint' ($null -ne $threshold -and [string]$threshold.type -ieq 'bigint') 'The @ModificationThreshold row is missing or has the wrong type.'
    $norecompute = Get-ParameterRow $rs3 '@TargetNorecompute'
    Add-Assertion 'HC20: @TargetNorecompute default is BOTH' ($null -ne $norecompute -and [string]$norecompute.default -ceq 'BOTH') 'The @TargetNorecompute default is not BOTH.'
    Add-Assertion 'HC20: @TargetNorecompute entry exists with default BOTH' ($null -ne $norecompute -and [string]$norecompute.default -ceq 'BOTH') 'The @TargetNorecompute row is missing or has the wrong default.'
    Add-Assertion 'HC21: @Help is listed in RS3' ($null -ne (Get-ParameterRow $rs3 '@Help')) 'RS3 does not list @Help.'
} catch {
    $reason = $_.Exception.Message.Replace($Password, '[redacted]')
    foreach ($name in $expectedCheckNames) {
        if (@($checks | Where-Object check -eq $name).Count -eq 0) { Add-Check $name 'FAIL' "Setup or query failed ($reason)." }
    }
} finally {
    try {
        $connectionString = "Server=$Server,$port;Database=master;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Connection Timeout=10;Pooling=false"
        $connection = [System.Data.SqlClient.SqlConnection]::new($connectionString)
        $connection.Open()
        $buildCommand = $connection.CreateCommand()
        $buildCommand.CommandText = "SELECT CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion'));"
        $sqlBuild = [string]$buildCommand.ExecuteScalar()
        $aliveAtEnd = $true
    } catch { Write-Warning "SQL liveness probe failed ($($_.Exception.GetType().Name))." }
    finally { if ($connection) { $connection.Dispose() } }
}

if ($checks.Count -ne $expectedCheckNames.Count) { throw "Expected $($expectedCheckNames.Count) HelpContent checks; received $($checks.Count)." }
$commit = (& git -C $repoRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
Import-Module (Join-Path $PSScriptRoot 'ReleaseResults.psm1') -Force
$ended = Get-Date
[void](Write-ReleaseRunList -Path $ResultsPath -Commit $commit -SqlVersion $Version -SqlBuild $sqlBuild `
    -ServerAliveAtEnd $aliveAtEnd -Suite 'HelpContent' -ExpectedCount $expectedCheckNames.Count -Checks @($checks) `
    -StartedAt $started -EndedAt $ended -DurationMs ([long](($ended - $started).TotalMilliseconds)))
if (@($checks | Where-Object result -ne 'PASS').Count) { exit 1 }
exit 0
