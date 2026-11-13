[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Server,
    [string[]]$Versions = @('2022'),
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [string]$ResultsPath = ''
)

$ErrorActionPreference = 'Stop'
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }
if ($Versions.Count -ne 1) { throw 'Run one supported SQL version at a time.' }
$version = $Versions[0]
$port = @{ '2019' = 1434; '2022' = 1432; '2025' = 1431 }[$version]
if (-not $port) { throw 'Unsupported SQL Server version.' }
$run = [guid]::NewGuid().ToString('N').Substring(0, 10)
$database = "AwkwardNames_$run"
$outTable = "AN_O_$run"
$repoRoot = Split-Path -Parent $PSScriptRoot
$started = Get-Date
$lastCheck = $started
$checks = [System.Collections.Generic.List[object]]::new()
$aliveAtEnd = $false
$sqlBuild = 'unavailable'
$expectedNames = @('Space Name', 'Bracket]Name', 'Quote"Name', 'Ωmega', 'CaseTwin', 'casetwin')

function Invoke-Sql {
    param([string]$Query, [string]$Db = 'master', [int]$Timeout = 120, [switch]$Rows)
    $cs = "Server=$Server,$port;Database=$Db;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Connection Timeout=15;Pooling=false"
    $c = [System.Data.SqlClient.SqlConnection]::new($cs)
    $c.FireInfoMessageEventOnUserErrors = $true
    try {
        $c.Open()
        $cmd = $c.CreateCommand(); $cmd.CommandText = $Query; $cmd.CommandTimeout = $Timeout
        if ($Rows) { $reader = $cmd.ExecuteReader(); $table = [System.Data.DataTable]::new(); $table.Load($reader); return ,$table }
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
    Invoke-Sql "CREATE DATABASE [$database] COLLATE Latin1_General_100_CS_AS;" 'master' 120
    $setup = @"
CREATE TABLE dbo.[Space Name](id int IDENTITY PRIMARY KEY, k int NOT NULL);
CREATE TABLE dbo.[Bracket]]Name](id int IDENTITY PRIMARY KEY, k int NOT NULL);
CREATE TABLE dbo.[Quote"Name](id int IDENTITY PRIMARY KEY, k int NOT NULL);
CREATE TABLE dbo.[Ωmega](id int IDENTITY PRIMARY KEY, k int NOT NULL);
CREATE TABLE dbo.[CaseTwin](id int IDENTITY PRIMARY KEY, k int NOT NULL);
CREATE TABLE dbo.[casetwin](id int IDENTITY PRIMARY KEY, k int NOT NULL);
INSERT dbo.[Space Name](k) SELECT TOP (2000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM sys.all_objects a CROSS JOIN sys.all_objects b;
INSERT dbo.[Bracket]]Name](k) SELECT TOP (2000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM sys.all_objects a CROSS JOIN sys.all_objects b;
INSERT dbo.[Quote"Name](k) SELECT TOP (2000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM sys.all_objects a CROSS JOIN sys.all_objects b;
INSERT dbo.[Ωmega](k) SELECT TOP (2000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM sys.all_objects a CROSS JOIN sys.all_objects b;
INSERT dbo.[CaseTwin](k) SELECT TOP (2000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM sys.all_objects a CROSS JOIN sys.all_objects b;
INSERT dbo.[casetwin](k) SELECT TOP (2000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM sys.all_objects a CROSS JOIN sys.all_objects b;
UPDATE STATISTICS dbo.[Space Name] WITH FULLSCAN; UPDATE STATISTICS dbo.[Bracket]]Name] WITH FULLSCAN;
UPDATE STATISTICS dbo.[Quote"Name] WITH FULLSCAN; UPDATE STATISTICS dbo.[Ωmega] WITH FULLSCAN;
UPDATE STATISTICS dbo.[CaseTwin] WITH FULLSCAN; UPDATE STATISTICS dbo.[casetwin] WITH FULLSCAN;
INSERT dbo.[Space Name](k) SELECT TOP (1000) -1 FROM sys.all_objects a CROSS JOIN sys.all_objects b;
INSERT dbo.[Bracket]]Name](k) SELECT TOP (1000) -1 FROM sys.all_objects a CROSS JOIN sys.all_objects b;
INSERT dbo.[Quote"Name](k) SELECT TOP (1000) -1 FROM sys.all_objects a CROSS JOIN sys.all_objects b;
INSERT dbo.[Ωmega](k) SELECT TOP (1000) -1 FROM sys.all_objects a CROSS JOIN sys.all_objects b;
INSERT dbo.[CaseTwin](k) SELECT TOP (1000) -1 FROM sys.all_objects a CROSS JOIN sys.all_objects b;
INSERT dbo.[casetwin](k) SELECT TOP (1000) -1 FROM sys.all_objects a CROSS JOIN sys.all_objects b;
"@
    Invoke-Sql $setup $database 180
    Invoke-Sql "IF OBJECT_ID(N'dbo.[$outTable]',N'U') IS NOT NULL DROP TABLE dbo.[$outTable];" 'master'
    Invoke-Sql "EXEC dbo.sp_StatUpdate @Databases=N'$database', @Tables=N'dbo.Space Name,dbo.Bracket]Name,dbo.Quote`"Name,dbo.Ωmega,dbo.CaseTwin,dbo.casetwin', @ModificationThreshold=1, @StaleHours=0, @Execute=N'N', @LogToTable=N'N', @WhatIfOutputTable=N'dbo.$outTable';" 'master' 180
    $rows = Invoke-Sql "SELECT TableName FROM dbo.[$outTable];" 'master' 30 -Rows
    $actual = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($row in $rows.Rows) { [void]$actual.Add([string]$row['TableName']) }
    $missing = @($expectedNames | Where-Object { -not $actual.Contains($_) })
    if ($missing.Count -eq 0) { Add-Check 'quoted_unicode_object_names' 'PASS' }
    else {
        $actualNames = @($actual | Sort-Object)
        $missingText = [string]::Join('|', [string[]]$missing)
        $actualText = [string]::Join('|', [string[]]$actualNames)
        Add-Check 'quoted_unicode_object_names' 'FAIL' ("missing=[$missingText]; actual=[$actualText].")
    }
    if ($actual.Contains('CaseTwin') -and $actual.Contains('casetwin')) { Add-Check 'case_sensitive_identifier_resolution' 'PASS' }
    else {
        $actualText = [string]::Join('|', [string[]]@($actual | Sort-Object))
        Add-Check 'case_sensitive_identifier_resolution' 'FAIL' ('CaseTwin={0}; casetwin={1}; actual=[{2}].' -f $actual.Contains('CaseTwin'), $actual.Contains('casetwin'), $actualText)
    }
    } catch {
    $reason = $_.Exception.Message.Replace($Password, '[redacted]')
    foreach ($name in @('quoted_unicode_object_names','case_sensitive_identifier_resolution')) {
        if (@($checks | Where-Object check -eq $name).Count -eq 0) { Add-Check $name 'FAIL' "Suite setup/execution failed ($reason)." }
    }
} finally {
    try { Invoke-Sql "IF OBJECT_ID(N'dbo.[$outTable]',N'U') IS NOT NULL DROP TABLE dbo.[$outTable];" 'master' 30 } catch { }
    try { Invoke-Sql "IF DB_ID(N'$database') IS NOT NULL BEGIN ALTER DATABASE [$database] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [$database]; END;" 'master' 60 } catch { }
    try { $status = Invoke-Sql "SELECT CONVERT(nvarchar(64), SERVERPROPERTY(N'ProductVersion')) AS Build;" 'master' 10 -Rows; $sqlBuild = [string]$status.Rows[0]['Build']; $aliveAtEnd = $true } catch { Write-Warning "Server liveness probe failed ($($_.Exception.GetType().Name))." }
}

if ($ResultsPath) {
    if ($checks.Count -ne 2) { throw "Expected two checks; received $($checks.Count)." }
    $commit = (& git -C $repoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Import-Module (Join-Path $PSScriptRoot 'ReleaseResults.psm1') -Force
    $ended = Get-Date
    [void](Write-ReleaseRunList -Path $ResultsPath -Commit $commit -SqlVersion $version -SqlBuild $sqlBuild `
        -ServerAliveAtEnd $aliveAtEnd -Suite 'AwkwardNamesCaseSensitive' -ExpectedCount 2 -Checks @($checks) `
        -StartedAt $started -EndedAt $ended -DurationMs ([long](($ended - $started).TotalMilliseconds)))
}
if (@($checks | Where-Object result -ne 'PASS').Count) { exit 1 }
exit 0
