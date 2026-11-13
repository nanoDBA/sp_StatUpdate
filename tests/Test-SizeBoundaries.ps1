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

function Add-Check([string]$Name, [bool]$Passed, [string]$Reason = '') {
    $now = Get-Date
    $item = [ordered]@{
        check = $Name
        result = if ($Passed) { 'PASS' } else { 'FAIL' }
        duration_ms = [long][Math]::Max(0, ($now - $script:lastCheck).TotalMilliseconds)
    }
    if (-not $Passed) { $item.reason = $Reason }
    $script:checks.Add([pscustomobject]$item)
    $script:lastCheck = $now
}

function New-SqlConnection {
    $connectionString = "Server=$Server,$port;Database=StatUpdateTest;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Connection Timeout=15;Pooling=false"
    return [System.Data.SqlClient.SqlConnection]::new($connectionString)
}

try {
    $connection = New-SqlConnection
    try {
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandText = @'
CREATE TABLE dbo.SizeBoundary10K (ID int NOT NULL PRIMARY KEY, SampleValue int NOT NULL);
CREATE TABLE dbo.SizeBoundary100K (ID int NOT NULL PRIMARY KEY, SampleValue int NOT NULL);
CREATE TABLE dbo.SizeBoundary1M (ID int NOT NULL PRIMARY KEY, SampleValue int NOT NULL);
CREATE TABLE dbo.SizeBoundary1024Pages (ID int NOT NULL, Payload char(7800) NOT NULL);
;WITH Digits(d) AS (
    SELECT d FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS v(d)
), Numbers(n) AS (
    SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
    FROM Digits d0 CROSS JOIN Digits d1 CROSS JOIN Digits d2
    CROSS JOIN Digits d3 CROSS JOIN Digits d4 CROSS JOIN Digits d5
)
SELECT CONVERT(int, n) AS n INTO #SizeBoundaryNumbers FROM Numbers
UNION ALL SELECT 1000001;
INSERT dbo.SizeBoundary10K (ID, SampleValue) SELECT n, n % 97 FROM #SizeBoundaryNumbers WHERE n <= 10001;
INSERT dbo.SizeBoundary100K (ID, SampleValue) SELECT n, n % 997 FROM #SizeBoundaryNumbers WHERE n <= 100001;
INSERT dbo.SizeBoundary1M (ID, SampleValue) SELECT n, n % 9973 FROM #SizeBoundaryNumbers;
INSERT dbo.SizeBoundary1024Pages (ID, Payload) SELECT n, REPLICATE('X', 7800) FROM #SizeBoundaryNumbers WHERE n <= 1024;
CREATE STATISTICS ST_SizeBoundary10K_SampleValue ON dbo.SizeBoundary10K (SampleValue);
CREATE STATISTICS ST_SizeBoundary100K_SampleValue ON dbo.SizeBoundary100K (SampleValue);
CREATE STATISTICS ST_SizeBoundary1M_SampleValue ON dbo.SizeBoundary1M (SampleValue);
DROP TABLE #SizeBoundaryNumbers;
'@
        [void]$command.ExecuteNonQuery()
        $command.CommandText = @'
SELECT
    (SELECT COUNT_BIG(*) FROM dbo.SizeBoundary10K) AS Rows10K,
    (SELECT COUNT_BIG(*) FROM dbo.SizeBoundary100K) AS Rows100K,
    (SELECT COUNT_BIG(*) FROM dbo.SizeBoundary1M) AS Rows1M,
    (SELECT SUM(in_row_data_page_count)
       FROM sys.dm_db_partition_stats
      WHERE object_id = OBJECT_ID(N'dbo.SizeBoundary1024Pages')
        AND index_id IN (0, 1)) AS Pages1024,
    CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')) AS ProductVersion;
'@
        $command.CommandTimeout = 180
        $reader = $command.ExecuteReader()
        try {
            if (-not $reader.Read()) { throw 'Size-boundary query returned no row.' }
            $rows10K = [long]$reader['Rows10K']
            $rows100K = [long]$reader['Rows100K']
            $rows1M = [long]$reader['Rows1M']
            $pages = [long]$reader['Pages1024']
            $sqlBuild = [string]$reader['ProductVersion']
        } finally { $reader.Close() }
    } finally {
        if ($reader) { $reader.Dispose() }
        if ($command) { $command.Dispose() }
        $connection.Dispose()
    }

    Add-Check '10K' ($rows10K -gt 10000) "Expected >10,000 rows; observed $rows10K."
    Add-Check '100K' ($rows100K -gt 100000) "Expected >100,000 rows; observed $rows100K."
    Add-Check '1M' ($rows1M -gt 1000000) "Expected >1,000,000 rows; observed $rows1M."
    Add-Check '1024-pages' ($pages -ge 1000 -and $pages -le 1100) "Expected about 1,024 in-row pages (1,000-1,100); observed $pages."
} catch {
    $reason = $_.Exception.Message.Replace($Password, '[redacted]')
    foreach ($name in @('10K', '100K', '1M', '1024-pages')) {
        if (@($checks | Where-Object check -eq $name).Count -eq 0) { Add-Check $name $false "Size-boundary fixture verification failed ($reason)." }
    }
} finally {
    $aliveAtEnd = $false
    $probe = $null
    $command = $null
    try {
        $probe = New-SqlConnection
        $probe.Open()
        $command = $probe.CreateCommand()
        $command.CommandText = "SELECT CONVERT(nvarchar(128),SERVERPROPERTY(N'ProductVersion'));"
        $sqlBuild = [string]$command.ExecuteScalar()
        $aliveAtEnd = $true
    } catch { }
    finally {
        if ($command) { $command.Dispose() }
        if ($probe) { $probe.Dispose() }
    }
}

if ($checks.Count -ne 4) { throw "Expected four SizeBoundaries checks; received $($checks.Count)." }
$commit = (& git -C $repoRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
Import-Module (Join-Path $PSScriptRoot 'ReleaseResults.psm1') -Force
$ended = Get-Date
[void](Write-ReleaseRunList -Path $ResultsPath -Commit $commit -SqlVersion $version -SqlBuild $sqlBuild `
    -ServerAliveAtEnd $aliveAtEnd -Suite 'SizeBoundaries' -ExpectedCount 4 -Checks @($checks) `
    -StartedAt $started -EndedAt $ended -DurationMs ([long](($ended - $started).TotalMilliseconds)))
if (@($checks | Where-Object result -ne 'PASS').Count) { exit 1 }
exit 0
