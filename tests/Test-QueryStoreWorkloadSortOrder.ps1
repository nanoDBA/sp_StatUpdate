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
$high = "QSW_H_$run"
$low = "QSW_L_$run"
$outTable = "QSW_O_$run"
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
    $db = Invoke-Sql "SELECT database_id FROM sys.databases WHERE name = N'$Database';" -Rows
    if ($db.Rows.Count -ne 1) { throw "Required test database '$Database' is missing." }
    Invoke-Sql "ALTER DATABASE [$Database] SET AUTO_CREATE_STATISTICS ON; ALTER DATABASE [$Database] SET AUTO_UPDATE_STATISTICS ON; ALTER DATABASE [$Database] SET QUERY_STORE = ON; ALTER DATABASE [$Database] SET QUERY_STORE (OPERATION_MODE = READ_WRITE, QUERY_CAPTURE_MODE = ALL);"
    $setup = @"
IF OBJECT_ID(N'dbo.$high', N'U') IS NOT NULL DROP TABLE dbo.[$high];
IF OBJECT_ID(N'dbo.$low', N'U') IS NOT NULL DROP TABLE dbo.[$low];
CREATE TABLE dbo.[$high] (id int IDENTITY PRIMARY KEY, k int NOT NULL, payload char(800) NOT NULL);
CREATE TABLE dbo.[$low] (id int IDENTITY PRIMARY KEY, k int NOT NULL, payload char(800) NOT NULL);
INSERT dbo.[$high](k,payload) SELECT TOP (30000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 1000, REPLICATE('H',800) FROM sys.all_objects a CROSS JOIN sys.all_objects b;
INSERT dbo.[$low](k,payload) SELECT TOP (30000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 1000, REPLICATE('L',800) FROM sys.all_objects a CROSS JOIN sys.all_objects b;
CREATE INDEX IX_$high ON dbo.[$high](k);
CREATE INDEX IX_$low ON dbo.[$low](k);
UPDATE STATISTICS dbo.[$high] WITH FULLSCAN;
UPDATE STATISTICS dbo.[$low] WITH FULLSCAN;
"@
    Invoke-Sql $setup $Database 180

    $work = @"
DECLARE @i int = 0, @s bigint;
WHILE @i < 80
BEGIN
    SELECT @s = SUM(CONVERT(bigint, CHECKSUM(k, payload))) /* QSW_WORKLOAD_$run */ FROM dbo.[$high] WHERE k BETWEEN @i AND @i + 700;
    SET @i += 1;
END;
    SELECT @s = SUM(CONVERT(bigint, CHECKSUM(k, payload))) /* QSW_COMPARISON_$run */ FROM dbo.[$low] WHERE k BETWEEN 10 AND 20;
"@
    Invoke-Sql $work $Database 180
    Invoke-Sql 'EXEC sys.sp_query_store_flush_db;' $Database 60
    $capture = Invoke-Sql "SELECT COUNT_BIG(*) AS Captured FROM sys.query_store_query_text AS qt JOIN sys.query_store_query AS q ON q.query_text_id=qt.query_text_id JOIN sys.query_store_plan AS p ON p.query_id=q.query_id JOIN sys.query_store_runtime_stats AS rs ON rs.plan_id=p.plan_id WHERE qt.query_sql_text LIKE N'%QSW_WORKLOAD_$run%';" $Database 60 -Rows

    # Query Store keys by query text. Make sure the unique workload marker survived
    # capture before asking the procedure to rank its tables.
    if ($capture.Rows.Count -eq 1 -and [long]$capture.Rows[0]['Captured'] -gt 0) {
        Add-Check 'query_store_workload_candidates' 'PASS'
    } else {
        Add-Check 'query_store_workload_candidates' 'FAIL' 'Query Store did not retain runtime rows for this run.'
    }

    Invoke-Sql "INSERT dbo.[$high](k,payload) SELECT TOP (1000) 3, REPLICATE('X',800) FROM sys.all_objects; INSERT dbo.[$low](k,payload) SELECT TOP (1000) 3, REPLICATE('Y',800) FROM sys.all_objects;" $Database 60
    Invoke-Sql "IF OBJECT_ID(N'dbo.[$outTable]',N'U') IS NOT NULL DROP TABLE dbo.[$outTable];" 'master'
    Invoke-Sql "EXEC dbo.sp_StatUpdate @Databases=N'$Database', @Tables=N'dbo.$high,dbo.$low', @SortOrder=N'QUERY_STORE', @QueryStore=N'CPU', @QueryStoreRecentHours=1, @ModificationThreshold=1, @Execute=N'N', @LogToTable=N'N', @WhatIfOutputTable=N'dbo.$outTable';" 'master' 180
    $order = Invoke-Sql "SELECT TableName, SequenceNum FROM dbo.[$outTable] WHERE TableName IN (N'$high',N'$low') ORDER BY SequenceNum;" 'master' 30 -Rows
    $hRows = @($order.Rows | Where-Object { [string]$_.TableName -like "*$high" })
    $lRows = @($order.Rows | Where-Object { [string]$_.TableName -like "*$low" })
    $h = @($hRows | Sort-Object { [int]$_.SequenceNum } | Select-Object -First 1)
    $l = @($lRows | Sort-Object { [int]$_.SequenceNum } | Select-Object -First 1)
    if ($h.Count -eq 1 -and $l.Count -eq 1 -and [int]$h[0]['SequenceNum'] -lt [int]$l[0]['SequenceNum']) {
        Add-Check 'query_store_sort_order_observed' 'PASS'
    } else {
        $observed = @($order.Rows | ForEach-Object { ([string]$_.TableName) + '=' + ([string]$_.SequenceNum) }) -join ', '
        Add-Check 'query_store_sort_order_observed' 'FAIL' ('Observed=[' + $observed + ']; high_count=' + $h.Count + '; low_count=' + $l.Count + '.')
    }
} catch {
    $reason = $_.Exception.Message.Replace($Password, '[redacted]')
    foreach ($name in @('query_store_workload_candidates','query_store_sort_order_observed')) {
        if (@($checks | Where-Object check -eq $name).Count -eq 0) { Add-Check $name 'FAIL' "Suite setup/execution failed ($reason)." }
    }
} finally {
    try { Invoke-Sql "IF OBJECT_ID(N'dbo.[$outTable]',N'U') IS NOT NULL DROP TABLE dbo.[$outTable];" 'master' 30 } catch { }
    try { Invoke-Sql "IF OBJECT_ID(N'dbo.[$high]',N'U') IS NOT NULL DROP TABLE dbo.[$high]; IF OBJECT_ID(N'dbo.[$low]',N'U') IS NOT NULL DROP TABLE dbo.[$low];" $Database 30 } catch { }
    try {
        $status = Invoke-Sql "SELECT CONVERT(nvarchar(64), SERVERPROPERTY(N'ProductVersion')) AS Build;" 'master' 10 -Rows
        $sqlBuild = [string]$status.Rows[0]['Build']; $aliveAtEnd = $true
    } catch { Write-Warning "Server liveness probe failed ($($_.Exception.GetType().Name))." }
}

if ($ResultsPath) {
    if ($checks.Count -ne 2) { throw "Expected two checks; received $($checks.Count)." }
    $commit = (& git -C $repoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Import-Module (Join-Path $PSScriptRoot 'ReleaseResults.psm1') -Force
    $ended = Get-Date
    [void](Write-ReleaseRunList -Path $ResultsPath -Commit $commit -SqlVersion $version -SqlBuild $sqlBuild `
        -ServerAliveAtEnd $aliveAtEnd -Suite 'QueryStoreWorkloadSortOrder' -ExpectedCount 2 -Checks @($checks) `
        -StartedAt $started -EndedAt $ended -DurationMs ([long](($ended - $started).TotalMilliseconds)))
}
if (@($checks | Where-Object result -ne 'PASS').Count) { exit 1 }
exit 0
