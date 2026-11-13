<#
.SYNOPSIS
    @ExcludeTables LIKE wildcard tests (M6).

.DESCRIPTION
    Verifies that the @ExcludeTables parameter filters tables using LIKE
    wildcards, comma-separated patterns, and exact matches.

    Test matrix:
    - ET1: Suffix wildcard  -- @ExcludeTables = 'dbo.ET_TableA%'
           Matches ET_TableA and ET_TableAB; ET_TableB not matched, appears in output.
    - ET2: Prefix wildcard  -- @ExcludeTables = '%_TableB'
           Matches ET_TableB; ET_TableA appears in output.
    - ET3: Mixed patterns   -- @ExcludeTables = '%_TableA, %_TableB'
           Both ET_TableA and ET_TableB excluded; output contains neither.
    - ET4: Exact match      -- @ExcludeTables = 'dbo.ET_TableA'
           Only ET_TableA excluded; ET_TableB present in output.
    - ET5: Schema-missing pattern (no dot) -- @ExcludeTables = 'ET_TableA'
           The proc matches schema.table LIKE pattern.  Without a dot,
           the pattern cannot match 'dbo.ET_TableA', so ET_TableA should
           remain in the output.
    - ET6: No wildcard matches -- @ExcludeTables = 'dbo.DoesNotExist'
           Nothing excluded; all ET_ tables still appear in output.

    Uses @WhatIfOutputTable + @Execute=N to inspect which tables were
    included in discovery without actually running UPDATE STATISTICS.

.PARAMETER Server
    Remote Docker host IP.  Default: 100.88.59.72.

.PARAMETER Port
    SQL Server port.  Default: 1432 (SQL 2022 on remote host).

.PARAMETER Password
    SA password.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Database
    Test database name.  Default: StatUpdateTest.

.EXAMPLE
    .\tests\Test-ExcludeTablesWildcards.ps1
    .\tests\Test-ExcludeTablesWildcards.ps1 -Port 1431
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Server,
    [ValidateSet('2019','2022','2025')][string]$Version = '2022',
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [string]$Database = 'StatUpdateTest',
    [string]$ResultsPath = '',
    [switch]$SkipSetup
)

$ErrorActionPreference = "Continue"
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }
$Port = @{ '2019' = 1434; '2022' = 1432; '2025' = 1431 }[$Version]
$startedAt = Get-Date
$repoRoot = Split-Path -Parent $PSScriptRoot
Import-Module (Join-Path $PSScriptRoot 'StructuredLegacyResults.psm1') -Force

# =====================================================================
# ADO.NET HELPER
# =====================================================================
function Invoke-Sql {
    param(
        [string]$ServerAddr,
        [int]   $Port,
        [string]$Pwd,
        [string]$Query,
        [string]$Db      = "master",
        [int]   $Timeout = 300
    )
    $connStr = "Server=$ServerAddr,$Port;Database=$Db;User Id=sa;Password=$Pwd;" +
               "Encrypt=false;TrustServerCertificate=true;Connection Timeout=30;Pooling=false"
    $messages = [System.Collections.ArrayList]::new()

    try {
        $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
        $conn.add_InfoMessage({
            param($sender, $e)
            $null = $messages.Add($e.Message)
        })
        $conn.FireInfoMessageEventOnUserErrors = $true
        $conn.Open()

        $cmd                = $conn.CreateCommand()
        $cmd.CommandText    = $Query
        $cmd.CommandTimeout = $Timeout

        $ds     = [System.Data.DataSet]::new()
        $reader = $cmd.ExecuteReader()
        do {
            $dt = [System.Data.DataTable]::new()
            $dt.Load($reader)
            $null = $ds.Tables.Add($dt)
        } while (-not $reader.IsClosed)

        $conn.Close()
        return @{ Output = $messages; DataSet = $ds; ExitCode = 0 }
    }
    catch {
        try { if ($conn -and $conn.State -eq 'Open') { $conn.Close() } } catch { }
        $inner = if ($_.Exception.InnerException) { $_.Exception.InnerException.Message } else { "" }
        return @{ Output = @($inner, $_.Exception.Message) + $messages; DataSet = $null; ExitCode = 1 }
    }
}

function Clear-StatUpdateApplock {
    param([string]$ServerAddr, [int]$Port, [string]$Pwd)

    $r = Invoke-Sql -ServerAddr $ServerAddr -Port $Port -Pwd $Pwd -Query "
        SELECT request_session_id AS spid
        FROM   sys.dm_tran_locks
        WHERE  resource_type        = N'APPLICATION'
        AND    resource_description LIKE N'%sp_StatUpdate%';"

    if ($r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
        foreach ($row in $r.DataSet.Tables[0].Rows) {
            $spid = [int]$row["spid"]
            Invoke-Sql -ServerAddr $ServerAddr -Port $Port -Pwd $Pwd `
                -Query "IF EXISTS (SELECT 1 FROM sys.dm_exec_sessions WHERE session_id = $spid) EXEC(N'KILL $spid');" | Out-Null
        }
    }

    $deadline = (Get-Date).AddSeconds(10)
    while ((Get-Date) -lt $deadline) {
        $check = Invoke-Sql -ServerAddr $ServerAddr -Port $Port -Pwd $Pwd -Query "
            SELECT COUNT(*) AS cnt
            FROM   sys.dm_tran_locks
            WHERE  resource_type        = N'APPLICATION'
            AND    resource_description LIKE N'%sp_StatUpdate%';"
        if ($check.DataSet -and $check.DataSet.Tables.Count -gt 0 -and
            [int]$check.DataSet.Tables[0].Rows[0]["cnt"] -eq 0) { break }
        Start-Sleep -Milliseconds 500
    }
}

$results = [System.Collections.ArrayList]::new()

function Add-Result {
    param([string]$Test, [string]$Result, [string]$Detail = "")
    $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail })
    $color = switch ($Result) { "PASS" { "Green" } "FAIL" { "Red" } "SKIP" { "DarkGray" } default { "Yellow" } }
    $d = if ($Detail) { " ($Detail)" } else { "" }
    Write-Host "  [$Result] $Test$d" -ForegroundColor $color
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " @ExcludeTables Wildcard Tests (ET1-ET6)" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:   ${Server}:${Port}"
Write-Host "  Database: $Database"
Write-Host ""

# =====================================================================
# CONNECTIVITY
# =====================================================================
Write-Host "[Setup] Verifying connectivity..." -ForegroundColor Yellow
$vr = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "SELECT @@VERSION AS v;"
if ($vr.ExitCode -ne 0) {
    Write-Host "  ERROR: Cannot connect -- $($vr.Output | Select-Object -First 1)" -ForegroundColor Red
    exit 1
}
Write-Host "  Connected" -ForegroundColor Green
Write-Host ""

# =====================================================================
# SETUP
# Creates three test tables:
#   dbo.ET_TableA   -- standard table
#   dbo.ET_TableAB  -- name starting like ET_TableA (for suffix-wildcard tests)
#   dbo.ET_TableB   -- standard table with different name
# All get a manual statistic and enough rows to have modification_counter > 1.
# =====================================================================
Write-Host "[Setup] Creating ExcludeTables test tables..." -ForegroundColor Yellow

$tables = @("ET_TableA", "ET_TableAB", "ET_TableB")

foreach ($tbl in $tables) {
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
        IF OBJECT_ID(N'dbo.$tbl', N'U') IS NOT NULL DROP TABLE dbo.$tbl;" | Out-Null

    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
        CREATE TABLE dbo.$tbl
        (
            Id   int          NOT NULL IDENTITY(1,1) PRIMARY KEY,
            ColA nvarchar(50) NOT NULL
        );" | Out-Null

    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
        INSERT INTO dbo.$tbl (ColA)
        SELECT TOP 5000 N'setup' FROM sys.all_columns;" | Out-Null

    # Create explicit stat so there is always at least one non-auto stat
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
        IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE object_id = OBJECT_ID(N'dbo.$tbl') AND name = N'ET_Stat_$tbl')
            CREATE STATISTICS [ET_Stat_$tbl] ON dbo.$tbl (ColA);" | Out-Null

    # Insert additional rows after stat creation to drive modification_counter
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
        INSERT INTO dbo.$tbl (ColA)
        SELECT TOP 5000 N'delta' FROM sys.all_columns;" | Out-Null
}

Write-Host "  Tables created: $($tables -join ', ')" -ForegroundColor Green
Write-Host ""

# Helper: run dry-run with @ExcludeTables and a temp WIO table; return TableName list from output
function Invoke-ExcludeTest {
    param(
        [string]$Suffix,
        [string]$ExcludeTablesParam,
        [string]$TablesParam = "dbo.ET_TableA, dbo.ET_TableAB, dbo.ET_TableB"
    )

    $wio = "dbo.WhatIfET_$Suffix"
    # WIO table is created in master (proc installs and runs in master context)
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" `
        -Query "IF OBJECT_ID(N'$wio', N'U') IS NOT NULL DROP TABLE $wio;" | Out-Null

    $excludeClause = if ($ExcludeTablesParam) { "@ExcludeTables = N'$ExcludeTablesParam'," } else { "" }
    $tablesClause  = if ($TablesParam)        { "@Tables = N'$TablesParam',"               } else { "" }

    $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
        -Query "
            EXEC dbo.sp_StatUpdate
                @Databases            = N'$Database',
                $tablesClause
                $excludeClause
                @ModificationThreshold = 1,
                @Execute              = N'N',
                @LogToTable           = N'N',
                @WhatIfOutputTable    = N'$wio';"

    Clear-StatUpdateApplock -ServerAddr $Server -Port $Port -Pwd $Password

    if ($r.ExitCode -ne 0) {
        return @{ Error = ($r.Output | Where-Object { $_ -notmatch "^\s*$" } | Select-Object -First 1); Tables = @() }
    }

    $r2 = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" -Query "
        SELECT DISTINCT TableName FROM $wio ORDER BY TableName;"

    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" `
        -Query "IF OBJECT_ID(N'$wio', N'U') IS NOT NULL DROP TABLE $wio;" | Out-Null

    if ($r2.ExitCode -ne 0 -or $r2.DataSet.Tables.Count -eq 0) {
        return @{ Error = "Cannot read $wio"; Tables = @() }
    }

    $tableNames = @($r2.DataSet.Tables[0].Rows | ForEach-Object { [string]$_["TableName"] })
    return @{ Error = $null; Tables = $tableNames }
}

# =====================================================================
# ET1: Suffix wildcard 'dbo.ET_TableA%' excludes ET_TableA and ET_TableAB
# ET_TableB must appear in output.
# =====================================================================
Write-Host "[Tests] ET1: suffix wildcard dbo.ET_TableA%..." -ForegroundColor Yellow

$res = Invoke-ExcludeTest -Suffix "ET1" -ExcludeTablesParam "dbo.ET_TableA%"
if ($res.Error) {
    Add-Result "ET1: suffix wildcard 'dbo.ET_TableA%' -- proc runs" "FAIL" $res.Error
}
else {
    Add-Result "ET1: suffix wildcard 'dbo.ET_TableA%' -- proc runs" "PASS" "Tables in output: $($res.Tables -join ', ')"

    $hasA  = $res.Tables -contains "ET_TableA"
    $hasAB = $res.Tables -contains "ET_TableAB"
    $hasB  = $res.Tables -contains "ET_TableB"

    if (-not $hasA) {
        Add-Result "ET1: ET_TableA excluded" "PASS" "ET_TableA not in WhatIfOutputTable"
    } else {
        Add-Result "ET1: ET_TableA excluded" "FAIL" "ET_TableA still appears in WhatIfOutputTable"
    }

    if (-not $hasAB) {
        Add-Result "ET1: ET_TableAB excluded (also matches A%)" "PASS" "ET_TableAB not in WhatIfOutputTable"
    } else {
        Add-Result "ET1: ET_TableAB excluded (also matches A%)" "FAIL" "ET_TableAB still appears in WhatIfOutputTable"
    }

    if ($hasB) {
        Add-Result "ET1: ET_TableB present (not matched by A%)" "PASS" "ET_TableB in WhatIfOutputTable"
    } else {
        Add-Result "ET1: ET_TableB present (not matched by A%)" "FAIL" "ET_TableB missing from WhatIfOutputTable"
    }
}

# =====================================================================
# ET2: Prefix wildcard '%_TableB' excludes ET_TableB
# ET_TableA and ET_TableAB must appear in output.
# =====================================================================
Write-Host "[Tests] ET2: prefix wildcard %_TableB..." -ForegroundColor Yellow

$res = Invoke-ExcludeTest -Suffix "ET2" -ExcludeTablesParam "%_TableB"
if ($res.Error) {
    Add-Result "ET2: prefix wildcard '%_TableB' -- proc runs" "FAIL" $res.Error
}
else {
    Add-Result "ET2: prefix wildcard '%_TableB' -- proc runs" "PASS" "Tables in output: $($res.Tables -join ', ')"

    $hasA  = $res.Tables -contains "ET_TableA"
    $hasAB = $res.Tables -contains "ET_TableAB"
    $hasB  = $res.Tables -contains "ET_TableB"

    if (-not $hasB) {
        Add-Result "ET2: ET_TableB excluded" "PASS" "ET_TableB not in WhatIfOutputTable"
    } else {
        Add-Result "ET2: ET_TableB excluded" "FAIL" "ET_TableB still appears (pattern '%_TableB' should match)"
    }

    if ($hasA -or $hasAB) {
        Add-Result "ET2: ET_TableA/AB present (not matched by %_TableB)" "PASS" "Tables retained: $($res.Tables -join ', ')"
    } else {
        Add-Result "ET2: ET_TableA/AB present (not matched by %_TableB)" "FAIL" "Both ET_TableA and ET_TableAB missing from output"
    }
}

# =====================================================================
# ET3: Multi-pattern 'dbo.ET_TableA%, dbo.ET_TableB' excludes all three
# =====================================================================
Write-Host "[Tests] ET3: multi-pattern (exclude all three tables)..." -ForegroundColor Yellow

$res = Invoke-ExcludeTest -Suffix "ET3" -ExcludeTablesParam "dbo.ET_TableA%, dbo.ET_TableB"
if ($res.Error) {
    Add-Result "ET3: multi-pattern -- proc runs" "FAIL" $res.Error
}
else {
    Add-Result "ET3: multi-pattern -- proc runs" "PASS" "Tables in output: $($res.Tables -join ', ')"

    $etTables = $res.Tables | Where-Object { $_ -match "^ET_Table" }
    if (($etTables | Measure-Object).Count -eq 0) {
        Add-Result "ET3: All ET_ tables excluded by multi-pattern" "PASS" "No ET_Table* rows in output"
    } else {
        Add-Result "ET3: All ET_ tables excluded by multi-pattern" "FAIL" "Still present: $($etTables -join ', ')"
    }
}

# =====================================================================
# ET4: Exact match 'dbo.ET_TableA' excludes only ET_TableA
# =====================================================================
Write-Host "[Tests] ET4: exact match dbo.ET_TableA..." -ForegroundColor Yellow

$res = Invoke-ExcludeTest -Suffix "ET4" -ExcludeTablesParam "dbo.ET_TableA"
if ($res.Error) {
    Add-Result "ET4: exact match -- proc runs" "FAIL" $res.Error
}
else {
    Add-Result "ET4: exact match -- proc runs" "PASS" "Tables in output: $($res.Tables -join ', ')"

    $hasA  = $res.Tables -contains "ET_TableA"
    $hasAB = $res.Tables -contains "ET_TableAB"
    $hasB  = $res.Tables -contains "ET_TableB"

    if (-not $hasA) {
        Add-Result "ET4: ET_TableA excluded (exact match)" "PASS" ""
    } else {
        Add-Result "ET4: ET_TableA excluded (exact match)" "FAIL" "ET_TableA still in output"
    }

    # ET_TableAB shares prefix but is NOT an exact match -- must remain
    if ($hasAB) {
        Add-Result "ET4: ET_TableAB not excluded by exact match" "PASS" ""
    } else {
        Add-Result "ET4: ET_TableAB not excluded by exact match" "FAIL" "ET_TableAB was excluded by exact match (over-matching)"
    }
}

# =====================================================================
# ET5: Pattern without schema prefix ('ET_TableA' -- no dot)
# The proc joins schema + '.' + table against the pattern.
# 'ET_TableA' (no dot) can only match when the schema+table string
# literally contains 'ET_TableA' with LIKE -- which 'dbo.ET_TableA' does
# because LIKE '%ET_TableA%' would, but 'ET_TableA' alone is an anchor.
# The proc uses: schema.table COLLATE DATABASE_DEFAULT LIKE pattern
# So 'dbo.ET_TableA' LIKE 'ET_TableA' is FALSE (no wildcard, anchored).
# Expected: ET_TableA still appears in output (pattern does not match).
# =====================================================================
Write-Host "[Tests] ET5: schema-missing pattern 'ET_TableA' (no dot)..." -ForegroundColor Yellow

$res = Invoke-ExcludeTest -Suffix "ET5" -ExcludeTablesParam "ET_TableA"
if ($res.Error) {
    Add-Result "ET5: schema-less pattern -- proc runs" "FAIL" $res.Error
}
else {
    Add-Result "ET5: schema-less pattern -- proc runs" "PASS" "Tables in output: $($res.Tables -join ', ')"

    $hasA = $res.Tables -contains "ET_TableA"
    # Without schema prefix and no wildcard, 'dbo.ET_TableA' LIKE 'ET_TableA' is FALSE
    # so ET_TableA should NOT be excluded and should appear in output.
    if ($hasA) {
        Add-Result "ET5: ET_TableA present when pattern has no schema prefix" "PASS" "Pattern 'ET_TableA' (no dot) does not match 'dbo.ET_TableA'"
    } else {
        # If the proc does partial matching, ET_TableA may be excluded.  Document the behavior.
        Add-Result "ET5: ET_TableA present when pattern has no schema prefix" "SKIP" "ET_TableA excluded -- pattern 'ET_TableA' matched 'dbo.ET_TableA' (proc does substring/partial match)"
    }
}

# =====================================================================
# ET6: Pattern that matches nothing ('dbo.DoesNotExist')
# All ET_ tables must still appear in output.
# =====================================================================
Write-Host "[Tests] ET6: pattern matches nothing..." -ForegroundColor Yellow

$res = Invoke-ExcludeTest -Suffix "ET6" -ExcludeTablesParam "dbo.DoesNotExist"
if ($res.Error) {
    Add-Result "ET6: non-matching pattern -- proc runs" "FAIL" $res.Error
}
else {
    Add-Result "ET6: non-matching pattern -- proc runs" "PASS" "Tables in output: $($res.Tables -join ', ')"

    $hasA  = $res.Tables -contains "ET_TableA"
    $hasAB = $res.Tables -contains "ET_TableAB"
    $hasB  = $res.Tables -contains "ET_TableB"

    if ($hasA -and $hasAB -and $hasB) {
        Add-Result "ET6: All ET_ tables present (nothing excluded)" "PASS" ""
    } elseif (-not $hasA -or -not $hasAB -or -not $hasB) {
        $missing = @()
        if (-not $hasA)  { $missing += "ET_TableA" }
        if (-not $hasAB) { $missing += "ET_TableAB" }
        if (-not $hasB)  { $missing += "ET_TableB" }
        # Tables with 0 qualifying stats also won't appear -- treat as SKIP not FAIL
        Add-Result "ET6: All ET_ tables present (nothing excluded)" "SKIP" "Missing from output (may have 0 qualifying stats): $($missing -join ', ')"
    }
}

# =====================================================================
# CLEANUP
# =====================================================================
foreach ($tbl in $tables) {
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database `
        -Query "IF OBJECT_ID(N'dbo.$tbl', N'U') IS NOT NULL DROP TABLE dbo.$tbl;" | Out-Null
}

# =====================================================================
# SUMMARY
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " SUMMARY" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""

$passed  = @($results | Where-Object { $_.Result -eq "PASS" }).Count
$failed  = @($results | Where-Object { $_.Result -eq "FAIL" }).Count
$skipped = @($results | Where-Object { $_.Result -eq "SKIP" }).Count
$total   = $passed + $failed + $skipped

if ($ResultsPath) {
    if ($results.Count -ne 16) { throw "Expected 16 ExcludeTablesWildcards checks; received $($results.Count)." }
    $major = @{ '2019' = 15; '2022' = 16; '2025' = 17 }[$Version]
    $status = Get-StructuredSqlStatus -Server $Server -Port $Port -Password $Password -ExpectedMajor $major
    $commit = (& git -C $repoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $commit -SqlVersion $Version -SqlBuild $status.Build `
        -ServerAliveAtEnd $status.Alive -Suite 'ExcludeTablesWildcards' -ExpectedCount 16 `
        -Results @($results) -StartedAt $startedAt -EndedAt (Get-Date)
}

$color = if ($failed -eq 0) { "Green" } else { "Red" }
Write-Host "  $passed/$total passed, $failed failed, $skipped skipped" -ForegroundColor $color
Write-Host ""

if ($failed -gt 0 -or $skipped -gt 0) {
    Write-Host "  FAILED:" -ForegroundColor Red
    foreach ($t in ($results | Where-Object { $_.Result -eq "FAIL" })) {
        Write-Host "    - $($t.Test): $($t.Detail)" -ForegroundColor Red
    }
    Write-Host ""
    exit 1
}

exit 0
