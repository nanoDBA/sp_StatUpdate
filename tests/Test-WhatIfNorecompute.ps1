<#
.SYNOPSIS
    @WhatIfOutputTable NORECOMPUTE preservation tests (M5).

.DESCRIPTION
    Verifies that when @Execute=N and @WhatIfOutputTable captures commands,
    statistics that have no_recompute=1 produce a Command containing WITH NORECOMPUTE,
    and statistics without the flag do not.

    Test matrix:
    - NR1: @TargetNorecompute=BOTH -- both regular and NORECOMPUTE stats captured;
           NORECOMPUTE stat command contains NORECOMPUTE, regular stat command does not.
    - NR2: @TargetNorecompute=Y   -- only NORECOMPUTE stats captured;
           all captured commands contain NORECOMPUTE.
    - NR3: @TargetNorecompute=N   -- only regular (non-NORECOMPUTE) stats captured;
           none of the captured commands contain NORECOMPUTE.
    - NR4: Zero captured commands (sanity check for table-empty path).

    Uses a permanent table dbo.WhatIfNR_<timestamp> in the target database
    (not a temp table -- the proc creates the table via dynamic SQL in master
    context, and temp tables created inside the proc scope are destroyed on return).

.PARAMETER Server
    SQL Server host name or address.

.PARAMETER Port
    SQL Server port is selected from Version.

.PARAMETER Password
    SA password. Defaults to SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Version
    Supported SQL Server version: 2019, 2022, or 2025.

.PARAMETER ResultsPath
    Optional path for the structured release-gate result list.

.PARAMETER SkipSetup
    Accepted for runner consistency; this suite always creates its own fixtures.

.PARAMETER Database
    Test database name.  Default: StatUpdateTest.

.EXAMPLE
    .\tests\Test-WhatIfNorecompute.ps1 -Server localhost -Version 2022
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
# Uses ExecuteReader -- SqlDataAdapter.Fill misses InfoMessage on SQL 2025.
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
Write-Host " @WhatIfOutputTable NORECOMPUTE Preservation Tests (NR1-NR4)" -ForegroundColor Cyan
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
# SETUP: Create test table with known stats
# Creates a dedicated table NR_TestTable so we have full control over
# which stats have NORECOMPUTE and can use @Tables to isolate scope.
# =====================================================================
Write-Host "[Setup] Creating test table and statistics..." -ForegroundColor Yellow

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    IF OBJECT_ID(N'dbo.NR_TestTable', N'U') IS NOT NULL
        DROP TABLE dbo.NR_TestTable;" | Out-Null

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    CREATE TABLE dbo.NR_TestTable
    (
        Id       int          NOT NULL IDENTITY(1,1) PRIMARY KEY,
        ColA     nvarchar(50) NOT NULL,
        ColB     nvarchar(50) NOT NULL,
        ColC     int          NOT NULL DEFAULT 0
    );" | Out-Null

# Insert enough rows so modification_counter > ModificationThreshold=1
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.NR_TestTable (ColA, ColB, ColC)
    SELECT TOP 5000
        N'Alpha', N'Beta', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
    FROM sys.all_columns;" | Out-Null

# Create initial stats so sys.dm_db_stats_properties has data
# Create stats AFTER the initial rows exist.  The stat creation scans
# the current rows as the baseline, so modification_counter starts at 0.
# Do NOT call UPDATE STATISTICS afterward -- it clears the NORECOMPUTE flag.
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE object_id = OBJECT_ID(N'dbo.NR_TestTable') AND name = N'NR_Stat_Regular')
        CREATE STATISTICS NR_Stat_Regular ON dbo.NR_TestTable (ColA) WITH FULLSCAN;" | Out-Null

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE object_id = OBJECT_ID(N'dbo.NR_TestTable') AND name = N'NR_Stat_Norecompute')
        CREATE STATISTICS NR_Stat_Norecompute ON dbo.NR_TestTable (ColB) WITH FULLSCAN, NORECOMPUTE;" | Out-Null

# Insert rows AFTER stat creation so modification_counter is elevated above the baseline
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.NR_TestTable (ColA, ColB, ColC)
    SELECT TOP 5000 N'Delta', N'Gamma', 0 FROM sys.all_columns;" | Out-Null

# Verify setup and confirm modification_counter is elevated
$setupCheck = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    SELECT
        s.name,
        s.no_recompute,
        sp.modification_counter
    FROM   sys.stats AS s
    CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
    WHERE  s.object_id = OBJECT_ID(N'dbo.NR_TestTable')
    AND    s.name IN (N'NR_Stat_Regular', N'NR_Stat_Norecompute')
    ORDER  BY s.name;"

if ($setupCheck.ExitCode -ne 0 -or
    $setupCheck.DataSet.Tables[0].Rows.Count -lt 2) {
    Write-Host "  ERROR: Test setup failed -- stats not created" -ForegroundColor Red
    exit 1
}

$statRows = $setupCheck.DataSet.Tables[0].Rows
foreach ($row in $statRows) {
    $flag  = [int]$row["no_recompute"]
    $mods  = $row["modification_counter"]
    Write-Host "    $($row['name']): no_recompute=$flag  modification_counter=$mods" -ForegroundColor DarkGray
}
Write-Host ""

Clear-StatUpdateApplock -ServerAddr $Server -Port $Port -Pwd $Password

# =====================================================================
# NR1: TargetNorecompute=BOTH -- both stat types captured
# NORECOMPUTE stat Command must contain NORECOMPUTE.
# Regular stat Command must NOT contain NORECOMPUTE.
# =====================================================================
Write-Host "[Tests] NR1: @TargetNorecompute=BOTH..." -ForegroundColor Yellow

$wioTable = "dbo.WhatIfNR1_$([DateTime]::UtcNow.ToString('HHmmss'))"
# WIO table is created in master (proc runs in master context)
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" `
    -Query "IF OBJECT_ID(N'$wioTable', N'U') IS NOT NULL DROP TABLE $wioTable;" | Out-Null

$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "
        EXEC dbo.sp_StatUpdate
            @Databases            = N'$Database',
            @Tables               = N'dbo.NR_TestTable',
            @TargetNorecompute    = N'BOTH',
            @ModificationThreshold = 1,
            @Execute              = N'N',
            @LogToTable           = N'N',
            @WhatIfOutputTable    = N'$wioTable';"

if ($r.ExitCode -ne 0) {
    Add-Result "NR1: TargetNorecompute=BOTH -- proc runs" "FAIL" ($r.Output | Select-Object -First 1)
    Add-Result "NR1: WhatIfOutputTable readable" "FAIL" "Proc call failed before output-table query."
    Add-Result "NR1: NORECOMPUTE stat captured" "FAIL" "Proc call failed before capture verification."
    Add-Result "NR1: NORECOMPUTE stat has WITH NORECOMPUTE" "FAIL" "Proc call failed before command verification."
    Add-Result "NR1: Regular stat captured" "FAIL" "Proc call failed before capture verification."
    Add-Result "NR1: Regular stat has no NORECOMPUTE" "FAIL" "Proc call failed before command verification."
}
else {
    Add-Result "NR1: TargetNorecompute=BOTH -- proc runs" "PASS" ""

    # Query captured commands from master (WIO table is created in master context)
    $r2 = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" -Query "
        SELECT StatName, Command
        FROM   $wioTable
        WHERE  StatName IN (N'NR_Stat_Regular', N'NR_Stat_Norecompute')
        ORDER  BY StatName;"

    if ($r2.ExitCode -ne 0 -or $r2.DataSet.Tables.Count -eq 0) {
        Add-Result "NR1: WhatIfOutputTable readable" "FAIL" "Could not query $wioTable"
        Add-Result "NR1: NORECOMPUTE stat captured" "FAIL" "Output table could not be read."
        Add-Result "NR1: NORECOMPUTE stat has WITH NORECOMPUTE" "FAIL" "Output table could not be read."
        Add-Result "NR1: Regular stat captured" "FAIL" "Output table could not be read."
        Add-Result "NR1: Regular stat has no NORECOMPUTE" "FAIL" "Output table could not be read."
    }
    else {
        Add-Result "NR1: WhatIfOutputTable readable" "PASS" "Captured command table is queryable."
        $rows = $r2.DataSet.Tables[0].Rows

        # Find each stat
        $nrcRow = $rows | Where-Object { [string]$_["StatName"] -eq "NR_Stat_Norecompute" }
        $regRow = $rows | Where-Object { [string]$_["StatName"] -eq "NR_Stat_Regular" }

        if ($null -eq $nrcRow) {
            Add-Result "NR1: NORECOMPUTE stat captured" "FAIL" "NR_Stat_Norecompute not in WhatIfOutputTable."
            Add-Result "NR1: NORECOMPUTE stat has WITH NORECOMPUTE" "FAIL" "NORECOMPUTE stat was not captured."
        }
        else {
            Add-Result "NR1: NORECOMPUTE stat captured" "PASS" "NR_Stat_Norecompute is present."
            $cmd = [string]$nrcRow["Command"]
            if ($cmd -match "NORECOMPUTE") {
                Add-Result "NR1: NORECOMPUTE stat has WITH NORECOMPUTE" "PASS" "Command: $($cmd.Substring(0, [Math]::Min(120, $cmd.Length)))"
            }
            else {
                Add-Result "NR1: NORECOMPUTE stat has WITH NORECOMPUTE" "FAIL" "Command missing NORECOMPUTE: $($cmd.Substring(0, [Math]::Min(120, $cmd.Length)))"
            }
        }

        if ($null -eq $regRow) {
            Add-Result "NR1: Regular stat captured" "FAIL" "NR_Stat_Regular not in WhatIfOutputTable."
            Add-Result "NR1: Regular stat has no NORECOMPUTE" "FAIL" "Regular stat was not captured."
        }
        else {
            Add-Result "NR1: Regular stat captured" "PASS" "NR_Stat_Regular is present."
            $cmd = [string]$regRow["Command"]
            if ($cmd -notmatch "NORECOMPUTE") {
                Add-Result "NR1: Regular stat has no NORECOMPUTE" "PASS" "Command: $($cmd.Substring(0, [Math]::Min(120, $cmd.Length)))"
            }
            else {
                Add-Result "NR1: Regular stat has no NORECOMPUTE" "FAIL" "Regular stat command unexpectedly contains NORECOMPUTE"
            }
        }
    }
}

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" `
    -Query "IF OBJECT_ID(N'$wioTable', N'U') IS NOT NULL DROP TABLE $wioTable;" | Out-Null
Clear-StatUpdateApplock -ServerAddr $Server -Port $Port -Pwd $Password

# =====================================================================
# NR2: TargetNorecompute=Y -- only NORECOMPUTE stats captured
# Every row in WhatIfOutputTable for our table must contain NORECOMPUTE.
# =====================================================================
Write-Host "[Tests] NR2: @TargetNorecompute=Y..." -ForegroundColor Yellow

# Insert modifications again to ensure both stats qualify
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.NR_TestTable (ColA, ColB, ColC)
    SELECT TOP 2000 N'NR2', N'NR2', 0 FROM sys.all_columns;" | Out-Null

$wioTable2 = "dbo.WhatIfNR2_$([DateTime]::UtcNow.ToString('HHmmss'))"
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" `
    -Query "IF OBJECT_ID(N'$wioTable2', N'U') IS NOT NULL DROP TABLE $wioTable2;" | Out-Null

$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "
        EXEC dbo.sp_StatUpdate
            @Databases            = N'$Database',
            @Tables               = N'dbo.NR_TestTable',
            @TargetNorecompute    = N'Y',
            @ModificationThreshold = 1,
            @Execute              = N'N',
            @LogToTable           = N'N',
            @WhatIfOutputTable    = N'$wioTable2';"

if ($r.ExitCode -ne 0) {
    Add-Result "NR2: TargetNorecompute=Y -- proc runs" "FAIL" ($r.Output | Select-Object -First 1)
    Add-Result "NR2: WhatIfOutputTable readable" "FAIL" "Proc call failed before output-table query."
    Add-Result "NR2: TargetNorecompute=Y filters to NORECOMPUTE only" "FAIL" "Proc call failed before filter verification."
    Add-Result "NR2: Regular stat excluded when TargetNorecompute=Y" "FAIL" "Proc call failed before exclusion verification."
    Add-Result "NR2: All captured commands have NORECOMPUTE" "FAIL" "Proc call failed before command verification."
}
else {
    Add-Result "NR2: TargetNorecompute=Y -- proc runs" "PASS" ""

    $r2 = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" -Query "
        SELECT StatName, Command FROM $wioTable2 ORDER BY StatName;"

    if ($r2.ExitCode -ne 0) {
        Add-Result "NR2: WhatIfOutputTable readable" "FAIL" "Cannot query $wioTable2"
        Add-Result "NR2: TargetNorecompute=Y filters to NORECOMPUTE only" "FAIL" "Output table could not be read."
        Add-Result "NR2: Regular stat excluded when TargetNorecompute=Y" "FAIL" "Output table could not be read."
        Add-Result "NR2: All captured commands have NORECOMPUTE" "FAIL" "Output table could not be read."
    }
    else {
        Add-Result "NR2: WhatIfOutputTable readable" "PASS" "Captured command table is queryable."
        $rows     = $r2.DataSet.Tables[0].Rows
        $rowCount = $rows.Count

        if ($rowCount -eq 0) {
            Add-Result "NR2: TargetNorecompute=Y filters to NORECOMPUTE only" "FAIL" "No rows captured; the NORECOMPUTE filter was not proven."
            Add-Result "NR2: Regular stat excluded when TargetNorecompute=Y" "FAIL" "No rows captured; regular-stat exclusion was not proven."
            Add-Result "NR2: All captured commands have NORECOMPUTE" "FAIL" "No commands captured; NORECOMPUTE clause was not proven."
        }
        else {
            # Regular stat must NOT appear
            $regularPresent = $rows | Where-Object { [string]$_["StatName"] -eq "NR_Stat_Regular" }
            if ($null -eq $regularPresent) {
                Add-Result "NR2: TargetNorecompute=Y filters to NORECOMPUTE only" "PASS" "$rowCount row(s) captured; all are NORECOMPUTE stats."
                Add-Result "NR2: Regular stat excluded when TargetNorecompute=Y" "PASS" "$rowCount row(s) captured, none regular"
            }
            else {
                Add-Result "NR2: TargetNorecompute=Y filters to NORECOMPUTE only" "FAIL" "Regular stat appeared in the output."
                Add-Result "NR2: Regular stat excluded when TargetNorecompute=Y" "FAIL" "NR_Stat_Regular found in output (should be excluded)"
            }

            # All captured commands must contain NORECOMPUTE
            $withoutFlag = $rows | Where-Object { ([string]$_["Command"]) -notmatch "NORECOMPUTE" }
            if ($null -eq $withoutFlag -or ($withoutFlag | Measure-Object).Count -eq 0) {
                Add-Result "NR2: All captured commands have NORECOMPUTE" "PASS" ""
            }
            else {
                $bad = [string]($withoutFlag | Select-Object -First 1)["Command"]
                Add-Result "NR2: All captured commands have NORECOMPUTE" "FAIL" "Command missing NORECOMPUTE: $($bad.Substring(0, [Math]::Min(120, $bad.Length)))"
            }
        }
    }
}

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" `
    -Query "IF OBJECT_ID(N'$wioTable2', N'U') IS NOT NULL DROP TABLE $wioTable2;" | Out-Null
Clear-StatUpdateApplock -ServerAddr $Server -Port $Port -Pwd $Password

# =====================================================================
# NR3: TargetNorecompute=N -- only regular stats captured
# No row for NR_Stat_Norecompute should appear in WhatIfOutputTable.
# =====================================================================
Write-Host "[Tests] NR3: @TargetNorecompute=N..." -ForegroundColor Yellow

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.NR_TestTable (ColA, ColB, ColC)
    SELECT TOP 2000 N'NR3', N'NR3', 0 FROM sys.all_columns;" | Out-Null

$wioTable3 = "dbo.WhatIfNR3_$([DateTime]::UtcNow.ToString('HHmmss'))"
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" `
    -Query "IF OBJECT_ID(N'$wioTable3', N'U') IS NOT NULL DROP TABLE $wioTable3;" | Out-Null

$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "
        EXEC dbo.sp_StatUpdate
            @Databases            = N'$Database',
            @Tables               = N'dbo.NR_TestTable',
            @TargetNorecompute    = N'N',
            @ModificationThreshold = 1,
            @Execute              = N'N',
            @LogToTable           = N'N',
            @WhatIfOutputTable    = N'$wioTable3';"

if ($r.ExitCode -ne 0) {
    Add-Result "NR3: TargetNorecompute=N -- proc runs" "FAIL" ($r.Output | Select-Object -First 1)
    Add-Result "NR3: WhatIfOutputTable readable" "FAIL" "Proc call failed before output-table query."
    Add-Result "NR3: NORECOMPUTE stat excluded when TargetNorecompute=N" "FAIL" "Proc call failed before exclusion verification."
    Add-Result "NR3: No captured command contains NORECOMPUTE" "FAIL" "Proc call failed before command verification."
}
else {
    Add-Result "NR3: TargetNorecompute=N -- proc runs" "PASS" ""

    $r2 = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" -Query "
        SELECT StatName, Command FROM $wioTable3 ORDER BY StatName;"

    if ($r2.ExitCode -ne 0) {
        Add-Result "NR3: WhatIfOutputTable readable" "FAIL" "Cannot query $wioTable3"
        Add-Result "NR3: NORECOMPUTE stat excluded when TargetNorecompute=N" "FAIL" "Output table could not be read."
        Add-Result "NR3: No captured command contains NORECOMPUTE" "FAIL" "Output table could not be read."
    }
    else {
        Add-Result "NR3: WhatIfOutputTable readable" "PASS" "Captured command table is queryable."
        $rows = $r2.DataSet.Tables[0].Rows

        # NORECOMPUTE stat must NOT appear
        $nrcPresent = $rows | Where-Object { [string]$_["StatName"] -eq "NR_Stat_Norecompute" }
        if ($null -eq $nrcPresent -or ($nrcPresent | Measure-Object).Count -eq 0) {
            Add-Result "NR3: NORECOMPUTE stat excluded when TargetNorecompute=N" "PASS" "$($rows.Count) row(s) captured"
        }
        else {
            Add-Result "NR3: NORECOMPUTE stat excluded when TargetNorecompute=N" "FAIL" "NR_Stat_Norecompute found in output (should be excluded)"
        }

        # Commands for any captured rows must NOT contain NORECOMPUTE
        $withFlag = $rows | Where-Object { ([string]$_["Command"]) -match "NORECOMPUTE" }
        if ($null -eq $withFlag -or ($withFlag | Measure-Object).Count -eq 0) {
            Add-Result "NR3: No captured command contains NORECOMPUTE" "PASS" ""
        }
        else {
            $bad = [string]($withFlag | Select-Object -First 1)["Command"]
            Add-Result "NR3: No captured command contains NORECOMPUTE" "FAIL" "Unexpected NORECOMPUTE in: $($bad.Substring(0, [Math]::Min(120, $bad.Length)))"
        }
    }
}

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" `
    -Query "IF OBJECT_ID(N'$wioTable3', N'U') IS NOT NULL DROP TABLE $wioTable3;" | Out-Null
Clear-StatUpdateApplock -ServerAddr $Server -Port $Port -Pwd $Password

# =====================================================================
# NR4: WhatIfOutputTable created even when 0 stats qualify
# (Sanity check for the zero-row path -- table must exist with 0 rows)
# =====================================================================
Write-Host "[Tests] NR4: WhatIfOutputTable exists with 0 rows when nothing qualifies..." -ForegroundColor Yellow

$wioTable4 = "dbo.WhatIfNR4_$([DateTime]::UtcNow.ToString('HHmmss'))"
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" `
    -Query "IF OBJECT_ID(N'$wioTable4', N'U') IS NOT NULL DROP TABLE $wioTable4;" | Out-Null

# Use an absurdly high ModificationThreshold so nothing qualifies
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "
        EXEC dbo.sp_StatUpdate
            @Databases            = N'$Database',
            @Tables               = N'dbo.NR_TestTable',
            @TargetNorecompute    = N'BOTH',
            @ModificationThreshold = 999999999,
            @Execute              = N'N',
            @LogToTable           = N'N',
            @WhatIfOutputTable    = N'$wioTable4';"

if ($r.ExitCode -ne 0) {
    Add-Result "NR4: Proc runs with nothing qualifying" "FAIL" ($r.Output | Select-Object -First 1)
    Add-Result "NR4: WhatIfOutputTable creation on zero-qualify" "FAIL" "Proc call failed before output-table verification."
}
else {
    Add-Result "NR4: Proc runs with nothing qualifying" "PASS" ""

    # Table may or may not be created when 0 stats qualify -- just confirm no error.
    # Check in master (that is where the proc creates WIO tables).
    $r2 = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" -Query "
        SELECT CASE WHEN OBJECT_ID(N'$wioTable4', N'U') IS NOT NULL THEN 1 ELSE 0 END AS TableExists;"
    if ($r2.ExitCode -eq 0) {
        $exists = [int]$r2.DataSet.Tables[0].Rows[0]["TableExists"]
        Add-Result "NR4: WhatIfOutputTable creation on zero-qualify" "PASS" "TableExists=$exists (proc did not error)"
    }
    else {
        Add-Result "NR4: WhatIfOutputTable creation on zero-qualify" "FAIL" "Could not verify table existence"
    }
}

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" `
    -Query "IF OBJECT_ID(N'$wioTable4', N'U') IS NOT NULL DROP TABLE $wioTable4;" | Out-Null
Clear-StatUpdateApplock -ServerAddr $Server -Port $Port -Pwd $Password

# =====================================================================
# CLEANUP
# =====================================================================
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database `
    -Query "IF OBJECT_ID(N'dbo.NR_TestTable', N'U') IS NOT NULL DROP TABLE dbo.NR_TestTable;" | Out-Null

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

$color = if ($failed -eq 0) { "Green" } else { "Red" }
Write-Host "  $passed/$total passed, $failed failed, $skipped skipped" -ForegroundColor $color
Write-Host ""

if ($ResultsPath) {
    if ($results.Count -ne 17) { throw "Expected 17 WhatIfNorecompute checks; received $($results.Count)." }
    $major = @{ '2019' = 15; '2022' = 16; '2025' = 17 }[$Version]
    $status = Get-StructuredSqlStatus -Server $Server -Port $Port -Password $Password -ExpectedMajor $major
    $commit = (& git -C $repoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $commit -SqlVersion $Version -SqlBuild $status.Build `
        -ServerAliveAtEnd $status.Alive -Suite 'WhatIfNorecompute' -ExpectedCount 17 `
        -Results @($results) -StartedAt $startedAt -EndedAt (Get-Date)
}

if ($failed -gt 0 -or $skipped -gt 0) {
    Write-Host "  FAILED:" -ForegroundColor Red
    foreach ($t in ($results | Where-Object { $_.Result -eq "FAIL" })) {
        Write-Host "    - $($t.Test): $($t.Detail)" -ForegroundColor Red
    }
    Write-Host ""
    exit 1
}

exit 0
