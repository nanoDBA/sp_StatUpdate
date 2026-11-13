<#
.SYNOPSIS
    Assertion coverage for diag checks W8-W10, C5, I11-I14 (H5, powz).

.DESCRIPTION
    Seeds minimal CommandLog data that forces each new check to fire, then
    runs sp_StatUpdate_Diag and asserts the expected Category appears in RS2
    (Recommendations).  Uses @SkipHistory=1 and @ExpertMode=1 throughout
    so only test data drives results.

    Tests:
    - D1: W8  MOPUP_INEFFECTIVE      -- 3+ mop-up runs all finding 0 stats
    - D2: W9  LOCK_TIMEOUT_INEFFECTIVE -- same stat with ErrorNumber=1222 in 3+ runs
    - D3: W10 PARAMETER_CHURN        -- 5+ runs with 3+ distinct TimeLimit values
                                         + 2+ distinct QueryStorePriority values
    - D4: C5  SAMPLE_RATE_DEGRADATION -- stat going from 80%->20% avg sample rate
    - D5: I11 FAILURE_CLUSTERING     -- dominant error code covering 80%+ of failures
    - D6: I12 QS_COVERAGE_DRIFT      -- QS plan count dropping >50% week-over-week
    - D7: I13 PARALLEL_OPPORTUNITY   -- 3+ serial COMPLETED runs using <40% of TimeLimit
    - D8: I14 MOPUP_MISSING_PAGECOUNT -- mop-up stats with NULL PageCount

.PARAMETER Server
    SQL Server instance.  Default: 127.0.0.1.

.PARAMETER Port
    Port.  Default: 1432 (SQL 2022).

.PARAMETER Password
    SA password.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Database
    Database where CommandLog lives.  Default: master.

.EXAMPLE
    .\tests\diag\Test-DiagChecksCoverage.ps1
    .\tests\diag\Test-DiagChecksCoverage.ps1 -Port 1434
#>
[CmdletBinding()]
param(
    [string]$Server   = "127.0.0.1",
    [int]$Port        = 1432,
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { "$env:SPSTATUPDATE_TEST_SQL_PASSWORD" }),
    [string]$Database = "master",
    [string]$ResultsPath,
    [ValidateSet('2019','2022','2025')][string]$SqlVersion,
    [string]$Commit
)

$ErrorActionPreference = "Continue"
$suiteStartedAt = [DateTime]::UtcNow

# =====================================================================
# PATHS
# =====================================================================
$scriptRoot = $PSScriptRoot
$repoRoot   = (Split-Path -Parent (Split-Path -Parent $scriptRoot))
$diagProc   = Join-Path $repoRoot "sp_StatUpdate_Diag.sql"

# =====================================================================
# ADO.NET HELPER  (ExecuteReader -- works on SQL 2025)
# =====================================================================
function Invoke-Sql {
    param(
        [string]$ServerAddr,
        [int]$Port,
        [string]$Pwd,
        [string]$Query,
        [string]$Db      = "master",
        [int]$Timeout    = 120
    )
    $connStr = "Server=$ServerAddr,$Port;Database=$Db;User Id=sa;Password=$Pwd;Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
    $messages = [System.Collections.ArrayList]::new()
    try {
        $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
        $conn.add_InfoMessage({ param($sender, $e); $null = $messages.Add($e.Message) })
        $conn.FireInfoMessageEventOnUserErrors = $true
        $conn.Open()
        $cmd  = $conn.CreateCommand()
        $cmd.CommandText    = $Query
        $cmd.CommandTimeout = $Timeout
        $ds     = [System.Data.DataSet]::new()
        $reader = $cmd.ExecuteReader()
        do {
            $dt = [System.Data.DataTable]::new()
            $dt.Load($reader)
            $null = $ds.Tables.Add($dt)
        } while (-not $reader.IsClosed)
        $reader.Dispose()
        $conn.Close()
        return @{ Output = $messages; DataSet = $ds; ExitCode = 0 }
    }
    catch {
        return @{ Output = @($_.Exception.InnerException.Message, $_.Exception.Message) + $messages; DataSet = $null; ExitCode = 1 }
    }
}

function Invoke-SqlFile {
    param([string]$FilePath)
    $content = Get-Content $FilePath -Raw
    $batches = $content -split "(?m)^\s*GO\s*$"
    $connStr = "Server=$Server,$Port;Database=$Database;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
    $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
    $conn.Open()
    try {
        foreach ($batch in $batches) {
            $trimmed = $batch.Trim()
            if ($trimmed.Length -eq 0) { continue }
            $cmd = $conn.CreateCommand()
            $cmd.CommandText    = $trimmed
            $cmd.CommandTimeout = 60
            $cmd.ExecuteNonQuery() | Out-Null
        }
    }
    finally { $conn.Close() }
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
Write-Host " Diag Check Coverage Tests: W8-W10, C5, I11-I14 (H5, powz)" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:   ${Server}:${Port}"
Write-Host "  Database: $Database"
Write-Host ""

# =====================================================================
# CONNECTIVITY + DEPLOY PROC
# =====================================================================
Write-Host "[Setup] Verifying connectivity..." -ForegroundColor Yellow
$vr = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "SELECT CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS MajorVer, CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')) AS ProductVersion;"
if ($vr.ExitCode -ne 0) {
    Write-Host "  ERROR: Cannot connect to ${Server}:${Port}" -ForegroundColor Red
    exit 1
}
$serverMajor = [int]$vr.DataSet.Tables[0].Rows[0]["MajorVer"]
$serverBuild = [string]$vr.DataSet.Tables[0].Rows[0]["ProductVersion"]
Write-Host "  Connected (SQL $serverMajor)" -ForegroundColor Green

Write-Host "  Deploying sp_StatUpdate_Diag..." -NoNewline
try {
    Invoke-SqlFile -FilePath $diagProc
    Write-Host " OK" -ForegroundColor Green
}
catch {
    Write-Host " FAILED: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
Write-Host ""

# =====================================================================
# SHARED HELPER: Run diag and return the recommendations table (RS2)
# =====================================================================
function Get-Recommendations {
    param([int]$TimeoutSec = 60, [int]$DaysBack = 60)
    # Access script-level params explicitly to avoid PowerShell 5.1 function scope issues
    $srv = $script:Server
    $prt = $script:Port
    $pwd = $script:Password
    $db  = $script:Database
    $connStr = "Server=$srv,$prt;Database=$db;User Id=sa;Password=$pwd;Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
    try {
        $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
        $conn.Open()
        $cmd = $conn.CreateCommand()
        $cmd.CommandText = "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack=$DaysBack, @ExpertMode=1, @SkipHistory=1;"
        $cmd.CommandTimeout = $TimeoutSec
        $reader = $cmd.ExecuteReader()
        $tables = [System.Collections.ArrayList]::new()
        do {
            $dt = [System.Data.DataTable]::new()
            $null = $dt.Load($reader)
            $null = $tables.Add($dt)
        } while (-not $reader.IsClosed)
        $reader.Dispose()
        $conn.Close()
        $found = $null
        for ($ti = 0; $ti -lt $tables.Count; $ti++) {
            $t = $tables[$ti]
            $hasC = $t.Columns.Contains("Category")
            $hasS = $t.Columns.Contains("Severity")
            $hasF = $t.Columns.Contains("Finding")
            if ($hasC -and $hasS -and $hasF) { $found = $t; break }
        }
        # Use comma operator (,) to return the DataTable as-is without PowerShell
        # unrolling it into individual DataRows (DataTable implements IEnumerable).
        return ,$found
    }
    catch {
        Write-Host "    WARN: Get-Recommendations error: $($_.Exception.Message)" -ForegroundColor Yellow
        return $null
    }
}

function Has-Category {
    param($Table, [string]$Category)
    if ($null -eq $Table) { return $false }
    for ($i = 0; $i -lt $Table.Rows.Count; $i++) {
        $row = $Table.Rows[$i]
        if ($row -and $row.ItemArray.Count -gt 0) {
            $catIdx = $Table.Columns.IndexOf("Category")
            if ($catIdx -ge 0 -and $row.ItemArray[$catIdx] -ne [System.DBNull]::Value -and
                $row.ItemArray[$catIdx].ToString() -eq $Category) {
                return $true
            }
        }
    }
    return $false
}

function Get-Categories {
    param($Table)
    if ($null -eq $Table) { return "(no recs)" }
    if (-not $Table.Columns.Contains("Category")) { return "(no Category column)" }
    $cats = [System.Collections.ArrayList]::new()
    for ($i = 0; $i -lt $Table.Rows.Count; $i++) {
        try {
            $v = $Table.Rows[$i]["Category"]
            if ($v -and $v -ne [System.DBNull]::Value) { $null = $cats.Add($v.ToString()) }
        } catch { }
    }
    if ($cats.Count -eq 0) { return "(empty recommendations)" }
    return ($cats -join ",")
}

# =====================================================================
# ENSURE CommandLog TABLE EXISTS AND IS CLEAN
# These tests are self-contained: they truncate CommandLog so that
# accumulated state from Setup-DiagTestData.sql or prior test runs
# does not cause PK violations inside sp_StatUpdate_Diag (#runs).
# Run only on test/dev servers.
# =====================================================================
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 30 -Query "
    IF OBJECT_ID(N'dbo.CommandLog') IS NULL
    BEGIN
        CREATE TABLE dbo.CommandLog (
            ID            int IDENTITY(1,1) NOT NULL PRIMARY KEY,
            DatabaseName  sysname NULL,
            SchemaName    sysname NULL,
            ObjectName    sysname NULL,
            ObjectType    char(2) NULL,
            IndexName     sysname NULL,
            IndexType     tinyint NULL,
            StatisticsName sysname NULL,
            PartitionNumber int NULL,
            ExtendedInfo  xml NULL,
            Command       nvarchar(max) NOT NULL,
            CommandType   nvarchar(60)  NOT NULL,
            StartTime     datetime2(7)  NOT NULL,
            EndTime       datetime2(7)  NULL,
            ErrorNumber   int NULL,
            ErrorMessage  nvarchar(max) NULL
        );
    END;
    TRUNCATE TABLE dbo.CommandLog;
" | Out-Null
Invoke-SqlFile -FilePath (Join-Path $scriptRoot 'CommandLogContractFixture.sql')

# =====================================================================
# HELPERS: Insert a START/END pair and UPDATE_STATISTICS rows
# =====================================================================

# Build a minimal SP_STATUPDATE_START/END pair
# $RunLabel, $StartOffset (negative days from now), $DurationSec, $StopReason,
# $TimeLimit, $ModThreshold, $SortOrder, $QSPriority, $Parallel,
# $MopUpTriggered, $MopUpFound, $MopUpProcessed, $TieredThresholds
function Insert-Run {
    param(
        [string]$RunLabel,
        [int]$StartOffsetDays   = -1,
        [int]$StartOffsetHours  = 0,
        [int]$DurationSec       = 300,
        [string]$StopReason     = "COMPLETED",
        [int]$TimeLimit         = 3600,
        [int]$ModThreshold      = 5000,
        [string]$SortOrder      = "MODIFICATION_COUNTER",
        [string]$QSPriority     = "N",
        [string]$Parallel       = "N",
        [int]$MopUpTriggered    = 0,
        [int]$MopUpFound        = 0,
        [int]$MopUpProcessed    = 0,
        [int]$TieredThresholds  = 1,
        [int]$StatsFound        = 100,
        [int]$StatsProcessed    = 100,
        [int]$StatsSucceeded    = 100,
        [int]$StatsFailed       = 0,
        [int]$StatsRemaining    = 0
    )
    $sql = "
        DECLARE @s datetime2(3) = DATEADD(HOUR, $StartOffsetHours, DATEADD(DAY, $StartOffsetDays, GETDATE()));
        DECLARE @e datetime2(3) = DATEADD(SECOND, $DurationSec, @s);
        INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
        VALUES (
            N'SP_STATUPDATE_START',
            N'EXECUTE dbo.sp_StatUpdate',
            @s, @s,
            N'<Parameters>
                <Version>2.29.2026.03.24</Version>
                <RunLabel>$RunLabel</RunLabel>
                <Databases>USER_DATABASES</Databases>
                <TimeLimit>$TimeLimit</TimeLimit>
                <ModificationThreshold>$ModThreshold</ModificationThreshold>
                <TieredThresholds>$TieredThresholds</TieredThresholds>
                <ThresholdLogic>OR</ThresholdLogic>
                <SortOrder>$SortOrder</SortOrder>
                <QueryStorePriority>$QSPriority</QueryStorePriority>
                <StatsInParallel>$Parallel</StatsInParallel>
            </Parameters>'
        );
        INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
        VALUES (
            N'SP_STATUPDATE_END',
            N'EXECUTE dbo.sp_StatUpdate',
            @s, @e,
            N'<Summary>
                <Version>2.29.2026.03.24</Version>
                <RunLabel>$RunLabel</RunLabel>
                <StatsFound>$StatsFound</StatsFound>
                <StatsProcessed>$StatsProcessed</StatsProcessed>
                <StatsSucceeded>$StatsSucceeded</StatsSucceeded>
                <StatsFailed>$StatsFailed</StatsFailed>
                <StatsRemaining>$StatsRemaining</StatsRemaining>
                <DurationSeconds>$DurationSec</DurationSeconds>
                <StopReason>$StopReason</StopReason>
                <MopUpTriggered>$MopUpTriggered</MopUpTriggered>
                <MopUpFound>$MopUpFound</MopUpFound>
                <MopUpProcessed>$MopUpProcessed</MopUpProcessed>
            </Summary>'
        );
    "
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 30 -Query $sql | Out-Null
}

# Insert a single UPDATE_STATISTICS row
function Insert-StatUpdate {
    param(
        [string]$RunLabel,
        [int]$StartOffsetDays  = -1,
        [string]$DbName        = "TestDB",
        [string]$SchName       = "dbo",
        [string]$ObjName       = "Orders",
        [string]$StatName      = "IX_Orders_Date",
        [int]$ErrorNumber      = 0,
        [string]$ErrorMessage  = "",
        [int]$SamplePct        = 25,
        [int]$QSPlanCount      = 5,
        [int]$PageCount        = 1000,
        [int]$DurationMs       = 200
    )
    $errMsgXml = if ($ErrorMessage) { $ErrorMessage } else { "" }
    $sql = "
        DECLARE @s datetime2(3) = DATEADD(MILLISECOND, $DurationMs * -1,
                                           DATEADD(DAY, $StartOffsetDays, GETDATE()));
        DECLARE @e datetime2(3) = DATEADD(MILLISECOND, $DurationMs, @s);
        INSERT INTO dbo.CommandLog (
            CommandType, Command, StartTime, EndTime,
            DatabaseName, SchemaName, ObjectName, StatisticsName,
            ErrorNumber, ErrorMessage, ExtendedInfo
        ) VALUES (
            N'UPDATE_STATISTICS',
            N'UPDATE STATISTICS [$DbName].[$SchName].[$ObjName] ([$StatName])',
            @s, @e,
            N'$DbName', N'$SchName', N'$ObjName', N'$StatName',
            $(if ($ErrorNumber -eq 0) { 'NULL' } else { $ErrorNumber }),
            $(if ($ErrorMessage) { "N'$errMsgXml'" } else { 'NULL' }),
            N'<ExtendedInfo>
                <RunLabel>$RunLabel</RunLabel>
                <RowCount>100000</RowCount>
                <ModificationCounter>6000</ModificationCounter>
                <PageCount>$PageCount</PageCount>
                <SizeMB>8</SizeMB>
                <DaysStale>2</DaysStale>
                <HasNorecompute>0</HasNorecompute>
                <IsHeap>0</IsHeap>
                <QSPlanCount>$QSPlanCount</QSPlanCount>
                <QSTotalCpuMs>50000</QSTotalCpuMs>
                <EffectiveSamplePct>$SamplePct</EffectiveSamplePct>
                <ProcessingPosition>1</ProcessingPosition>
            </ExtendedInfo>'
        );
    "
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 30 -Query $sql | Out-Null
}

# =====================================================================
# D7: I13 PARALLEL_OPPORTUNITY
# Trigger: >=3 serial COMPLETED runs using avg <40% of TimeLimit,
#          AND no OVERLAPPING_RUNS warning.
# Seed 4 serial COMPLETED runs each finishing in 600s with TimeLimit=3600
# (600/3600 = 16.7% usage -- well under 40%).
# Run this against the clean table before other synthetic scenarios exist.
# =====================================================================
Write-Host "[D7] I13 PARALLEL_OPPORTUNITY..." -ForegroundColor Yellow
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
    DELETE FROM dbo.CommandLog WHERE ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') LIKE N'i13_test_%'
    OR ExtendedInfo.value('(/Summary/RunLabel)[1]','nvarchar(100)') LIKE N'i13_test_%';
" | Out-Null
for ($i = 1; $i -le 4; $i++) {
    # Space runs 5h apart -- no overlap possible
    Insert-Run -RunLabel "i13_test_$i" -StartOffsetDays -20 -StartOffsetHours (-5 * $i) `
        -DurationSec 600 -StopReason "COMPLETED" -TimeLimit 3600 `
        -Parallel "N" -StatsProcessed 20 -StatsSucceeded 20
}
$recs = Get-Recommendations -DaysBack 30
if (Has-Category $recs "PARALLEL_OPPORTUNITY") {
    Add-Result "D7: I13 PARALLEL_OPPORTUNITY detected" "PASS" ""
}
else {
    # I13 is suppressed when OVERLAPPING_RUNS (W4) fires
    $hasW4 = Has-Category $recs "OVERLAPPING_RUNS"
    $cats = Get-Categories $recs
    if ($hasW4) {
        Add-Result "D7: I13 PARALLEL_OPPORTUNITY detected" "FAIL" "W4 OVERLAPPING_RUNS suppresses I13 inside the isolated 24-hour seed window"
    }
    else {
        Add-Result "D7: I13 PARALLEL_OPPORTUNITY detected" "FAIL" "Categories found: $cats"
    }
}
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
    DELETE FROM dbo.CommandLog WHERE ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') LIKE N'i13_test_%'
    OR ExtendedInfo.value('(/Summary/RunLabel)[1]','nvarchar(100)') LIKE N'i13_test_%';
" | Out-Null


# =====================================================================
# D1: W8 MOPUP_INEFFECTIVE
# Trigger: >= 3 non-killed mop-up runs, >=80% of them found 0 stats.
# Seed: 3 runs with MopUpTriggered=1, MopUpFound=0.
# =====================================================================
Write-Host "[D1] W8 MOPUP_INEFFECTIVE..." -ForegroundColor Yellow
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
    DELETE FROM dbo.CommandLog WHERE ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') LIKE N'w8_test_%'
    OR ExtendedInfo.value('(/Summary/RunLabel)[1]','nvarchar(100)') LIKE N'w8_test_%';
" | Out-Null
for ($i = 1; $i -le 3; $i++) {
    Insert-Run -RunLabel "w8_test_$i" -StartOffsetDays (-$i * 2) `
        -MopUpTriggered 1 -MopUpFound 0 -MopUpProcessed 0 `
        -StopReason "COMPLETED" -TimeLimit 3600 -SortOrder "MODIFICATION_COUNTER"
}
$recs = Get-Recommendations
if (Has-Category $recs "MOPUP_INEFFECTIVE") {
    Add-Result "D1: W8 MOPUP_INEFFECTIVE detected" "PASS" ""
}
else {
    $cats = Get-Categories $recs
    Add-Result "D1: W8 MOPUP_INEFFECTIVE detected" "FAIL" "Categories found: $cats"
}

# =====================================================================
# D2: W9 LOCK_TIMEOUT_INEFFECTIVE
# Trigger: same stat has ErrorNumber=1222 in 3+ distinct runs.
# Seed: 3 runs each with a failed stat having ErrorNumber=1222.
# =====================================================================
Write-Host "[D2] W9 LOCK_TIMEOUT_INEFFECTIVE..." -ForegroundColor Yellow
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
    DELETE FROM dbo.CommandLog WHERE ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') LIKE N'w9_test_%'
    OR ExtendedInfo.value('(/Summary/RunLabel)[1]','nvarchar(100)') LIKE N'w9_test_%'
    OR ExtendedInfo.value('(/ExtendedInfo/RunLabel)[1]','nvarchar(100)') LIKE N'w9_test_%';
" | Out-Null
for ($i = 1; $i -le 3; $i++) {
    Insert-Run -RunLabel "w9_test_$i" -StartOffsetDays (-$i * 2) `
        -StatsFailed 1 -StatsSucceeded 99 -StopReason "COMPLETED" -TimeLimit 3600
    # Same stat failing with lock timeout in each run
    Insert-StatUpdate -RunLabel "w9_test_$i" -StartOffsetDays (-$i * 2) `
        -ObjName "LockVictimTable" -StatName "IX_LockVictim" `
        -ErrorNumber 1222 -ErrorMessage "Lock request time out period exceeded."
}
$recs = Get-Recommendations
if (Has-Category $recs "LOCK_TIMEOUT_INEFFECTIVE") {
    Add-Result "D2: W9 LOCK_TIMEOUT_INEFFECTIVE detected" "PASS" ""
}
else {
    $cats = Get-Categories $recs
    Add-Result "D2: W9 LOCK_TIMEOUT_INEFFECTIVE detected" "FAIL" "Categories found: $cats"
}

# =====================================================================
# D3: W10 PARAMETER_CHURN
# Trigger: >=5 runs (IsKilled=0), >=2 of the churn conditions:
#   @w10_tl_vals >= 3 (3+ distinct TimeLimit values)
#   @w10_so_vals >= 3 (3+ distinct SortOrder values)
# Seed: 6 runs with 3 different TimeLimit values and 3 different SortOrders.
# =====================================================================
Write-Host "[D3] W10 PARAMETER_CHURN..." -ForegroundColor Yellow
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
    DELETE FROM dbo.CommandLog WHERE ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') LIKE N'w10_test_%'
    OR ExtendedInfo.value('(/Summary/RunLabel)[1]','nvarchar(100)') LIKE N'w10_test_%';
" | Out-Null
$tlVals    = @(1800, 3600, 7200, 1800, 3600, 7200)
$soVals    = @("MODIFICATION_COUNTER","DAYS_STALE","ROWS","MODIFICATION_COUNTER","DAYS_STALE","ROWS")
for ($i = 0; $i -lt 6; $i++) {
    Insert-Run -RunLabel "w10_test_$($i+1)" -StartOffsetDays (-($i+1)*2) `
        -TimeLimit $tlVals[$i] -SortOrder $soVals[$i] -StopReason "COMPLETED"
}
$recs = Get-Recommendations
if (Has-Category $recs "PARAMETER_CHURN") {
    Add-Result "D3: W10 PARAMETER_CHURN detected" "PASS" ""
}
else {
    $cats = Get-Categories $recs
    Add-Result "D3: W10 PARAMETER_CHURN detected" "FAIL" "Categories found: $cats"
}

# =====================================================================
# D4: C5 SAMPLE_RATE_DEGRADATION
# Trigger: >=4 appearances of same stat; avg_recent < avg_prior * 0.5.
# Split: recent half uses 10%, prior half uses 80%.
# Use DaysBack=60 -- 4 runs spanning 60 days.
# Spread: 2 runs at -50/-45 days (prior half of @DaysBack=60 window)
#         2 runs at -5/-2 days  (recent half)
# =====================================================================
Write-Host "[D4] C5 SAMPLE_RATE_DEGRADATION..." -ForegroundColor Yellow
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
    DELETE FROM dbo.CommandLog WHERE ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') LIKE N'c5_test_%'
    OR ExtendedInfo.value('(/Summary/RunLabel)[1]','nvarchar(100)') LIKE N'c5_test_%'
    OR ExtendedInfo.value('(/ExtendedInfo/RunLabel)[1]','nvarchar(100)') LIKE N'c5_test_%';
" | Out-Null
# Prior runs (>30 days ago): high sample
$priorDays = @(-50, -45)
foreach ($d in $priorDays) {
    $lbl = "c5_test_prior$d"
    Insert-Run -RunLabel $lbl -StartOffsetDays $d -StopReason "COMPLETED" -TimeLimit 3600
    Insert-StatUpdate -RunLabel $lbl -StartOffsetDays $d `
        -ObjName "SampleDriftTable" -StatName "IX_SampleDrift" -SamplePct 80
}
# Recent runs (<30 days ago): low sample
$recentDays = @(-5, -2)
foreach ($d in $recentDays) {
    $lbl = "c5_test_recent$d"
    Insert-Run -RunLabel $lbl -StartOffsetDays $d -StopReason "COMPLETED" -TimeLimit 3600
    Insert-StatUpdate -RunLabel $lbl -StartOffsetDays $d `
        -ObjName "SampleDriftTable" -StatName "IX_SampleDrift" -SamplePct 10
}
$recs = Get-Recommendations
if (Has-Category $recs "SAMPLE_RATE_DEGRADATION") {
    Add-Result "D4: C5 SAMPLE_RATE_DEGRADATION detected" "PASS" ""
}
else {
    $cats = Get-Categories $recs
    Add-Result "D4: C5 SAMPLE_RATE_DEGRADATION detected" "FAIL" "Categories found: $cats"
}

# =====================================================================
# D5: I11 FAILURE_CLUSTERING
# Trigger: C2 (REPEATED_FAILURES) fires AND a dominant error code covers
#          >= 80% of failures in #stat_updates.
# Strategy: seed 3+ runs with the same stat failing (triggers C2), all
#           with the same ErrorNumber (triggers I11 dominant cluster).
# We reuse the w9 seed (lock timeout) which already creates C2 territory,
#   but those are scoped to a stat that doesn't necessarily overlap
#   with what C2 looks for (3+ runs for same stat).
# Seed dedicated c2+i11 data: same stat failing across 3 runs with
#   ErrorNumber=262 (permission denied) which won't conflict with lock
#   timeout stat used for W9.
# =====================================================================
Write-Host "[D5] I11 FAILURE_CLUSTERING..." -ForegroundColor Yellow
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
    DELETE FROM dbo.CommandLog WHERE ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') LIKE N'i11_test_%'
    OR ExtendedInfo.value('(/Summary/RunLabel)[1]','nvarchar(100)') LIKE N'i11_test_%'
    OR ExtendedInfo.value('(/ExtendedInfo/RunLabel)[1]','nvarchar(100)') LIKE N'i11_test_%';
" | Out-Null
for ($i = 1; $i -le 4; $i++) {
    Insert-Run -RunLabel "i11_test_$i" -StartOffsetDays (-$i * 2) `
        -StatsFailed 5 -StatsSucceeded 95 -StopReason "COMPLETED" -TimeLimit 3600
    # 4 stats failing with same error in each run (dominates failures)
    for ($j = 1; $j -le 4; $j++) {
        Insert-StatUpdate -RunLabel "i11_test_$i" -StartOffsetDays (-$i * 2) `
            -ObjName "ClusteredFailTable_$j" -StatName "IX_ClustFail_$j" `
            -ErrorNumber 262 -ErrorMessage "UPDATE STATISTICS permission denied."
    }
    # 1 stat failing with a different error (minority)
    Insert-StatUpdate -RunLabel "i11_test_$i" -StartOffsetDays (-$i * 2) `
        -ObjName "ClusteredFailOther" -StatName "IX_OtherFail" `
        -ErrorNumber 1205 -ErrorMessage "Deadlock."
}
$recs = Get-Recommendations
if (Has-Category $recs "FAILURE_CLUSTERING") {
    Add-Result "D5: I11 FAILURE_CLUSTERING detected" "PASS" ""
}
else {
    # I11 only fires when C2 is also active; check both
    $hasC2 = Has-Category $recs "REPEATED_FAILURES"
    $cats = Get-Categories $recs
    if (-not $hasC2) {
        Add-Result "D5: I11 FAILURE_CLUSTERING detected" "FAIL" "C2 (REPEATED_FAILURES) did not fire either.  Cats: $cats"
    }
    else {
        Add-Result "D5: I11 FAILURE_CLUSTERING detected" "FAIL" "C2 fired but I11 did not.  Cats: $cats"
    }
}

# =====================================================================
# D6: I12 QS_COVERAGE_DRIFT
# Trigger: avg QSPlanCount in recent half < 50% of prior half,
#          both halves must be non-NULL and runs have QueryStorePriority=Y.
# Prior half: QSPlanCount=10 per stat; recent half: QSPlanCount=1.
# =====================================================================
Write-Host "[D6] I12 QS_COVERAGE_DRIFT..." -ForegroundColor Yellow
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
    DELETE FROM dbo.CommandLog WHERE ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') LIKE N'i12_test_%'
    OR ExtendedInfo.value('(/Summary/RunLabel)[1]','nvarchar(100)') LIKE N'i12_test_%'
    OR ExtendedInfo.value('(/ExtendedInfo/RunLabel)[1]','nvarchar(100)') LIKE N'i12_test_%';
" | Out-Null
# Prior (> 30 days): high QS plan count
foreach ($d in @(-50, -45, -40)) {
    $lbl = "i12_test_prior$($d)"
    Insert-Run -RunLabel $lbl -StartOffsetDays $d -QSPriority "Y" `
        -SortOrder "QUERY_STORE" -StopReason "COMPLETED" -TimeLimit 3600
    Insert-StatUpdate -RunLabel $lbl -StartOffsetDays $d `
        -ObjName "QSDriftTable" -StatName "IX_QSDrift" -QSPlanCount 10
}
# Recent (< 30 days): very low QS plan count
foreach ($d in @(-10, -5, -2)) {
    $lbl = "i12_test_recent$($d)"
    Insert-Run -RunLabel $lbl -StartOffsetDays $d -QSPriority "Y" `
        -SortOrder "QUERY_STORE" -StopReason "COMPLETED" -TimeLimit 3600
    Insert-StatUpdate -RunLabel $lbl -StartOffsetDays $d `
        -ObjName "QSDriftTable" -StatName "IX_QSDrift" -QSPlanCount 1
}
$recs = Get-Recommendations
if (Has-Category $recs "QS_COVERAGE_DRIFT") {
    Add-Result "D6: I12 QS_COVERAGE_DRIFT detected" "PASS" ""
}
else {
    $cats = Get-Categories $recs
    Add-Result "D6: I12 QS_COVERAGE_DRIFT detected" "FAIL" "Categories found: $cats"
}

# =====================================================================
# D8: I14 MOPUP_MISSING_PAGECOUNT
# Trigger: mop-up run exists (MopUpTriggered=1, IsKilled=0) and >=20%
#          of UPDATE_STATISTICS rows in that run have NULL PageCount.
# Seed: 1 mop-up run with 4 stat updates: 3 have NULL PageCount.
# =====================================================================
Write-Host "[D8] I14 MOPUP_MISSING_PAGECOUNT..." -ForegroundColor Yellow
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
    DELETE FROM dbo.CommandLog WHERE ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') LIKE N'i14_test_%'
    OR ExtendedInfo.value('(/Summary/RunLabel)[1]','nvarchar(100)') LIKE N'i14_test_%'
    OR ExtendedInfo.value('(/ExtendedInfo/RunLabel)[1]','nvarchar(100)') LIKE N'i14_test_%';
" | Out-Null
Insert-Run -RunLabel "i14_test_1" -StartOffsetDays -3 `
    -MopUpTriggered 1 -MopUpFound 4 -MopUpProcessed 4 `
    -StopReason "COMPLETED" -TimeLimit 3600

# 3 stats with NULL PageCount (no <PageCount> tag in ExtendedInfo)
$nullPageSql = "
    DECLARE @s datetime2(3) = DATEADD(DAY, -3, GETDATE());
    DECLARE @e datetime2(3) = DATEADD(SECOND, 5, @s);
"
for ($k = 1; $k -le 3; $k++) {
    $nullStatSql = "
        DECLARE @s datetime2(3) = DATEADD(SECOND, $($k * 10), DATEADD(DAY, -3, GETDATE()));
        DECLARE @e datetime2(3) = DATEADD(SECOND, 2, @s);
        INSERT INTO dbo.CommandLog (
            CommandType, Command, StartTime, EndTime,
            DatabaseName, SchemaName, ObjectName, StatisticsName,
            ErrorNumber, ErrorMessage, ExtendedInfo
        ) VALUES (
            N'UPDATE_STATISTICS',
            N'UPDATE STATISTICS [TestDB].[dbo].[MopNullPage$k] ([IX_MopNull$k])',
            @s, @e,
            N'TestDB', N'dbo', N'MopNullPage$k', N'IX_MopNull$k',
            NULL, NULL,
            N'<ExtendedInfo>
                <RunLabel>i14_test_1</RunLabel>
                <RowCount>50000</RowCount>
                <ModificationCounter>6000</ModificationCounter>
                <!-- PageCount intentionally omitted to simulate mop-up lazy discovery -->
                <SizeMB>4</SizeMB>
                <EffectiveSamplePct>25</EffectiveSamplePct>
                <ProcessingPosition>$k</ProcessingPosition>
            </ExtendedInfo>'
        );
    "
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 30 -Query $nullStatSql | Out-Null
}
# 1 stat with a real PageCount (so it is not 100% NULL)
Insert-StatUpdate -RunLabel "i14_test_1" -StartOffsetDays -3 `
    -ObjName "MopNullPageGood" -StatName "IX_MopGood" -PageCount 2000

$recs = Get-Recommendations
if (Has-Category $recs "MOPUP_MISSING_PAGECOUNT") {
    Add-Result "D8: I14 MOPUP_MISSING_PAGECOUNT detected" "PASS" ""
}
else {
    $cats = Get-Categories $recs
    Add-Result "D8: I14 MOPUP_MISSING_PAGECOUNT detected" "FAIL" "Categories found: $cats"
}

# =====================================================================
# D9: c53 - W3 NO false positive for all-parallel zero-remaining runs
# Scenario: 4 workers using StatsInParallel='Y', all exit TIME_LIMIT,
# aggregate StatsRemaining=0 (queue drained before time expired).
# W3 STALE_BACKLOG must NOT fire.  C3 TIME_LIMIT_EXHAUSTION must NOT fire.
# Each worker reports StatsFound=500, StatsProcessed=125, StatsRemaining=0.
# =====================================================================
Write-Host "[D9] c53 parallel false-positive: W3+C3 suppressed (all-parallel, zero remaining)..." -ForegroundColor Yellow
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
    DELETE FROM dbo.CommandLog WHERE ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') LIKE N'c53_par_%'
    OR ExtendedInfo.value('(/Summary/RunLabel)[1]','nvarchar(100)') LIKE N'c53_par_%';
" | Out-Null
# 4 workers: all StatsInParallel=Y, all TIME_LIMIT, StatsRemaining=0 each
# Workers start 15 seconds apart (same logical batch from a 23:00 job)
for ($w = 1; $w -le 4; $w++) {
    $startSec = ($w - 1) * 15
    $durSec = 1800  # 30 min each
    $sql = "
        DECLARE @s datetime2(3) = DATEADD(SECOND, $startSec, DATEADD(DAY, -1, GETDATE()));
        DECLARE @e datetime2(3) = DATEADD(SECOND, $durSec, @s);
        INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
        VALUES (
            N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @s, @s,
            N'<Parameters><Version>3.2.1.2026.04.17</Version><RunLabel>c53_par_$w</RunLabel>
              <Databases>USER_DATABASES</Databases><TimeLimit>1800</TimeLimit>
              <ModificationThreshold>5000</ModificationThreshold>
              <StatsInParallel>Y</StatsInParallel></Parameters>'
        );
        INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
        VALUES (
            N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @s, @e,
            N'<Summary><Version>3.2.1.2026.04.17</Version><RunLabel>c53_par_$w</RunLabel>
              <StatsFound>500</StatsFound><StatsProcessed>125</StatsProcessed>
              <StatsSucceeded>125</StatsSucceeded><StatsFailed>0</StatsFailed>
              <StatsRemaining>0</StatsRemaining><DurationSeconds>$durSec</DurationSeconds>
              <StopReason>TIME_LIMIT</StopReason><MopUpTriggered>0</MopUpTriggered>
              <MopUpFound>0</MopUpFound><MopUpProcessed>0</MopUpProcessed></Summary>'
        );
    "
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 30 -Query $sql | Out-Null
}
$recs = Get-Recommendations
$hasW3 = Has-Category $recs "STALE_BACKLOG"
$hasC3 = Has-Category $recs "TIME_LIMIT_EXHAUSTION"
if (-not $hasW3) {
    Add-Result "D9a: W3 STALE_BACKLOG not fired for all-parallel zero-remaining TIME_LIMIT runs" "PASS" ""
}
else {
    $cats = Get-Categories $recs
    Add-Result "D9a: W3 STALE_BACKLOG not fired for all-parallel zero-remaining TIME_LIMIT runs" "FAIL" "W3 fired. Categories: $cats"
}
if (-not $hasC3) {
    Add-Result "D9b: C3 TIME_LIMIT_EXHAUSTION not fired for all-parallel zero-remaining TIME_LIMIT runs" "PASS" ""
}
else {
    $cats = Get-Categories $recs
    Add-Result "D9b: C3 TIME_LIMIT_EXHAUSTION not fired for all-parallel zero-remaining TIME_LIMIT runs" "FAIL" "C3 fired. Categories: $cats"
}

# =====================================================================
# D10: c53 - C3 DOES fire for all-parallel runs WITH real remaining work
# Scenario: 4 workers StatsInParallel='Y', all TIME_LIMIT, StatsRemaining>0.
# This is a real backlog -- the window is too short.
# C3 TIME_LIMIT_EXHAUSTION SHOULD fire (not suppressed).
# =====================================================================
Write-Host "[D10] c53 parallel false-positive: C3 fires correctly when remaining>0..." -ForegroundColor Yellow
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
    DELETE FROM dbo.CommandLog WHERE ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') LIKE N'c53_real_%'
    OR ExtendedInfo.value('(/Summary/RunLabel)[1]','nvarchar(100)') LIKE N'c53_real_%';
" | Out-Null
# 4 workers, each with StatsRemaining=80 (genuine unprocessed backlog)
for ($w = 1; $w -le 4; $w++) {
    $startSec = ($w - 1) * 15
    $durSec = 1800
    $sql = "
        DECLARE @s datetime2(3) = DATEADD(SECOND, $startSec, DATEADD(DAY, -2, GETDATE()));
        DECLARE @e datetime2(3) = DATEADD(SECOND, $durSec, @s);
        INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
        VALUES (
            N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @s, @s,
            N'<Parameters><Version>3.2.1.2026.04.17</Version><RunLabel>c53_real_$w</RunLabel>
              <Databases>USER_DATABASES</Databases><TimeLimit>1800</TimeLimit>
              <ModificationThreshold>5000</ModificationThreshold>
              <StatsInParallel>Y</StatsInParallel></Parameters>'
        );
        INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
        VALUES (
            N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @s, @e,
            N'<Summary><Version>3.2.1.2026.04.17</Version><RunLabel>c53_real_$w</RunLabel>
              <StatsFound>500</StatsFound><StatsProcessed>45</StatsProcessed>
              <StatsSucceeded>45</StatsSucceeded><StatsFailed>0</StatsFailed>
              <StatsRemaining>80</StatsRemaining><DurationSeconds>$durSec</DurationSeconds>
              <StopReason>TIME_LIMIT</StopReason><MopUpTriggered>0</MopUpTriggered>
              <MopUpFound>0</MopUpFound><MopUpProcessed>0</MopUpProcessed></Summary>'
        );
    "
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 30 -Query $sql | Out-Null
}
# D10 uses @TimeLimitExhaustionPct=1 to ensure C3 fires regardless of
# the global ratio (accumulated D1-D9 data dilutes the percentage).
# The key assertion: @parallel_aggregate_done is NOT set when StatsRemaining>0,
# so C3 is not suppressed and fires when the threshold is met.
$srv  = $Server; $prt  = $Port; $pwd  = $Password; $db   = $Database
$connStr = "Server=$srv,$prt;Database=$db;User Id=sa;Password=$pwd;Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
$d10recs = $null
try {
    $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
    $conn.Open()
    $cmd = $conn.CreateCommand()
    $cmd.CommandText = "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack=60, @ExpertMode=1, @SkipHistory=1, @TimeLimitExhaustionPct=1;"
    $cmd.CommandTimeout = 60
    $reader = $cmd.ExecuteReader()
    $tables = [System.Collections.ArrayList]::new()
    do {
        $dt = [System.Data.DataTable]::new()
        $null = $dt.Load($reader)
        $null = $tables.Add($dt)
    } while (-not $reader.IsClosed)
    $reader.Dispose(); $conn.Close()
    for ($ti = 0; $ti -lt $tables.Count; $ti++) {
        $t = $tables[$ti]
        if ($t.Columns.Contains("Category") -and $t.Columns.Contains("Severity") -and $t.Columns.Contains("Finding")) {
            $d10recs = $t; break
        }
    }
} catch { Write-Host "  D10 error: $($_.Exception.Message)" -ForegroundColor Yellow }
$hasC3 = Has-Category $d10recs "TIME_LIMIT_EXHAUSTION"
if ($hasC3) {
    Add-Result "D10: C3 TIME_LIMIT_EXHAUSTION fires correctly when parallel workers have real remaining work" "PASS" ""
}
else {
    $cats = Get-Categories $d10recs
    Add-Result "D10: C3 TIME_LIMIT_EXHAUSTION fires correctly when parallel workers have real remaining work" "FAIL" "C3 did not fire. Categories: $cats"
}

# =====================================================================
# D11: c53 - W4 NO false positive for two-schedule parallel runs
# Scenario: runs from 20:15 job (Y) overlap with runs from 23:00 job (Y).
# W4 OVERLAPPING_RUNS must NOT fire (both sides use StatsInParallel='Y').
# =====================================================================
Write-Host "[D11] c53 parallel false-positive: W4 suppressed for two-schedule parallel overlap..." -ForegroundColor Yellow
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
    DELETE FROM dbo.CommandLog WHERE ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') LIKE N'c53_sched_%'
    OR ExtendedInfo.value('(/Summary/RunLabel)[1]','nvarchar(100)') LIKE N'c53_sched_%';
" | Out-Null
# Schedule A: starts at T-8h, runs 2h (ends T-6h). Schedule B: starts at T-7h (overlap with A), runs 2h.
$schedPairs = @(
    @{ lbl="c53_sched_a1"; startSec=-28800; durSec=7200 },  # -8h, 2h duration
    @{ lbl="c53_sched_b1"; startSec=-25200; durSec=7200 }   # -7h, 2h duration -- overlaps A
)
foreach ($sp in $schedPairs) {
    $lbl    = $sp.lbl
    $offset = $sp.startSec
    $dur    = $sp.durSec
    $sql = "
        DECLARE @s datetime2(3) = DATEADD(SECOND, $offset, GETDATE());
        DECLARE @e datetime2(3) = DATEADD(SECOND, $dur, @s);
        INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
        VALUES (
            N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @s, @s,
            N'<Parameters><Version>3.2.1.2026.04.17</Version><RunLabel>$lbl</RunLabel>
              <Databases>USER_DATABASES</Databases><TimeLimit>7200</TimeLimit>
              <ModificationThreshold>5000</ModificationThreshold>
              <StatsInParallel>Y</StatsInParallel></Parameters>'
        );
        INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
        VALUES (
            N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @s, @e,
            N'<Summary><Version>3.2.1.2026.04.17</Version><RunLabel>$lbl</RunLabel>
              <StatsFound>500</StatsFound><StatsProcessed>500</StatsProcessed>
              <StatsSucceeded>500</StatsSucceeded><StatsFailed>0</StatsFailed>
              <StatsRemaining>0</StatsRemaining><DurationSeconds>$dur</DurationSeconds>
              <StopReason>PARALLEL_COMPLETE</StopReason><MopUpTriggered>0</MopUpTriggered>
              <MopUpFound>0</MopUpFound><MopUpProcessed>0</MopUpProcessed></Summary>'
        );
    "
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 30 -Query $sql | Out-Null
}
$recs = Get-Recommendations
$hasW4 = Has-Category $recs "OVERLAPPING_RUNS"
# W4 should not fire because both runs have StatsInParallel='Y'
# Note: other test data may trigger W4 for other serial overlaps; filter to our runs only
$w4ForOurRuns = $false
if ($null -ne $recs) {
    for ($i = 0; $i -lt $recs.Rows.Count; $i++) {
        $row = $recs.Rows[$i]
        $cat = $row["Category"].ToString()
        $fnd = $row["Finding"].ToString()
        if ($cat -eq "OVERLAPPING_RUNS" -and ($fnd -like "*c53_sched*")) {
            $w4ForOurRuns = $true
        }
    }
}
if (-not $w4ForOurRuns) {
    Add-Result "D11: W4 OVERLAPPING_RUNS not fired for intentional two-schedule parallel runs" "PASS" ""
}
else {
    Add-Result "D11: W4 OVERLAPPING_RUNS not fired for intentional two-schedule parallel runs" "FAIL" "W4 fired for c53_sched runs"
}

# =====================================================================
# SUMMARY
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
$pass = ($results | Where-Object { $_.Result -eq "PASS" }).Count
$fail = ($results | Where-Object { $_.Result -eq "FAIL" }).Count
$skip = ($results | Where-Object { $_.Result -eq "SKIP" }).Count
Write-Host " Results: $pass PASS  $fail FAIL  $skip SKIP  (of $($results.Count) tests)" `
    -ForegroundColor $(if ($fail -gt 0) { "Red" } else { "Green" })
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""

if ($ResultsPath) {
    if (-not $SqlVersion -or -not $Commit) { throw 'Structured results require SqlVersion and Commit.' }
    $majorToVersion = @{ 15 = '2019'; 16 = '2022'; 17 = '2025' }
    $actualVersion = $majorToVersion[$serverMajor]
    if ($actualVersion -ne $SqlVersion) { throw "Connected SQL version '$actualVersion' did not match requested '$SqlVersion'." }
    $structuredResults = @($results | ForEach-Object {
        [pscustomobject]@{ Test = $_.Test; Result = $_.Result; Detail = $_.Detail }
    })
    Import-Module (Join-Path $PSScriptRoot '..\StructuredLegacyResults.psm1') -Force
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $Commit -SqlVersion $SqlVersion `
        -SqlBuild $serverBuild -ServerAliveAtEnd $true -Suite 'DiagChecksCoverage' `
        -ExpectedCount $structuredResults.Count -Results $structuredResults `
        -StartedAt $suiteStartedAt -EndedAt ([DateTime]::UtcNow)
}

if ($fail -gt 0) { exit 1 }
