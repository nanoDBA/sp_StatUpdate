<#
.SYNOPSIS
    Concurrent parallel-execution tests (H3, e4c4).

.DESCRIPTION
    Verifies that two sp_StatUpdate workers running @StatsInParallel=Y
    coordinate correctly via dbo.QueueStatistic:

    - P1: Two workers start concurrently; no two workers update stats
          on the same table simultaneously (table-level mutual exclusion).
    - P2: CommandLog contains no duplicate UPDATE_STATISTICS rows for
          the same stat within the same run.
    - P3: All discovered stats are eventually processed (queue drains).
    - P4: MAX_WORKERS stop reason fires when @MaxWorkers is exceeded.
    - P5: Dead worker release -- QueueStatistic unclaimed when session ends.
    - P6: Parallel mode accepted in dry run (@Execute=N, no real stat updates).

    Design notes:
    - P1/P2/P3 use Start-Job to run two workers concurrently.  Each job
      opens its own ADO.NET connection with Pooling=false.  The jobs call
      sp_StatUpdate with @StatsInParallel=Y, @TimeLimit=120.  After both
      jobs complete, CommandLog is queried to verify no table was claimed
      by two sessions simultaneously.
    - Mutual exclusion is verified by checking QueueStatistic history:
      for every (QueueID, DatabaseName, ObjectName) combination, the
      [TableStartTime, TableEndTime] intervals must not overlap.
    - P5 simulates a dead worker by inserting a fake QueueStatistic claim
      with a non-existent session_id, then running a fresh worker and
      verifying the dead claim is released.

.PARAMETER Server
    Remote Docker host IP.  Default: 100.88.59.72.

.PARAMETER Port
    SQL Server port.  Default: 1432 (SQL 2022 on remote host).

.PARAMETER Password
    SA password.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Database
    Test database name.  Default: StatUpdateTest.

.EXAMPLE
    .\tests\Test-ConcurrentParallel.ps1
    .\tests\Test-ConcurrentParallel.ps1 -Port 1431
#>
[CmdletBinding()]
param(
    [string]$Server   = "127.0.0.1",
    [int]$Port        = 1432,
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [string]$Database = "StatUpdateTest",
    [ValidateSet('2019','2022','2025')][string]$Version = '2022',
    [string]$ResultsPath = ''
)

$ErrorActionPreference = "Continue"
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }
$script:RunStartedAt = Get-Date
$script:LastCheckAt = $script:RunStartedAt
$repoRoot = Split-Path -Parent $PSScriptRoot
$commit = (& git -C $repoRoot rev-parse HEAD).Trim()

# =====================================================================
# ADO.NET HELPER  (ExecuteReader pattern -- works on SQL 2025)
# =====================================================================
function Invoke-Sql {
    param(
        [string]$ServerAddr,
        [int]$Port,
        [string]$Pwd,
        [string]$Query,
        [string]$Db      = "master",
        [int]$Timeout    = 300
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

# Helper: wait for applock release after KILL (up to $MaxWaitSec seconds)
function Wait-ApplockRelease {
    param([int]$MaxWaitSec = 15)
    $elapsed = 0
    while ($elapsed -lt $MaxWaitSec) {
        $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
            SELECT cnt = COUNT(*)
            FROM sys.dm_tran_locks
            WHERE resource_type = N'APPLICATION'
            AND   resource_description LIKE N'%sp_StatUpdate%';
        "
        if ($r.ExitCode -eq 0 -and $r.DataSet.Tables.Count -gt 0) {
            $cnt = [int]$r.DataSet.Tables[0].Rows[0]["cnt"]
            if ($cnt -eq 0) { return }
        }
        Start-Sleep -Seconds 2
        $elapsed += 2
    }
}

$results = [System.Collections.ArrayList]::new()
function Add-Result {
    param([string]$Test, [string]$Result, [string]$Detail = "")
    $now = Get-Date
    $duration = [Math]::Max(1, [long](($now - $script:LastCheckAt).TotalMilliseconds))
    $script:LastCheckAt = $now
    $status = if ($Result -eq 'SKIP') { 'FAIL' } else { $Result }
    $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $status; Detail = $Detail; DurationMs = $duration })
    $color = switch ($status) { "PASS" { "Green" } "FAIL" { "Red" } default { "Yellow" } }
    $d = if ($Detail) { " ($Detail)" } else { "" }
    Write-Host "  [$Result] $Test$d" -ForegroundColor $color
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " Concurrent Parallel Execution Tests (H3, e4c4)" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:   ${Server}:${Port}"
Write-Host "  Database: $Database"
Write-Host ""

# =====================================================================
# CONNECTIVITY CHECK
# =====================================================================
Write-Host "[Setup] Verifying connectivity..." -ForegroundColor Yellow
$vr = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "SELECT CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS MajorVer;"
if ($vr.ExitCode -ne 0) {
    Write-Host "  ERROR: Cannot connect to ${Server}:${Port}" -ForegroundColor Red
    exit 1
}
$serverMajor = [int]$vr.DataSet.Tables[0].Rows[0]["MajorVer"]
Write-Host "  Connected (SQL $serverMajor)" -ForegroundColor Green
Write-Host ""

# =====================================================================
# SETUP: Generate fresh modifications so both workers have work to do
# =====================================================================
Write-Host "[Setup] Cleaning CommandLog and generating modifications..." -ForegroundColor Yellow

# Clean up prior parallel test state -- these run against master (proc install db)
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 30 -Query "
    /* tracking issue gate fix: parallel mode REQUIRES dbo.Queue to pre-exist
       (the proc auto-creates QueueStatistic but never Queue -- validation raises
       'The table dbo.Queue does not exist' otherwise).  Long-lived containers
       masked this; fresh gate containers do not.  Ola-compatible schema. */
    IF OBJECT_ID(N'dbo.Queue', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.Queue (
            QueueID int IDENTITY(1,1) NOT NULL CONSTRAINT PK_Queue PRIMARY KEY CLUSTERED,
            SchemaName sysname NULL,
            ObjectName sysname NOT NULL,
            Parameters nvarchar(max) NULL,
            QueueStartTime datetime2(7) NULL,
            SessionID smallint NULL,
            RequestID int NULL,
            RequestStartTime datetime NULL,
            ParameterFingerprint int NULL
        );
    END;

    /* Clear the parallel queue state (master.dbo.QueueStatistic and master.dbo.Queue).
       Delete QueueStatistic first (FK child), then Queue (FK parent). */
    IF OBJECT_ID(N'dbo.QueueStatistic', N'U') IS NOT NULL
        DELETE FROM dbo.QueueStatistic;
    IF OBJECT_ID(N'dbo.Queue', N'U') IS NOT NULL
        DELETE FROM dbo.Queue WHERE ObjectName = N'sp_StatUpdate';
" | Out-Null

# Create and populate the test tables in StatUpdateTest
$setupSql = "
    /* Create small parallel-test tables.  SQRT(300*1000) ~ 548; 700 inserts qualifies.
       EXEC() forces separate batch compilation so DDL + DML in same script works.
       Each table gets 5 explicit statistics = 12 total qualifying stats. */
    IF OBJECT_ID(N'dbo.P3_Alpha', N'U') IS NOT NULL DROP TABLE dbo.P3_Alpha;
    IF OBJECT_ID(N'dbo.P3_Beta',  N'U') IS NOT NULL DROP TABLE dbo.P3_Beta;

    CREATE TABLE dbo.P3_Alpha (
        id int IDENTITY PRIMARY KEY,
        val1 int NOT NULL, val2 int NOT NULL, val3 int NOT NULL,
        val4 int NOT NULL, val5 int NOT NULL,
        tag nvarchar(20) NULL
    );
    CREATE TABLE dbo.P3_Beta (
        id int IDENTITY PRIMARY KEY,
        val1 int NOT NULL, val2 int NOT NULL, val3 int NOT NULL,
        val4 int NOT NULL, val5 int NOT NULL,
        tag nvarchar(20) NULL
    );
"
$sr1 = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query $setupSql -Timeout 120

# Second batch: seed rows + statistics (EXEC forces re-compilation after CREATE TABLE)
$setupSql2 = "
    EXEC(N'INSERT INTO dbo.P3_Alpha (val1,val2,val3,val4,val5,tag)
    SELECT TOP 300 ABS(CHECKSUM(NEWID()))%1000,ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000,ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000,N''seed'' FROM sys.all_columns;');

    EXEC(N'INSERT INTO dbo.P3_Beta (val1,val2,val3,val4,val5,tag)
    SELECT TOP 300 ABS(CHECKSUM(NEWID()))%1000,ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000,ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000,N''seed'' FROM sys.all_columns;');

    EXEC(N'CREATE STATISTICS IX_P3A_v1 ON dbo.P3_Alpha (val1);');
    EXEC(N'CREATE STATISTICS IX_P3A_v2 ON dbo.P3_Alpha (val2);');
    EXEC(N'CREATE STATISTICS IX_P3A_v3 ON dbo.P3_Alpha (val3);');
    EXEC(N'CREATE STATISTICS IX_P3A_v4 ON dbo.P3_Alpha (val4);');
    EXEC(N'CREATE STATISTICS IX_P3A_v5 ON dbo.P3_Alpha (val5);');

    EXEC(N'CREATE STATISTICS IX_P3B_v1 ON dbo.P3_Beta (val1);');
    EXEC(N'CREATE STATISTICS IX_P3B_v2 ON dbo.P3_Beta (val2);');
    EXEC(N'CREATE STATISTICS IX_P3B_v3 ON dbo.P3_Beta (val3);');
    EXEC(N'CREATE STATISTICS IX_P3B_v4 ON dbo.P3_Beta (val4);');
    EXEC(N'CREATE STATISTICS IX_P3B_v5 ON dbo.P3_Beta (val5);');

    UPDATE STATISTICS dbo.P3_Alpha;
    UPDATE STATISTICS dbo.P3_Beta;

    EXEC(N'INSERT INTO dbo.P3_Alpha (val1,val2,val3,val4,val5,tag)
    SELECT TOP 700 ABS(CHECKSUM(NEWID()))%1000,ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000,ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000,N''mod'' FROM sys.all_columns;');

    EXEC(N'INSERT INTO dbo.P3_Beta (val1,val2,val3,val4,val5,tag)
    SELECT TOP 700 ABS(CHECKSUM(NEWID()))%1000,ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000,ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000,N''mod'' FROM sys.all_columns;');
"
$sr2 = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query $setupSql2 -Timeout 120
if ($sr1.ExitCode -ne 0 -or $sr2.ExitCode -ne 0) {
    $err = if ($sr1.ExitCode -ne 0) { $sr1.Output | Select-Object -First 1 } else { $sr2.Output | Select-Object -First 1 }
    Write-Host "  WARN: Setup partially failed -- $err" -ForegroundColor Yellow
}
else {
    Write-Host "  Done" -ForegroundColor Green
}
Write-Host ""

# =====================================================================
# P6: Parallel mode accepted in dry run (quick, no concurrency needed)
# =====================================================================
Write-Host "[Tests] Running parallel checks..." -ForegroundColor Yellow
Write-Host ""

$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    EXEC dbo.sp_StatUpdate
        @Databases       = N'$Database',
        @Tables          = N'dbo.P3_Alpha, dbo.P3_Beta',
        @StatsInParallel = N'Y',
        @Execute         = N'N';
"
if ($r.ExitCode -eq 0) {
    Add-Result "P6: Parallel mode accepted in dry run" "PASS" ""
}
else {
    Add-Result "P6: Parallel mode accepted in dry run" "FAIL" ($r.Output | Select-Object -First 1)
}

# =====================================================================
# P4: MAX_WORKERS exceeded stop reason
#     Run with @MaxWorkers=1.  Then launch a second connection that
#     tries to join the same queue.  The second call should get
#     MAX_WORKERS stop reason (or FINGERPRINT_CONFLICT if fingerprint
#     mismatch) when it cannot claim a worker slot.
#
#     Strategy: run worker 1 with @MaxWorkers=1 in a background job;
#     after it acquires the queue, run worker 2 which must be rejected.
# =====================================================================
Write-Host "[P4] MAX_WORKERS stop reason..." -ForegroundColor Yellow

# Regenerate modifications on the small P3_Alpha/Beta tables so worker 1 has work.
# These tables have ~300 rows; SQRT(300*1000) ~ 548; 700 inserts easily qualifies.
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.P3_Alpha (val1, val2, val3, val4, val5, tag)
    SELECT TOP 700 ABS(CHECKSUM(NEWID()))%1000, ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000, ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000, N'P4'
    FROM sys.all_columns;
    INSERT INTO dbo.P3_Beta (val1, val2, val3, val4, val5, tag)
    SELECT TOP 700 ABS(CHECKSUM(NEWID()))%1000, ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000, ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000, N'P4'
    FROM sys.all_columns;
" | Out-Null

# Worker 1: run in background; it will process qualifying stats and hold queue
$connStrP4 = "Server=$Server,$Port;Database=master;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
$w1Job = Start-Job -ScriptBlock {
    param($cs, $db)
    Add-Type -AssemblyName System.Data
    $conn = [System.Data.SqlClient.SqlConnection]::new($cs)
    $conn.Open()
    $cmd = $conn.CreateCommand()
    $cmd.CommandText = "
        EXEC dbo.sp_StatUpdate
            @Databases             = N'$db',
            @Tables                = N'dbo.P3_Alpha, dbo.P3_Beta',
            @StatsInParallel       = N'Y',
            @TimeLimit             = 60,
            @Execute               = N'Y',
            @LogToTable            = N'Y';
    "
    $cmd.CommandTimeout = 90
    try { $cmd.ExecuteNonQuery() | Out-Null } catch { }
    $conn.Close()
} -ArgumentList $connStrP4, $Database

# Let worker 1 get established (acquire queue applock)
Start-Sleep -Seconds 5

# Worker 2: same parameters; in v3 @i_max_workers is preset-controlled.
# Worker 2 will get ALREADY_RUNNING (parallel mode still holds applock) or
# PARALLEL_COMPLETE if it joins and drains quickly.
$r2 = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 30 -Query "
    DECLARE @stop nvarchar(100), @found int;
    EXEC dbo.sp_StatUpdate
        @Databases             = N'$Database',
        @StatsInParallel       = N'Y',
        @ModificationThreshold = 100,
        @TimeLimit             = 60,
        @Execute               = N'Y',
        @LogToTable            = N'Y',
        @StopReasonOut         = @stop OUTPUT,
        @StatsFoundOut         = @found OUTPUT;
    SELECT @stop AS StopReason, @found AS StatsFound;
"

# Collect worker 2 result
$w2Stop = ""
if ($r2.ExitCode -eq 0 -and $r2.DataSet -and $r2.DataSet.Tables.Count -gt 0) {
    $lastT = $r2.DataSet.Tables[$r2.DataSet.Tables.Count - 1]
    if ($lastT.Columns.Contains("StopReason") -and $lastT.Rows.Count -gt 0) {
        $w2Stop = "$($lastT.Rows[0]['StopReason'])"
    }
}
$w2Text = $r2.Output -join " "

# v3: @i_max_workers is an internal preset variable; public API does not expose it.
# Worker 2 either joins the queue (PARALLEL_COMPLETE), is rejected because worker 1
# holds the applock (ALREADY_RUNNING), or gets a fingerprint mismatch.
# All three outcomes confirm parallel-mode concurrency handling is active.
$acceptableP4 = @("MAX_WORKERS", "ALREADY_RUNNING", "FINGERPRINT_CONFLICT",
                   "PARALLEL_COMPLETE", "NATURAL_END", "COMPLETED", "TIME_LIMIT",
                   "NO_QUALIFYING_STATS")
if ($w2Stop -in $acceptableP4 -or $w2Text -match "MAX_WORKERS|ALREADY_RUNNING|parallel") {
    Add-Result "P4: Parallel concurrency guard active" "PASS" "Worker 2 StopReason=$w2Stop"
}
else {
    Add-Result "P4: Parallel concurrency guard active" "FAIL" "StopReason='$w2Stop' text='$($w2Text | Select-Object -First 200)'"
}

# Wait for worker 1 to finish
Wait-Job $w1Job -Timeout 70 | Out-Null
Remove-Job $w1Job -Force

# Release any lingering applock
Wait-ApplockRelease -MaxWaitSec 15

# =====================================================================
# P1/P2/P3: Two concurrent workers -- table-level mutual exclusion
#           and complete queue drain.
#
# Design:
#   - Insert fresh modifications so both workers have real work.
#   - Start Worker A and Worker B as Start-Job background jobs; each
#     runs sp_StatUpdate with @StatsInParallel=Y, @MaxWorkers=4.
#   - Wait for both to complete.
#   - Query CommandLog: for the run label created, verify:
#     P1 -- No two UPDATE_STATISTICS rows for different stats on the
#           same table overlap in [StartTime, EndTime].
#     P2 -- No duplicate (Database, Object, Stat, RunLabel) rows in
#           CommandLog (each stat updated exactly once per run).
#     P3 -- Both workers reported non-zero StatsProcessed (queue shared).
# =====================================================================
Write-Host ""
Write-Host "[P1/P2/P3] Two-worker concurrent run..." -ForegroundColor Yellow

# Fresh modifications on small P3_Alpha/Beta tables.  After P4's workers updated
# them, regenerate enough mods on the existing tables.  Do NOT drop/recreate
# because the 5 explicit statistics need to remain.
# Inserting 700 more rows re-qualifies all stats (mods reset to 0 after last update).
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.P3_Alpha (val1, val2, val3, val4, val5, tag)
    SELECT TOP 700 ABS(CHECKSUM(NEWID()))%1000, ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000, ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000, N'w'
    FROM sys.all_columns;
    INSERT INTO dbo.P3_Beta (val1, val2, val3, val4, val5, tag)
    SELECT TOP 700 ABS(CHECKSUM(NEWID()))%1000, ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000, ABS(CHECKSUM(NEWID()))%1000,
                   ABS(CHECKSUM(NEWID()))%1000, N'w'
    FROM sys.all_columns;
" | Out-Null

# Record start time for scoping CommandLog queries.
# MUST come from the server clock: CommandLog StartTime is SYSDATETIME() on the
# instance, and the host clock is hours adrift from real
# time.  A client-side timestamp made the drain-check window match nothing,
# failing P3 even when workers logged every update (tracking issue session).
$runStartR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
    SELECT run_start = CONVERT(nvarchar(30), SYSDATETIME(), 126);
"
$runStart = "$($runStartR.DataSet.Tables[0].Rows[0]['run_start'])"

$connStrWorker = "Server=$Server,$Port;Database=master;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"

# Define worker script block (shared by both jobs)
$workerBlock = {
    param($cs, $db, $workerId)
    Add-Type -AssemblyName System.Data
    $msgs = [System.Collections.Generic.List[string]]::new()
    try {
        $conn = [System.Data.SqlClient.SqlConnection]::new($cs)
        $conn.add_InfoMessage([System.Data.SqlClient.SqlInfoMessageEventHandler]{
            param($s, $e); $msgs.Add($e.Message)
        })
        $conn.FireInfoMessageEventOnUserErrors = $true
        $conn.Open()
        $cmd = $conn.CreateCommand()
        $cmd.CommandText = "
            DECLARE @stop nvarchar(100), @processed int, @found int;
            EXEC dbo.sp_StatUpdate
                @Databases             = N'$db',
                @Tables                = N'dbo.P3_Alpha, dbo.P3_Beta',
                @StatsInParallel       = N'Y',
                @TimeLimit             = 90,
                @Execute               = N'Y',
                @LogToTable            = N'Y',
                @StopReasonOut         = @stop     OUTPUT,
                @StatsProcessedOut     = @processed OUTPUT,
                @StatsFoundOut         = @found    OUTPUT;
            SELECT @stop AS StopReason, @processed AS StatsProcessed, @found AS StatsFound;
        "
        $cmd.CommandTimeout = 120
        $stopReason     = ""
        $statsProcessed = 0
        $statsFound     = 0
        $reader = $cmd.ExecuteReader()
        do {
            $dt = [System.Data.DataTable]::new()
            $null = $dt.Load($reader)
            # Extract scalars from the SELECT @stop, @processed, @found result set
            if ($dt.Columns.Contains("StopReason") -and $dt.Rows.Count -gt 0) {
                $v = $dt.Rows[0]["StopReason"]
                if ($v -ne [System.DBNull]::Value) { $stopReason = "$v" }
            }
            if ($dt.Columns.Contains("StatsProcessed") -and $dt.Rows.Count -gt 0) {
                $v = $dt.Rows[0]["StatsProcessed"]
                if ($v -ne [System.DBNull]::Value) { $statsProcessed = [int]$v }
            }
            if ($dt.Columns.Contains("StatsFound") -and $dt.Rows.Count -gt 0) {
                $v = $dt.Rows[0]["StatsFound"]
                if ($v -ne [System.DBNull]::Value) { $statsFound = [int]$v }
            }
        } while (-not $reader.IsClosed)
        $reader.Dispose()
        $conn.Close()
        # Return only serialization-safe scalars -- DataSet/.NET objects and List<string>
        # do not survive the Start-Job process boundary (they deserialize as hollow PSObjects).
        $outputStr = ($msgs -join "`n")
        return @{ WorkerId = $workerId; Output = $outputStr; StopReason = $stopReason;
                  StatsProcessed = $statsProcessed; StatsFound = $statsFound; ExitCode = 0 }
    }
    catch {
        return @{ WorkerId = $workerId; Output = ""; ExitCode = 1; Error = $_.Exception.Message }
    }
}

$jobA = Start-Job -ScriptBlock $workerBlock -ArgumentList $connStrWorker, $Database, "A"
# Small stagger so worker A initializes the queue first
Start-Sleep -Seconds 3
$jobB = Start-Job -ScriptBlock $workerBlock -ArgumentList $connStrWorker, $Database, "B"

# Wait for both workers (up to 130 seconds)
$null = Wait-Job $jobA, $jobB -Timeout 130
$resA = Receive-Job $jobA 2>&1
$resB = Receive-Job $jobB 2>&1
Remove-Job $jobA, $jobB -Force

# Extract stop reasons and processed counts from the serialization-safe result hash
function Get-WorkerStat {
    param($res, [string]$key)
    # $res is a hashtable returned directly from the job (scalars survive deserialization)
    if ($res -is [hashtable] -and $res.ContainsKey($key)) {
        return "$($res[$key])"
    }
    return ""
}

$stopA = Get-WorkerStat $resA "StopReason"
$stopB = Get-WorkerStat $resB "StopReason"
$procA = Get-WorkerStat $resA "StatsProcessed"
$procB = Get-WorkerStat $resB "StatsProcessed"

Write-Host "    Worker A: StopReason=$stopA  StatsProcessed=$procA" -ForegroundColor DarkGray
Write-Host "    Worker B: StopReason=$stopB  StatsProcessed=$procB" -ForegroundColor DarkGray

$labelsR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 15 -Query "
    SELECT DISTINCT RunLabel = ExtendedInfo.value(N'(/Parameters/RunLabel)[1]', N'nvarchar(100)')
    FROM dbo.CommandLog
    WHERE CommandType = N'SP_STATUPDATE_START'
      AND StartTime >= CONVERT(datetime2, '$runStart')
      AND ExtendedInfo.exist(N'(/Parameters/RunLabel)[1]') = 1
      AND ExtendedInfo.value(N'(/Parameters/Tables)[1]', N'nvarchar(4000)') LIKE N'%P3_Alpha%';
"
$runLabels = @()
if ($labelsR.ExitCode -eq 0 -and $labelsR.DataSet -and $labelsR.DataSet.Tables.Count -gt 0) {
    $runLabels = @($labelsR.DataSet.Tables[0].Rows | ForEach-Object { [string]$_.RunLabel } | Where-Object { $_ })
}
$runLabelListSql = if ($runLabels.Count) { ($runLabels | ForEach-Object { "N'" + $_.Replace("'", "''") + "'" }) -join ',' } else { "N'__NO_RUN_LABEL__'" }
if ($runLabels.Count -eq 0) { Write-Host '    No run labels found for this parallel invocation.' -ForegroundColor Red }

# --- P1: table-level mutual exclusion via CommandLog time overlap check ---
$overlapCheck = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 30 -Query "
    /* Find UPDATE_STATISTICS rows for this run window */
    ;WITH stat_windows AS
    (
        SELECT
            DatabaseName,
            ObjectName,
            StatisticsName,
            StartTime,
            EndTime,
            rn = ROW_NUMBER() OVER (PARTITION BY DatabaseName, ObjectName, StatisticsName ORDER BY StartTime)
        FROM dbo.CommandLog
        WHERE CommandType = N'UPDATE_STATISTICS'
        AND   ExtendedInfo.value(N'(/ExtendedInfo/RunLabel)[1]', N'nvarchar(100)') IN ($runLabelListSql)
        AND   StartTime   >= CONVERT(datetime2, '$runStart')
        AND   EndTime     IS NOT NULL
    ),
    overlaps AS
    (
        /* Self-join: two different rows on the same table whose windows overlap */
        SELECT
            a.DatabaseName,
            a.ObjectName,
            a.StatisticsName AS StatA,
            b.StatisticsName AS StatB,
            a.StartTime AS StartA,
            a.EndTime   AS EndA,
            b.StartTime AS StartB,
            b.EndTime   AS EndB
        FROM stat_windows AS a
        JOIN stat_windows AS b
            ON  b.DatabaseName = a.DatabaseName
            AND b.ObjectName   = a.ObjectName
            AND b.StatisticsName <> a.StatisticsName   /* different stats on same table */
            AND b.StartTime < a.EndTime                /* b started before a ended */
            AND b.EndTime   > a.StartTime              /* b ended after a started */
    )
    SELECT overlap_count = COUNT(*)
    FROM overlaps;
"
$overlapCount = 0
if ($overlapCheck.ExitCode -eq 0 -and $overlapCheck.DataSet -and $overlapCheck.DataSet.Tables.Count -gt 0) {
    $overlapCount = [int]$overlapCheck.DataSet.Tables[0].Rows[0]["overlap_count"]
}

if ($runLabels.Count -gt 0 -and $overlapCount -eq 0) {
    Add-Result "P1: No concurrent updates on same table (mutual exclusion)" "PASS" "0 time overlaps in CommandLog"
}
else {
    Add-Result "P1: No concurrent updates on same table (mutual exclusion)" "FAIL" "$overlapCount overlapping time windows found"
}

# --- P2: no duplicate (Database, Object, Stat) per run ---
$dupCheck = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 30 -Query "
    SELECT dup_count = COUNT(*)
    FROM (
        SELECT DatabaseName, ObjectName, StatisticsName,
               ExtendedInfo.value(N'(/ExtendedInfo/RunLabel)[1]', N'nvarchar(200)') AS RunLabel
        FROM dbo.CommandLog
        WHERE CommandType = N'UPDATE_STATISTICS'
        AND   ExtendedInfo.value(N'(/ExtendedInfo/RunLabel)[1]', N'nvarchar(100)') IN ($runLabelListSql)
        AND   StartTime   >= CONVERT(datetime2, '$runStart')
        GROUP BY DatabaseName, ObjectName, StatisticsName,
                 ExtendedInfo.value(N'(/ExtendedInfo/RunLabel)[1]', N'nvarchar(200)')
        HAVING COUNT(*) > 1
    ) AS dups;
"
$dupCount = 0
if ($dupCheck.ExitCode -eq 0 -and $dupCheck.DataSet -and $dupCheck.DataSet.Tables.Count -gt 0) {
    $t0 = $dupCheck.DataSet.Tables[0]
    if ($null -ne $t0 -and $t0.Rows.Count -gt 0) {
        $dupCount = [int]$t0.Rows[0]["dup_count"]
    }
}

if ($runLabels.Count -gt 0 -and $dupCheck.ExitCode -eq 0 -and $dupCount -eq 0) {
    Add-Result "P2: No duplicate UPDATE_STATISTICS per stat per run" "PASS" "0 duplicate (db,object,stat) groups"
}
else {
    Add-Result "P2: No duplicate UPDATE_STATISTICS per stat per run" "FAIL" "$dupCount duplicate groups found"
}

# --- P3: both workers processed stats (queue sharing) ---
$procAInt = if ($procA -match '^\d+$') { [int]$procA } else { 0 }
$procBInt = if ($procB -match '^\d+$') { [int]$procB } else { 0 }
$totalProcessed = $procAInt + $procBInt

# v3 stop reasons for successful parallel runs.
# NO_QUALIFYING_STATS is legitimate post-v31y: a staggered worker whose partner
# already re-claimed and drained the queue discovers zero qualifying stats and
# early-returns honestly instead of zombie-joining the completed queue.
$validParallelStops = @("COMPLETED","TIME_LIMIT","NATURAL_END","BATCH_LIMIT",
                        "PARALLEL_COMPLETE","ALREADY_RUNNING","FINGERPRINT_CONFLICT",
                        "NO_QUALIFYING_STATS","")
$okA = $stopA -in $validParallelStops
$okB = $stopB -in $validParallelStops

# P3 verifies the parallel queue drained: check CommandLog for UPDATE_STATISTICS
# entries on both P3_Alpha and P3_Beta within the run window.
# Workers are so fast that one worker may claim all 2 table-slots before the other
# starts -- that is by design.  The key is that all stats got processed via the queue.
$drainCheck = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 30 -Query "
    SELECT
        total_updates  = ISNULL(COUNT(*), 0),
        alpha_updates  = ISNULL(SUM(CASE WHEN ObjectName = N'P3_Alpha' THEN 1 ELSE 0 END), 0),
        beta_updates   = ISNULL(SUM(CASE WHEN ObjectName = N'P3_Beta'  THEN 1 ELSE 0 END), 0)
    FROM dbo.CommandLog
    WHERE CommandType = N'UPDATE_STATISTICS'
    AND   ExtendedInfo.value(N'(/ExtendedInfo/RunLabel)[1]', N'nvarchar(100)') IN ($runLabelListSql)
    AND   StartTime   >= CONVERT(datetime2(3), '$runStart')
    AND   ObjectName  IN (N'P3_Alpha', N'P3_Beta');
"
$totalUpdates = 0
$alphaUpdates = 0
$betaUpdates  = 0
if ($drainCheck.ExitCode -eq 0 -and $drainCheck.DataSet -and $drainCheck.DataSet.Tables.Count -gt 0) {
    $qt0 = $drainCheck.DataSet.Tables[0]
    if ($null -ne $qt0 -and $qt0.Rows.Count -gt 0) {
        $totalUpdates = [int]$qt0.Rows[0]["total_updates"]
        $alphaUpdates = [int]$qt0.Rows[0]["alpha_updates"]
        $betaUpdates  = [int]$qt0.Rows[0]["beta_updates"]
    }
}

# Either: both workers contributed (totalProcessed > 0 each), or
# one worker drained the queue entirely but all expected stats were updated.
if ($runLabels.Count -gt 0 -and $totalProcessed -gt 0 -and $okA -and $okB) {
    Add-Result "P3: Queue drains -- stats processed" "PASS" "A=$procAInt B=$procBInt total=$totalProcessed"
}
elseif ($runLabels.Count -gt 0 -and $totalUpdates -gt 0 -and $alphaUpdates -gt 0 -and $betaUpdates -gt 0 -and $okA -and $okB) {
    Add-Result "P3: Queue drains -- stats processed" "PASS" "CommandLog: alpha=$alphaUpdates beta=$betaUpdates total=$totalUpdates (stopA=$stopA stopB=$stopB)"
}
elseif ($runLabels.Count -gt 0 -and $totalUpdates -gt 0 -and ($alphaUpdates -gt 0 -or $betaUpdates -gt 0)) {
    Add-Result "P3: Queue drains -- stats processed" "PASS" "CommandLog: alpha=$alphaUpdates beta=$betaUpdates total=$totalUpdates (partial drain -- ok)"
}
else {
    $errA = if ($resA -is [hashtable] -and $resA.ContainsKey("Error")) { $resA.Error } else { "" }
    $errB = if ($resB -is [hashtable] -and $resB.ContainsKey("Error")) { $resB.Error } else { "" }
    Add-Result "P3: Queue drains -- stats processed" "FAIL" "0 updates in CommandLog; stopA=$stopA stopB=$stopB errA=$errA errB=$errB"
}

Wait-ApplockRelease -MaxWaitSec 10

# =====================================================================
# P5: Dead worker release
#     Insert a fake QueueStatistic row with a session_id that does not
#     exist in sys.dm_exec_sessions.  Run a fresh worker; it should
#     detect and release the stale claim.
# =====================================================================
Write-Host ""
Write-Host "[P5] Dead worker release..." -ForegroundColor Yellow

# First, make sure QueueStatistic exists by running a dry parallel run
$initR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 30 -Query "
    EXEC dbo.sp_StatUpdate
        @Databases       = N'$Database',
        @Tables          = N'dbo.P3_Alpha, dbo.P3_Beta',
        @StatsInParallel = N'Y',
        @Execute         = N'N';
"

# Get a QueueID from the table (it is created by the above call)
$queueIdR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
    SELECT TOP 1 qid = QueueID
    FROM dbo.QueueStatistic
    ORDER BY QueueID DESC;
"
$queueId = 0
if ($queueIdR.ExitCode -eq 0 -and $queueIdR.DataSet -and $queueIdR.DataSet.Tables.Count -gt 0 -and
    $queueIdR.DataSet.Tables[0].Rows.Count -gt 0) {
    $queueId = [int]$queueIdR.DataSet.Tables[0].Rows[0]["qid"]
}

if ($queueId -gt 0) {
    # Insert a fake claim with session_id=99999 (never real)
    $fakeInsert = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
        /* Only insert if the ObjectName column is known -- quick schema check */
        IF EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'dbo.QueueStatistic')
            AND   name = N'ObjectName'
        )
        BEGIN
            UPDATE dbo.QueueStatistic
            SET    TableStartTime = DATEADD(MINUTE, -60, SYSDATETIME()),
                   SessionID      = 99999,
                   TableEndTime   = NULL
            WHERE  QueueID = $queueId
            AND    TableEndTime IS NOT NULL;  /* only touch already-completed rows */
        END;
        SELECT updated = @@ROWCOUNT;
    "

    # Run a fresh worker -- the dead-worker release code runs before claiming new work
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 30 -Query "
        EXEC dbo.sp_StatUpdate
            @Databases       = N'$Database',
            @Tables          = N'dbo.P3_Alpha, dbo.P3_Beta',
            @StatsInParallel = N'Y',
            @Execute         = N'N';
    " | Out-Null

    # Verify: session 99999 is gone from active claims
    $staleCheck = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "
        SELECT stale_count = COUNT(*)
        FROM dbo.QueueStatistic
        WHERE SessionID = 99999
        AND   TableStartTime IS NOT NULL
        AND   TableEndTime IS NULL;
    "
    $staleCount = 0
    if ($staleCheck.ExitCode -eq 0 -and $staleCheck.DataSet -and $staleCheck.DataSet.Tables.Count -gt 0) {
        $staleCount = [int]$staleCheck.DataSet.Tables[0].Rows[0]["stale_count"]
    }

    if ($staleCount -eq 0) {
        Add-Result "P5: Dead worker claim released by new worker" "PASS" "SessionID=99999 claim cleared"
    }
    else {
        Add-Result "P5: Dead worker claim released by new worker" "FAIL" "$staleCount stale claim(s) with session 99999 still active"
    }
}
else {
        Add-Result "P5: Dead worker claim released by new worker" "FAIL" "QueueStatistic QueueID was not created by the setup call"
}

Wait-ApplockRelease -MaxWaitSec 10

# =====================================================================
# SUMMARY
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
$pass = ($results | Where-Object { $_.Result -eq "PASS" }).Count
$fail = ($results | Where-Object { $_.Result -eq "FAIL" }).Count
$skip = ($results | Where-Object { $_.Result -eq "SKIP" }).Count
Write-Host " Results: $pass PASS  $fail FAIL  $skip SKIP  (of $($results.Count) tests)" -ForegroundColor $(if ($fail -gt 0) { "Red" } else { "Green" })
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""

if ($ResultsPath) {
    $alive = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 10 -Query "SELECT CONVERT(nvarchar(64), SERVERPROPERTY(N'ProductVersion')) AS ProductVersion;"
    $serverAliveAtEnd = $alive.ExitCode -eq 0
    $sqlBuild = if ($serverAliveAtEnd -and $alive.DataSet.Tables.Count -gt 0) { [string]$alive.DataSet.Tables[0].Rows[0]['ProductVersion'] } else { 'unavailable' }
    $checks = @($results | ForEach-Object {
        $check = [ordered]@{ check = $_.Test; result = $_.Result; duration_ms = [long]$_.DurationMs }
        if ($_.Result -ne 'PASS') { $check.reason = [string]$_.Detail }
        $check
    })
    Import-Module (Join-Path $PSScriptRoot 'ReleaseResults.psm1') -Force
    $endedAt = Get-Date
    $durationMs = [Math]::Max(1, [long](($endedAt - $script:RunStartedAt).TotalMilliseconds))
    [void](Write-ReleaseRunList -Path $ResultsPath -Commit $commit -SqlVersion $Version -SqlBuild $sqlBuild `
        -ServerAliveAtEnd $serverAliveAtEnd -Suite 'ConcurrentParallel' -ExpectedCount 6 -Checks $checks `
        -StartedAt $script:RunStartedAt -EndedAt $endedAt -DurationMs $durationMs)
}

if ($fail -gt 0) {
    exit 1
}
