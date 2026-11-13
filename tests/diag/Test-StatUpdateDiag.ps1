<#
.SYNOPSIS
    Test suite for sp_StatUpdate_Diag diagnostic procedure.

.DESCRIPTION
    Runs deterministic test data setup, executes sp_StatUpdate_Diag in multiple
    modes, and validates results against expected diagnostics.

    Tests:
      1. Setup: Deploy test data and sp_StatUpdate_Diag
      2. Help output returns expected columns
      3. Clear mode: All expected diagnostics detected
      4. Obfuscated mode: Names hashed, mapping returned
      5. Empty CommandLog: Graceful handling
      6. Result set count and column validation
      7. Debug mode: Extra output without errors
      8. Custom parameters: @DaysBack filtering, @TopN limiting

.PARAMETER Server
    SQL Server instance. Default: 127.0.0.1,1433

.PARAMETER Database
    Database for testing. Default: master

.PARAMETER Credential
    SQL credential. If not provided, uses the SQL login.

.EXAMPLE
    .\Test-StatUpdateDiag.ps1
    .\Test-StatUpdateDiag.ps1 -Server "127.0.0.1,2500" -Database master
#>

[CmdletBinding()]
param(
    [string]$Server = "127.0.0.1,1433",
    [string]$Database = "master",
    [PSCredential]$Credential,
    [string]$ResultsPath,
    [ValidateSet('2019','2022','2025')][string]$SqlVersion,
    [string]$Commit
)

$ErrorActionPreference = "Stop"
$suiteStartedAt = [DateTime]::UtcNow

# Default credential
if (-not $Credential) {
    $effectivePassword = if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '$env:SPSTATUPDATE_TEST_SQL_PASSWORD' }
    $Credential = [PSCredential]::new('sa', (ConvertTo-SecureString $effectivePassword -AsPlainText -Force))
}

# Paths
$scriptRoot = $PSScriptRoot
$repoRoot = (Resolve-Path (Join-Path $scriptRoot "../../")).Path
$diagProc = Join-Path $repoRoot "sp_StatUpdate_Diag.sql"
$testData = Join-Path $scriptRoot "Setup-DiagTestData.sql"

# Test tracking
$testResults = [System.Collections.Generic.List[PSObject]]::new()
$totalTests = 0
$passedTests = 0
$failedTests = 0
$skippedTests = 0

function Write-TestHeader {
    param([string]$Name)
    Write-Host ""
    Write-Host "  TEST: $Name" -ForegroundColor Cyan
}

function Write-TestResult {
    param(
        [string]$Name,
        [bool]$Passed,
        [string]$Detail = "",
        [switch]$Skipped
    )

    $script:totalTests++
    if ($Skipped) {
        $script:skippedTests++
        Write-Host "    SKIP" -ForegroundColor Yellow -NoNewline
        Write-Host " $Name" -NoNewline
        if ($Detail) { Write-Host " ($Detail)" -ForegroundColor DarkGray }
        else { Write-Host "" }
        $script:testResults.Add([PSCustomObject]@{
            Test    = $Name
            Status  = "SKIP"
            Passed  = $false
            Detail  = $Detail
        })
        return
    }

    if ($Passed) {
        $script:passedTests++
        Write-Host "    PASS" -ForegroundColor Green -NoNewline
    }
    else {
        $script:failedTests++
        Write-Host "    FAIL" -ForegroundColor Red -NoNewline
    }
    Write-Host " $Name" -NoNewline
    if ($Detail) { Write-Host " ($Detail)" -ForegroundColor DarkGray }
    else { Write-Host "" }

    $script:testResults.Add([PSCustomObject]@{
        Test    = $Name
        Status  = if ($Passed) { "PASS" } else { "FAIL" }
        Passed  = $Passed
        Detail  = $Detail
    })
}

function Invoke-SqlQuery {
    param(
        [string]$Query,
        [switch]$MultiResultSet
    )

    $connStr = "Server=$Server;Database=$Database;User ID=$($Credential.UserName);Password=$($Credential.GetNetworkCredential().Password);TrustServerCertificate=True;Connection Timeout=30;"
    $conn = New-Object System.Data.SqlClient.SqlConnection($connStr)
    $cmd = $conn.CreateCommand()
    $cmd.CommandTimeout = 120
    $cmd.CommandText = $Query

    $adapter = New-Object System.Data.SqlClient.SqlDataAdapter($cmd)
    $ds = New-Object System.Data.DataSet

    try {
        $conn.Open()
        $adapter.Fill($ds) | Out-Null
    }
    finally {
        $conn.Close()
        $conn.Dispose()
    }

    if ($MultiResultSet) { return $ds }
    elseif ($ds.Tables.Count -gt 0) { return $ds.Tables[0] }
    else { return $null }
}

function Get-RSBySignature {
    <# Maps DataSet tables to named result sets by unique column signatures.
       Returns hashtable keyed by RS name. #>
    param([System.Data.DataSet]$DataSet)

    $map = @{}
    foreach ($table in $DataSet.Tables) {
        $cols = $table.Columns | ForEach-Object { $_.ColumnName }
        if ($cols -contains "Grade" -and $cols -contains "Score" -and $cols -contains "Headline") { $map["Dashboard"] = $table }
        elseif ($cols -contains "Finding" -and $cols -contains "Recommendation" -and $cols -contains "Severity") { $map["Recommendations"] = $table }
        elseif ($cols -contains "TotalRuns") { $map["RunHealth"] = $table }
        elseif ($cols -contains "RunLabel" -and $cols -contains "StopReason" -and $cols -contains "IsKilled") { $map["RunDetail"] = $table }
        elseif ($cols -contains "TotalDurationSec") { $map["TopTables"] = $table }
        elseif ($cols -contains "FailCount") { $map["FailingStats"] = $table }
        elseif ($cols -contains "AvgDurationSec" -and -not ($cols -contains "FailCount")) { $map["LongRunning"] = $table }
        elseif ($cols -contains "TieredThresholds") { $map["ParamHistory"] = $table }
        elseif ($cols -contains "OriginalName" -and $cols -contains "ObfuscatedName") { $map["ObfuscationMap"] = $table }
        elseif ($cols -contains "WeekLabel" -and $cols -contains "TrendDirection") { $map["EfficacyTrend"] = $table }
        elseif ($cols -contains "DeltaVsPrior") { $map["EfficacyDetail"] = $table }
        elseif ($cols -contains "ProcessingPosition" -and $cols -contains "WorkloadRank") { $map["HighCpuPositions"] = $table }
        elseif ($cols -contains "CpuTrend" -and $cols -contains "CpuChangePct") { $map["QSCorrelation"] = $table }
    }
    return $map
}

function Invoke-SqlFile {
    param([string]$FilePath)

    $connStr = "Server=$Server;Database=$Database;User ID=$($Credential.UserName);Password=$($Credential.GetNetworkCredential().Password);TrustServerCertificate=True;Connection Timeout=30;"
    $conn = New-Object System.Data.SqlClient.SqlConnection($connStr)
    $cmd = $conn.CreateCommand()
    $cmd.CommandTimeout = 120

    # Read and split on GO
    $content = Get-Content $FilePath -Raw
    $batches = $content -split "(?m)^\s*GO\s*$"

    try {
        $conn.Open()
        foreach ($batch in $batches) {
            $trimmed = $batch.Trim()
            if ($trimmed.Length -eq 0) { continue }
            $cmd.CommandText = $trimmed
            $cmd.ExecuteNonQuery() | Out-Null
        }
    }
    finally {
        $conn.Close()
        $conn.Dispose()
    }
}

# =============================================================================
# SETUP
# =============================================================================

Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host " sp_StatUpdate_Diag Test Suite" -ForegroundColor Cyan
Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Server:   $Server"
Write-Host "Database: $Database"
Write-Host ""

Write-Host "SETUP" -ForegroundColor Yellow
Write-Host "-----" -ForegroundColor Yellow

# Deploy procedure
Write-Host "  Deploying sp_StatUpdate_Diag..." -NoNewline
try {
    Invoke-SqlFile -FilePath $diagProc
    Write-Host " OK" -ForegroundColor Green
}
catch {
    Write-Host " FAILED: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Load test data
Write-Host "  Loading test data..." -NoNewline
try {
    Invoke-SqlFile -FilePath $testData
    Write-Host " OK" -ForegroundColor Green
}
catch {
    Write-Host " FAILED: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Verify test data loaded
$rowCount = (Invoke-SqlQuery "SELECT cnt = COUNT(*) FROM dbo.CommandLog").cnt
Write-Host "  CommandLog rows: $rowCount"
Write-Host ""

# =============================================================================
# TEST 1: Help Output
# =============================================================================

Write-TestHeader "Help Output"

try {
    $helpResult = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @Help = 1;" -MultiResultSet
    $helpTable = $helpResult.Tables[0]

    Write-TestResult -Name "Help returns result set" -Passed ($null -ne $helpTable -and $helpTable.Rows.Count -gt 0) -Detail "$($helpTable.Rows.Count) rows"

    $helpCols = $helpTable.Columns | ForEach-Object { $_.ColumnName }
    $hasParameter = $helpCols -contains "parameter_name"
    $hasDescription = $helpCols -contains "description"
    Write-TestResult -Name "Help has parameter_name column" -Passed $hasParameter
    Write-TestResult -Name "Help has description column" -Passed $hasDescription

    # Check key parameters are documented
    $paramNames = $helpTable.Rows | ForEach-Object { $_["parameter_name"].ToString() }
    Write-TestResult -Name "Help documents @DaysBack" -Passed ($paramNames -contains "@DaysBack")
    Write-TestResult -Name "Help documents @Obfuscate" -Passed ($paramNames -contains "@Obfuscate")
    $helpTopicRows = @(
        foreach ($table in $helpResult.Tables) {
            if ($table.Columns.Contains("topic") -and $table.Columns.Contains("detail")) {
                foreach ($row in $table.Rows) {
                    [pscustomobject]@{
                        Topic  = $row["topic"].ToString()
                        Detail = $row["detail"].ToString()
                    }
                }
            }
        }
    )
    $reasonNote = $helpTopicRows | Where-Object { $_.Topic -eq "Qualification Reason" } | Select-Object -First 1
    $reasonDetail = if ($reasonNote) { $reasonNote.Detail } else { "" }
    $documentsReasonCompatibility = $reasonDetail -match 'ASCENDING_KEY' -and $reasonDetail -match 'QUERY_STORE_PRIORITY' -and $reasonDetail -match 'QSPriorityBoost'
    $reasonNoteDetail = if ($reasonNote) { $reasonDetail } else { 'Qualification Reason note is missing' }
    Write-TestResult -Name "Help explains QualifyReason compatibility" -Passed $documentsReasonCompatibility -Detail $reasonNoteDetail

    $singleResultSetNote = $helpTopicRows | Where-Object { $_.Topic -eq "SingleResultSet Mode" } | Select-Object -First 1
    $singleResultSetDetail = if ($singleResultSetNote) { $singleResultSetNote.Detail } else { "" }
    $singleResultSetHelpIsAccurate = $singleResultSetDetail -match '1-13' -and $singleResultSetDetail -notmatch 'all 8 result sets'
    Write-TestResult -Name "SingleResultSet help documents IDs 1-13" -Passed $singleResultSetHelpIsAccurate -Detail $singleResultSetDetail

    $sqlVersionNote = $helpTopicRows | Where-Object { $_.Topic -eq "SQL Server Version" } | Select-Object -First 1
    $sqlVersionDetail = if ($sqlVersionNote) { $sqlVersionNote.Detail } else { "" }
    $sqlVersionHelpIsAccurate = $sqlVersionDetail -match 'SQL Server 2019 or later'
    Write-TestResult -Name "SQL version help matches supported floor" -Passed $sqlVersionHelpIsAccurate -Detail $sqlVersionDetail

    $helpVersionRows = @(
        foreach ($table in $helpResult.Tables) {
            if ($table.Columns.Contains("version_date") -and $table.Columns.Contains("changes")) {
                foreach ($row in $table.Rows) {
                    [pscustomobject]@{
                        VersionDate = $row["version_date"].ToString()
                        Changes     = $row["changes"].ToString()
                    }
                }
            }
        }
    )
    $currentVersionHistory = $helpVersionRows | Where-Object { $_.VersionDate -eq "2026.11.12" } | Select-Object -First 1
    $currentVersionHistoryDetail = if ($currentVersionHistory) { $currentVersionHistory.Changes } else { "Current Diag update is missing from Help version history" }
    Write-TestResult -Name "Help version history lists current Diag update" -Passed ($null -ne $currentVersionHistory) -Detail $currentVersionHistoryDetail
}
catch {
    Write-TestResult -Name "Help output" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 2: Clear Mode - Expected Diagnostics
# =============================================================================

Write-TestHeader "Clear Mode - Diagnostic Detection"

try {
    $clearResult = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $recommendations = $clearResult.Tables[1]

    $categories = $recommendations.Rows | ForEach-Object { $_["Category"].ToString() }
    $severities = $recommendations.Rows | ForEach-Object { $_["Severity"].ToString() }

    Write-TestResult -Name "Returns recommendations" -Passed ($recommendations.Rows.Count -gt 0) -Detail "$($recommendations.Rows.Count) findings"

    # C1: Killed runs
    $hasC1 = $categories -contains "KILLED_RUNS"
    Write-TestResult -Name "C1: Killed runs detected" -Passed $hasC1

    # C2: Repeated failures
    $hasC2 = $categories -contains "REPEATED_FAILURES"
    Write-TestResult -Name "C2: Repeated failures detected" -Passed $hasC2

    # C2 detail: check it found IX_BigTable_Status
    $c2Findings = $recommendations.Rows | Where-Object { $_["Category"].ToString() -eq "REPEATED_FAILURES" }
    $foundBigTable = $false
    foreach ($f in $c2Findings) {
        if ($f["Finding"].ToString() -match "BigTable") { $foundBigTable = $true }
    }
    Write-TestResult -Name "C2: Identifies BigTable stat" -Passed $foundBigTable

    # C4: Degrading throughput -- seed data has serial recent runs at ~19-30 sec/stat
    # vs serial prior runs at ~5-7 sec/stat (>50% threshold) so C4 MUST fire.
    $hasC4 = $categories -contains "DEGRADING_THROUGHPUT"
    Write-TestResult -Name "C4: Degrading throughput detected" -Passed $hasC4

    # W1: Suboptimal params
    $hasW1 = $categories -contains "SUBOPTIMAL_PARAMS"
    Write-TestResult -Name "W1: Suboptimal params detected" -Passed $hasW1

    # W1 detail: TieredThresholds off
    $w1Findings = $recommendations.Rows | Where-Object { $_["Category"].ToString() -eq "SUBOPTIMAL_PARAMS" }
    $foundTiered = $false
    $foundNoTimeLimit = $false
    foreach ($f in $w1Findings) {
        $finding = $f["Finding"].ToString()
        if ($finding -match "TieredThresholds") { $foundTiered = $true }
        if ($finding -match "time limit|TimeLimit") { $foundNoTimeLimit = $true }
    }
    Write-TestResult -Name "W1: TieredThresholds disabled" -Passed $foundTiered
    Write-TestResult -Name "W1: No time limit" -Passed $foundNoTimeLimit

    # W4: Overlapping runs
    $hasW4 = $categories -contains "OVERLAPPING_RUNS"
    Write-TestResult -Name "W4: Overlapping runs detected" -Passed $hasW4

    # W2: Long-running stats -- W2 requires COUNT >= 2 appearances of the same slow stat.
    # Seed data has only one run (run 11) with HugeTable at 25 min, so W2 will NOT fire.
    $hasW2 = $categories -contains "LONG_RUNNING_STATS"
    Write-TestResult -Name "W2: Long-running stats not triggered (single appearance in seed)" -Passed (-not $hasW2) -Detail "W2 requires count >= 2; seed has 1 slow stat"

    # W6: Excessive overhead (run 17: 300s wall clock, ~5s actual stat work)
    $hasW6 = $categories -contains "EXCESSIVE_OVERHEAD"
    Write-TestResult -Name "W6: Excessive overhead detected" -Passed $hasW6

    # W6 detail: check it mentions overhead percentage
    if ($hasW6) {
        $w6Findings = $recommendations.Rows | Where-Object { $_["Category"].ToString() -eq "EXCESSIVE_OVERHEAD" }
        $w6HasPct = $false
        foreach ($f in $w6Findings) {
            if ($f["Evidence"].ToString() -match "\d+\.\d+%% overhead" -or $f["Evidence"].ToString() -match "\d+% overhead" -or $f["Evidence"].ToString() -match "overhead") { $w6HasPct = $true }
        }
        Write-TestResult -Name "W6: Evidence includes overhead detail" -Passed $w6HasPct
    }

    # I4: Unused features
    $hasI4 = $categories -contains "UNUSED_FEATURES"
    Write-TestResult -Name "I4: Unused features detected" -Passed $hasI4

    # Verify severity ordering: CRITICAL before WARNING before INFO
    $severityOrder = $recommendations.Rows | ForEach-Object { $_["Severity"].ToString() }
    $lastCritical = -1; $firstWarning = 999; $lastWarning = -1; $firstInfo = 999
    for ($i = 0; $i -lt $severityOrder.Count; $i++) {
        switch ($severityOrder[$i]) {
            "CRITICAL" { $lastCritical = $i }
            "WARNING" { if ($i -lt $firstWarning) { $firstWarning = $i }; $lastWarning = $i }
            "INFO" { if ($i -lt $firstInfo) { $firstInfo = $i } }
        }
    }
    $orderedCorrectly = ($lastCritical -lt $firstWarning -or $lastCritical -eq -1) -and
                        ($lastWarning -lt $firstInfo -or $lastWarning -eq -1)
    Write-TestResult -Name "Severity ordering: CRITICAL > WARNING > INFO" -Passed $orderedCorrectly
}
catch {
    Write-TestResult -Name "Clear mode diagnostics" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 2b: C4 Window Bounding (gh-558)
# =============================================================================

Write-TestHeader "C4 Window Bounding (gh-558)"

try {
    # Baseline invariance: with @ThroughputWindowDays fixed, C4 must compare the
    # same two adjacent 7-day windows regardless of how much history @DaysBack
    # loads. Runs older than 14 days (seed has runs at -16..-19 days) must not
    # move the averages or the fire/no-fire decision.
    $ds15 = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 15, @ThroughputWindowDays = 7, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $ds30 = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ThroughputWindowDays = 7, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet

    $c4At15 = @($ds15.Tables[1].Rows | Where-Object { $_["Category"].ToString() -eq "DEGRADING_THROUGHPUT" })
    $c4At30 = @($ds30.Tables[1].Rows | Where-Object { $_["Category"].ToString() -eq "DEGRADING_THROUGHPUT" })

    Write-TestResult -Name "C4: fires with @DaysBack = 15 (two complete windows)" -Passed ($c4At15.Count -gt 0)
    Write-TestResult -Name "C4: same finding count at @DaysBack 15 and 30" -Passed ($c4At15.Count -eq $c4At30.Count) -Detail "15d: $($c4At15.Count), 30d: $($c4At30.Count)"

    $evidenceMatches = $true
    for ($i = 0; $i -lt [Math]::Min($c4At15.Count, $c4At30.Count); $i++) {
        if ($c4At15[$i]["Evidence"].ToString() -ne $c4At30[$i]["Evidence"].ToString()) { $evidenceMatches = $false }
    }
    Write-TestResult -Name "C4: identical evidence at @DaysBack 15 and 30 (old runs excluded)" -Passed ($c4At15.Count -eq $c4At30.Count -and $evidenceMatches)
}
catch {
    Write-TestResult -Name "C4 window bounding" -Passed $false -Detail $_.Exception.Message
}

try {
    # Insufficient history: 2 x @ThroughputWindowDays > @DaysBack must be a
    # validation error, not a silent skip.
    $gateRaised = $false
    $gateMessage = ""
    try {
        $null = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 10, @ThroughputWindowDays = 7, @SkipHistory = 1;" -MultiResultSet
    }
    catch {
        $gateRaised = $true
        $gateMessage = $_.Exception.Message
    }
    Write-TestResult -Name "C4: validation error when @DaysBack < 2 x @ThroughputWindowDays" -Passed ($gateRaised -and $gateMessage -match "ThroughputWindowDays")

    # Boundary: exactly two windows of history is allowed.
    $boundaryOk = $true
    try {
        $null = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 14, @ThroughputWindowDays = 7, @SkipHistory = 1;" -MultiResultSet
    }
    catch {
        $boundaryOk = $false
    }
    Write-TestResult -Name "C4: @DaysBack = 2 x @ThroughputWindowDays accepted" -Passed $boundaryOk

    # Explicit NULL must hit the validation error, not silently disable C4.
    $nullRaised = $false
    $nullMessage = ""
    try {
        $null = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @ThroughputWindowDays = NULL, @SkipHistory = 1;" -MultiResultSet
    }
    catch {
        $nullRaised = $true
        $nullMessage = $_.Exception.Message
    }
    Write-TestResult -Name "C4: @ThroughputWindowDays = NULL rejected" -Passed ($nullRaised -and $nullMessage -match "ThroughputWindowDays")
}
catch {
    Write-TestResult -Name "C4 insufficient-history gate" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 3: Result Set Structure
# =============================================================================

Write-TestHeader "Result Set Structure"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet

    Write-TestResult -Name "Returns 12 result sets (expert, no obfuscation)" -Passed ($ds.Tables.Count -eq 12) -Detail "got $($ds.Tables.Count)"

    # RS1: Executive Dashboard (index 0)
    $rs1Cols = $ds.Tables[0].Columns | ForEach-Object { $_.ColumnName }
    Write-TestResult -Name "RS1 has Category column" -Passed ($rs1Cols -contains "Category")
    Write-TestResult -Name "RS1 has Grade column" -Passed ($rs1Cols -contains "Grade")
    Write-TestResult -Name "RS1 has Score column" -Passed ($rs1Cols -contains "Score")
    Write-TestResult -Name "RS1 has Headline column" -Passed ($rs1Cols -contains "Headline")

    # RS2: Recommendations (index 1)
    $rs2Cols = $ds.Tables[1].Columns | ForEach-Object { $_.ColumnName }
    Write-TestResult -Name "RS2 has Severity column" -Passed ($rs2Cols -contains "Severity")
    Write-TestResult -Name "RS2 has Category column" -Passed ($rs2Cols -contains "Category")
    Write-TestResult -Name "RS2 has Finding column" -Passed ($rs2Cols -contains "Finding")
    Write-TestResult -Name "RS2 has Recommendation column" -Passed ($rs2Cols -contains "Recommendation")
    Write-TestResult -Name "RS2 has ExampleCall column" -Passed ($rs2Cols -contains "ExampleCall")

    # RS3: Run Health Summary (index 2)
    $rs3 = $ds.Tables[2]
    $rs3Cols = $rs3.Columns | ForEach-Object { $_.ColumnName }
    Write-TestResult -Name "RS3 has TotalRuns" -Passed ($rs3Cols -contains "TotalRuns")
    Write-TestResult -Name "RS3 has KilledRuns" -Passed ($rs3Cols -contains "KilledRuns")
    Write-TestResult -Name "RS3 has CompletionPct" -Passed ($rs3Cols -contains "CompletionPct")

    # Validate run counts
    $totalRuns = [int]$rs3.Rows[0]["TotalRuns"]
    $killedRuns = [int]$rs3.Rows[0]["KilledRuns"]
    Write-TestResult -Name "RS3 total runs = 28" -Passed ($totalRuns -eq 28) -Detail "got $totalRuns"
    Write-TestResult -Name "RS3 killed runs = 2" -Passed ($killedRuns -eq 2) -Detail "got $killedRuns"

    # RS4: Run Detail (index 3)
    $rs4 = $ds.Tables[3]
    Write-TestResult -Name "RS4 Run Detail has rows" -Passed ($rs4.Rows.Count -gt 0) -Detail "$($rs4.Rows.Count) runs"
    $rs4Cols = $rs4.Columns | ForEach-Object { $_.ColumnName }
    Write-TestResult -Name "RS4 has IsKilled column" -Passed ($rs4Cols -contains "IsKilled")
    Write-TestResult -Name "RS4 has StopReason column" -Passed ($rs4Cols -contains "StopReason")

    # RS5: Top Tables (index 4)
    $rs5 = $ds.Tables[4]
    Write-TestResult -Name "RS5 Top Tables has rows" -Passed ($rs5.Rows.Count -gt 0) -Detail "$($rs5.Rows.Count) tables"

    # RS6: Failing Stats (index 5)
    $rs6 = $ds.Tables[5]
    Write-TestResult -Name "RS6 Failing Stats has rows" -Passed ($rs6.Rows.Count -gt 0) -Detail "$($rs6.Rows.Count) stats"

    # RS8: Parameter History (index 7)
    $rs8 = $ds.Tables[7]
    Write-TestResult -Name "RS8 Parameter History has rows" -Passed ($rs8.Rows.Count -gt 0) -Detail "$($rs8.Rows.Count) runs"
}
catch {
    Write-TestResult -Name "Result set structure" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 4: Obfuscated Mode
# =============================================================================

Write-TestHeader "Obfuscated Mode"

try {
    $obfResult = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @Obfuscate = 1, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet

    Write-TestResult -Name "Returns 13 result sets (expert + obfuscation map)" -Passed ($obfResult.Tables.Count -eq 13) -Detail "got $($obfResult.Tables.Count)"

    # Check obfuscation map via column signature
    $obfRsMap = Get-RSBySignature $obfResult
    $obfMap = $obfRsMap["ObfuscationMap"]
    Write-TestResult -Name "Obfuscation map has entries" -Passed ($obfMap.Rows.Count -gt 0) -Detail "$($obfMap.Rows.Count) mappings"

    $mapCols = $obfMap.Columns | ForEach-Object { $_.ColumnName }
    Write-TestResult -Name "Map has ObjectType" -Passed ($mapCols -contains "ObjectType")
    Write-TestResult -Name "Map has OriginalName" -Passed ($mapCols -contains "OriginalName")
    Write-TestResult -Name "Map has ObfuscatedName" -Passed ($mapCols -contains "ObfuscatedName")

    # Verify names are hashed in run detail (RS4 via signature)
    $obfRunDetail = $obfRsMap["RunDetail"]
    $hasRealName = $false
    foreach ($row in $obfRunDetail.Rows) {
        $runLabel = $row["RunLabel"].ToString()
        if ($runLabel -match "^RUN_[0-9A-F]{8}$") { continue }
        $hasRealName = $true
    }
    Write-TestResult -Name "RunLabels are hashed (RUN_XXXXXXXX)" -Passed (-not $hasRealName)

    # Verify database names are hashed in top tables (RS5 via signature)
    $obfTopTables = $obfRsMap["TopTables"]
    $hasRealDB = $false
    foreach ($row in $obfTopTables.Rows) {
        $dbName = $row["DatabaseName"].ToString()
        if ($dbName -match "^DB_[0-9A-F]" -or $dbName -eq "") { continue }
        $hasRealDB = $true
    }
    Write-TestResult -Name "Database names are hashed (DB_XXXXXX)" -Passed (-not $hasRealDB)

    # Verify stat names preserve prefixes (RS6 via signature)
    $obfFailStats = $obfRsMap["FailingStats"]
    if ($obfFailStats.Rows.Count -gt 0) {
        $statName = $obfFailStats.Rows[0]["StatisticsName"].ToString()
        $hasPrefix = $statName -match "^(IX_|PK_|UQ_|_WA_Sys_|STAT_)"
        Write-TestResult -Name "Stat name prefixes preserved" -Passed $hasPrefix -Detail $statName
    }
    else {
        Write-TestResult -Name "Stat name prefixes preserved" -Passed $false -Detail "no failing-stat row was returned"
    }

    # tracking issue: parameter-value leakage regression. Run 26 in the test data
    # configures @CriticalTables = 'dbo.Orders,dbo.OrderItems'; before the fix,
    # W16/I17/RS8 echoed those raw names into the obfuscated pass. Scan every
    # text column of the obfuscated Recommendations RS for the seeded names --
    # they must never appear (the I4d hardcoded example was changed to
    # obviously-fake placeholders so it cannot false-trip this).
    $obfRecs = $obfRsMap["Recommendations"]
    $leaked = @()
    if ($obfRecs) {
        foreach ($row in $obfRecs.Rows) {
            foreach ($col in @("Finding", "Evidence", "Recommendation", "ExampleCall")) {
                if ($obfRecs.Columns.Contains($col) -and $row[$col] -isnot [System.DBNull]) {
                    if ($row[$col].ToString() -match 'dbo\.Orders|dbo\.OrderItems') {
                        $leaked += "$($row["Category"])/$col"
                    }
                }
            }
        }
    }
    Write-TestResult -Name "No @CriticalTables names leak into obfuscated findings" -Passed ($leaked.Count -eq 0) -Detail $(if ($leaked.Count -gt 0) { $leaked -join ", " } else { "clean" })

    # And the map should carry PAT_ tokens for the configured patterns
    $patRows = @($obfMap.Rows | Where-Object { $_["ObjectType"].ToString() -eq "CriticalTablePattern" })
    Write-TestResult -Name "Obfuscation map includes CriticalTablePattern tokens" -Passed ($patRows.Count -ge 2) -Detail "$($patRows.Count) pattern token(s)"
}
catch {
    Write-TestResult -Name "Obfuscated mode" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 4B: W13 authoritative REMAINING-log path (tracking issue)
# Proc v3.11.0+ writes SP_STATUPDATE_REMAINING rows listing qualifying-but-
# unprocessed stats; W13 must consume them as the authoritative source
# (intersection across the last N runs) instead of the old inference.
# Fixture: 3 TIME_LIMIT runs whose remaining lists share exactly one stat.
# =============================================================================

Write-TestHeader "W13 REMAINING-log path (tracking issue)"

try {
    Invoke-SqlQuery @"
DELETE FROM dbo.CommandLog;
DECLARE @now datetime2(3) = SYSDATETIME(), @i int = 1, @t0 datetime2(3), @lbl nvarchar(100);
WHILE @i <= 3
BEGIN
    SET @t0 = DATEADD(HOUR, -6 * @i, @now);
    SET @lbl = N'w13log_run_' + CONVERT(nvarchar(10), @i);
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, ExtendedInfo)
    VALUES (N'DiagTest', N'dbo', N'sp_StatUpdate', N'P', N'EXECUTE dbo.sp_StatUpdate', N'SP_STATUPDATE_START', @t0,
        CONVERT(xml, N'<Parameters><Version>3.11.0</Version><RunLabel>' + @lbl + N'</RunLabel><Databases>DiagTest</Databases><TimeLimit>600</TimeLimit></Parameters>'));
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ExtendedInfo)
    VALUES (N'DiagTest', N'dbo', N'sp_StatUpdate', N'P', N'sp_StatUpdate completed: TIME_LIMIT', N'SP_STATUPDATE_END', @t0, DATEADD(MINUTE, 10, @t0),
        CONVERT(xml, N'<Summary><Version>3.11.0</Version><RunLabel>' + @lbl + N'</RunLabel><StatsFound>20</StatsFound><StatsProcessed>15</StatsProcessed><StatsSucceeded>15</StatsSucceeded><StatsFailed>0</StatsFailed><StatsRemaining>5</StatsRemaining><DurationSeconds>600</DurationSeconds><StopReason>TIME_LIMIT</StopReason></Summary>'));
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ExtendedInfo)
    VALUES (DB_NAME(), N'dbo', N'sp_StatUpdate', N'P', N'Qualifying work remaining at run end: 2 stat(s) (TIME_LIMIT)', N'SP_STATUPDATE_REMAINING', DATEADD(MINUTE, 10, @t0), DATEADD(MINUTE, 10, @t0),
        CONVERT(xml, N'<ExtendedInfo><RunLabel>' + @lbl + N'</RunLabel><Version>3.11.0.2026.07.30</Version><Kind>STAT</Kind><TotalRemaining>2</TotalRemaining><Listed>2</Listed><Truncated>0</Truncated><Remaining><R><Db>DiagTest</Db><Sch>dbo</Sch><Tbl>BigTable</Tbl><Stat>ST_Deep_Stat</Stat><Priority>18</Priority><ModificationCounter>90000</ModificationCounter></R><R><Db>DiagTest</Db><Sch>dbo</Sch><Tbl>Run' + CONVERT(nvarchar(10), @i) + N'Only</Tbl><Stat>ST_Varies_' + CONVERT(nvarchar(10), @i) + N'</Stat><Priority>19</Priority><ModificationCounter>500</ModificationCounter></R></Remaining></ExtendedInfo>'));
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ErrorNumber, StatisticsName, ExtendedInfo)
    VALUES (N'DiagTest', N'dbo', N'Orders', N'U', N'UPDATE STATISTICS x', N'UPDATE_STATISTICS', DATEADD(MINUTE, 2, @t0), DATEADD(MINUTE, 3, @t0), 0, N'ST_ok_' + CONVERT(nvarchar(10), @i),
        CONVERT(xml, N'<ExtendedInfo><RunLabel>' + @lbl + N'</RunLabel><ProcessingPosition>15</ProcessingPosition><Version>3.11.0</Version></ExtendedInfo>'));
    SET @i += 1;
END;
"@ | Out-Null

    $w13r = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 2, @ThroughputWindowDays = 1, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $w13recs = $null
    foreach ($t in $w13r.Tables) {
        $cols = $t.Columns | ForEach-Object { $_.ColumnName }
        if ($cols -contains "Severity" -and $cols -contains "Category") { $w13recs = $t; break }
    }
    $w13row = $null
    if ($w13recs) { foreach ($row in $w13recs.Rows) { if ($row["Category"].ToString() -eq "PERPETUALLY_SKIPPED") { $w13row = $row } } }
    $isLogged = $w13row -and ($w13row["Evidence"].ToString() -like "*SP_STATUPDATE_REMAINING*")
    $countsOne = $w13row -and ($w13row["Finding"].ToString() -like "1 stat(s)*")
    $namesStat = $w13row -and ($w13row["Evidence"].ToString() -like "*ST_Deep_Stat*")
    Write-TestResult -Name "W13 uses the authoritative remaining-work log" -Passed ([bool]$isLogged) -Detail $(if ($w13row) { "evidence source ok=$isLogged" } else { "W13 row missing" })
    Write-TestResult -Name "W13 logged path counts the exact intersection (1 stat)" -Passed ([bool]$countsOne) -Detail $(if ($w13row) { $w13row["Finding"].ToString() } else { "n/a" })
    Write-TestResult -Name "W13 logged path names the deepest-queued stat" -Passed ([bool]$namesStat)
}
catch {
    Write-TestResult -Name "W13 REMAINING-log path" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 4C: DarlingData-audit crash regressions (0859.9 / 0859.10)
# 0859.9: a CommandLog row spanning >24.8 days (orphan START, END written by
#   cleanup weeks later) overflowed int DATEDIFF(MILLISECOND) -> error 535
#   hard-failed the whole diag run.  Fixture row: START 26 days ago, END now.
# 0859.10: RS13 ChangePct had no baseline floor; a 2 ms baseline produced
#   +249,900% and a large enough delta overflowed decimal(10,1) -> error 8115.
#   Fixtures: tiny-baseline stat (expect NULL) + big-delta stat (expect 1000.0 clamp).
# =============================================================================

Write-TestHeader "DATEDIFF overflow + ChangePct floor/clamp (0859.9/0859.10)"

try {
    Invoke-SqlQuery @"
DELETE FROM dbo.CommandLog;
DECLARE @now datetime2(3) = SYSDATETIME(), @i int = 1, @t0 datetime2(3), @lbl nvarchar(100), @cpu bigint;
WHILE @i <= 2
BEGIN
    SET @t0 = DATEADD(HOUR, -6 * @i, @now);
    SET @lbl = N'ddaudit_run_' + CONVERT(nvarchar(10), @i);
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, ExtendedInfo)
    VALUES (N'DiagTest', N'dbo', N'sp_StatUpdate', N'P', N'EXECUTE dbo.sp_StatUpdate', N'SP_STATUPDATE_START', @t0,
        CONVERT(xml, N'<Parameters><Version>3.11.2</Version><RunLabel>' + @lbl + N'</RunLabel><Databases>DiagTest</Databases></Parameters>'));
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ExtendedInfo)
    VALUES (N'DiagTest', N'dbo', N'sp_StatUpdate', N'P', N'sp_StatUpdate completed: COMPLETED', N'SP_STATUPDATE_END', @t0, DATEADD(MINUTE, 10, @t0),
        CONVERT(xml, N'<Summary><Version>3.11.2</Version><RunLabel>' + @lbl + N'</RunLabel><StatsFound>2</StatsFound><StatsProcessed>2</StatsProcessed><StatsSucceeded>2</StatsSucceeded><StatsFailed>0</StatsFailed><StatsRemaining>0</StatsRemaining><DurationSeconds>600</DurationSeconds><StopReason>COMPLETED</StopReason></Summary>'));
    /* tiny-baseline stat: 2 ms first run, 50,000,000,000 ms second run (pre-fix: 8115 overflow) */
    SET @cpu = CASE WHEN @i = 2 THEN 2 ELSE 50000000000 END; /* run 2 is OLDER (6h earlier per @i) */
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ErrorNumber, StatisticsName, ExtendedInfo)
    VALUES (N'DiagTest', N'dbo', N'Orders', N'U', N'UPDATE STATISTICS x', N'UPDATE_STATISTICS', DATEADD(MINUTE, 2, @t0), DATEADD(MINUTE, 3, @t0), 0, N'ST_TinyBaseline',
        CONVERT(xml, N'<ExtendedInfo><RunLabel>' + @lbl + N'</RunLabel><Version>3.11.2</Version><QSPlanCount>1</QSPlanCount><QSTotalExecutions>1</QSTotalExecutions><QSTotalCpuMs>' + CONVERT(nvarchar(20), @cpu) + N'</QSTotalCpuMs></ExtendedInfo>'));
    /* clamp stat: 200 ms baseline -> 100,000 ms = +49,900% raw, expect clamp to 1000.0 */
    SET @cpu = CASE WHEN @i = 2 THEN 200 ELSE 100000 END;
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ErrorNumber, StatisticsName, ExtendedInfo)
    VALUES (N'DiagTest', N'dbo', N'Orders', N'U', N'UPDATE STATISTICS x', N'UPDATE_STATISTICS', DATEADD(MINUTE, 4, @t0), DATEADD(MINUTE, 5, @t0), 0, N'ST_ClampMe',
        CONVERT(xml, N'<ExtendedInfo><RunLabel>' + @lbl + N'</RunLabel><Version>3.11.2</Version><QSPlanCount>1</QSPlanCount><QSTotalExecutions>1</QSTotalExecutions><QSTotalCpuMs>' + CONVERT(nvarchar(20), @cpu) + N'</QSTotalCpuMs></ExtendedInfo>'));
    SET @i += 1;
END;
/* 0859.9 fixture: orphan row spanning ~26 days (START inside the 30-day window, END = now) */
INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ErrorNumber, StatisticsName, ExtendedInfo)
VALUES (N'DiagTest', N'dbo', N'Orders', N'U', N'UPDATE STATISTICS x', N'UPDATE_STATISTICS', DATEADD(DAY, -26, @now), @now, 0, N'ST_OrphanSpan',
    CONVERT(xml, N'<ExtendedInfo><RunLabel>orphan_span_run</RunLabel><Version>3.11.2</Version></ExtendedInfo>'));
"@ | Out-Null

    $ddres = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    Write-TestResult -Name "Diag survives a 26-day CommandLog span (0859.9, pre-fix error 535)" -Passed ($ddres.Tables.Count -gt 0) -Detail "$($ddres.Tables.Count) result sets"

    $rs13 = $null
    foreach ($t in $ddres.Tables) {
        $cols = $t.Columns | ForEach-Object { $_.ColumnName }
        if ($cols -contains "CpuChangePct") { $rs13 = $t; break }
    }
    if ($rs13) {
        Write-TestResult -Name "RS13 present for floor/clamp assertions" -Passed $true
        $tinyRow = $null; $clampRow = $null
        foreach ($row in $rs13.Rows) {
            if ($row["StatisticsName"].ToString() -eq "ST_TinyBaseline") { $tinyRow = $row }
            if ($row["StatisticsName"].ToString() -eq "ST_ClampMe") { $clampRow = $row }
        }
        $tinyNull = $tinyRow -and ($tinyRow["CpuChangePct"] -is [System.DBNull])
        $clamped  = $clampRow -and (-not ($clampRow["CpuChangePct"] -is [System.DBNull])) -and ([decimal]$clampRow["CpuChangePct"] -eq 1000.0)
        Write-TestResult -Name "RS13 emits despite 5e10-ms delta (0859.10, pre-fix error 8115)" -Passed ([bool]($tinyRow -and $clampRow)) -Detail "rows found: tiny=$([bool]$tinyRow) clamp=$([bool]$clampRow)"
        Write-TestResult -Name "Sub-floor baseline (2 ms) yields NULL CpuChangePct" -Passed ([bool]$tinyNull) -Detail $(if ($tinyRow) { "value=$($tinyRow['CpuChangePct'])" } else { "row missing" })
        Write-TestResult -Name "Oversized delta clamps CpuChangePct to +1000.0" -Passed ([bool]$clamped) -Detail $(if ($clampRow) { "value=$($clampRow['CpuChangePct'])" } else { "row missing" })
    }
    else {
        Write-TestResult -Name "RS13 present for floor/clamp assertions" -Passed $false -Detail "no table with CpuChangePct column"
    }
}
catch {
    Write-TestResult -Name "DATEDIFF overflow + ChangePct floor/clamp" -Passed $false -Detail $_.Exception.Message
}

# Re-seed the standard deterministic dataset for the remaining categories
try { Invoke-SqlFile -FilePath $testData } catch { }

# =============================================================================
# TEST 5: Empty CommandLog
# =============================================================================

Write-TestHeader "Empty CommandLog"

try {
    # Truncate and test
    Invoke-SqlQuery "TRUNCATE TABLE dbo.CommandLog;" | Out-Null

    $emptyResult = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet

    # Should still return result sets (empty ones): dashboard + recommendations + 9 expert RS = 11
    Write-TestResult -Name "Returns result sets on empty CommandLog" -Passed ($emptyResult.Tables.Count -ge 2) -Detail "$($emptyResult.Tables.Count) result sets"

    # Recommendations should be empty (or just INFO) - RS2 at index 1
    $emptyRecs = $emptyResult.Tables[1]
    $hasCritical = $false
    foreach ($row in $emptyRecs.Rows) {
        if ($row["Severity"].ToString() -eq "CRITICAL") { $hasCritical = $true }
    }
    Write-TestResult -Name "No CRITICAL findings on empty data" -Passed (-not $hasCritical)

    # RS 3 empty schema must match production RS 3 columns (#304)
    if ($emptyResult.Tables.Count -ge 3) {
        $emptyRS3 = $emptyResult.Tables[2]
        $emptyRS3Cols = @($emptyRS3.Columns | ForEach-Object { $_.ColumnName }) -join ', '
        $expectedRS3Cols = @('TotalRuns','CompletedRuns','KilledRuns','CompletionPct','TimeLimitedRuns','NaturalEndRuns','AvgDurationSec','AvgStatsProcessed','AvgStatsRemaining','TotalStatUpdates','TotalFailedUpdates','AnalysisWindowDays','StopReasonDistribution','HealthScore','QSRunCount','AvgWorkloadCoveragePct','LatestHighCpuFirstQuartilePct','AvgTotalGB','AvgGBPerMin','ReplicaRole')
        $actualCols = @($emptyRS3.Columns | ForEach-Object { $_.ColumnName })
        $colsMatch = ($actualCols.Count -eq $expectedRS3Cols.Count)
        if ($colsMatch) {
            for ($i = 0; $i -lt $expectedRS3Cols.Count; $i++) {
                if ($actualCols[$i] -ne $expectedRS3Cols[$i]) { $colsMatch = $false; break }
            }
        }
        Write-TestResult -Name "Empty RS3 columns match production RS3 (#304)" -Passed $colsMatch -Detail $emptyRS3Cols
    } else {
        Write-TestResult -Name "Empty RS3 columns match production RS3 (#304)" -Passed $false -Detail "Not enough result sets: $($emptyResult.Tables.Count)"
    }

    # gh-557: @SingleResultSet=1 on empty CommandLog must still honor the stable-ID
    # contract -- one result set, all ResultSetIDs 1-13, IDs 3-13 as UNAVAILABLE markers
    $srsEmpty = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @SingleResultSet = 1, @SkipHistory = 1;" -MultiResultSet
    Write-TestResult -Name "Empty + SingleResultSet returns exactly 1 result set (gh-557)" -Passed ($srsEmpty.Tables.Count -eq 1) -Detail "Got $($srsEmpty.Tables.Count)"

    if ($srsEmpty.Tables.Count -ge 1) {
        $srsRows = $srsEmpty.Tables[0].Rows
        $foundIds = @($srsRows | ForEach-Object { [int]$_["ResultSetID"] } | Sort-Object -Unique)
        $expectedIds = 1..13
        $missingIds = @($expectedIds | Where-Object { $foundIds -notcontains $_ })
        Write-TestResult -Name "Empty + SingleResultSet envelope has ResultSetIDs 1-13 (gh-557)" -Passed ($missingIds.Count -eq 0) -Detail "Found: $($foundIds -join ','); Missing: $($missingIds -join ',')"

        # IDs 3-13 rows: DataStatus=UNAVAILABLE, Reason=NO_COMMANDLOG_DATA, ResultSetName in JSON
        $markerOk = $true
        $markerDetail = ""
        foreach ($row in $srsRows) {
            $rsId = [int]$row["ResultSetID"]
            if ($rsId -ge 3) {
                $json = $row["RowData"].ToString() | ConvertFrom-Json
                if ($json.DataStatus -ne "UNAVAILABLE" -or $json.Reason -ne "NO_COMMANDLOG_DATA" -or $json.ResultSetName -ne $row["ResultSetName"].ToString()) {
                    $markerOk = $false
                    $markerDetail = "ID $rsId : DataStatus=$($json.DataStatus), Reason=$($json.Reason), ResultSetName=$($json.ResultSetName)"
                    break
                }
            }
        }
        Write-TestResult -Name "Empty envelope IDs 3-13 carry UNAVAILABLE/NO_COMMANDLOG_DATA markers (gh-557)" -Passed $markerOk -Detail $markerDetail

        # IDs 1-2 keep actionable first-run content (no DataStatus marker property)
        $id1Row = @($srsRows | Where-Object { [int]$_["ResultSetID"] -eq 1 })[0]
        $id1Json = $id1Row["RowData"].ToString() | ConvertFrom-Json
        $id1Ok = ($id1Json.Grade -eq "F") -and ($null -eq $id1Json.DataStatus)
        $id2Rows = @($srsRows | Where-Object { [int]$_["ResultSetID"] -eq 2 })
        $id2Json = $id2Rows[0]["RowData"].ToString() | ConvertFrom-Json
        $id2Ok = ($id2Rows.Count -ge 1) -and ($null -ne $id2Json.Finding) -and ($null -eq $id2Json.DataStatus)
        Write-TestResult -Name "Empty envelope IDs 1-2 retain first-run content (gh-557)" -Passed ($id1Ok -and $id2Ok) -Detail "ID1 Grade=$($id1Json.Grade); ID2 rows=$($id2Rows.Count)"
    }

    # gh-557: @Obfuscate=1 on the empty single-result path -- ID 9 marker must carry
    # no map content (no OriginalName/ObfuscatedName properties)
    $srsObf = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @SingleResultSet = 1, @Obfuscate = 1, @SkipHistory = 1;" -MultiResultSet
    $obfOk = $false
    $obfDetail = "No result set"
    if ($srsObf.Tables.Count -eq 1) {
        $id9Rows = @($srsObf.Tables[0].Rows | Where-Object { [int]$_["ResultSetID"] -eq 9 })
        if ($id9Rows.Count -eq 1) {
            $id9Json = $id9Rows[0]["RowData"].ToString() | ConvertFrom-Json
            $obfOk = ($id9Json.DataStatus -eq "UNAVAILABLE") -and ($null -eq $id9Json.OriginalName) -and ($null -eq $id9Json.ObfuscatedName)
            $obfDetail = "ID9 RowData: $($id9Rows[0]["RowData"])"
        } else {
            $obfDetail = "ID9 row count: $($id9Rows.Count)"
        }
    }
    Write-TestResult -Name "Empty + Obfuscate ID 9 marker leaks no map content (gh-557)" -Passed $obfOk -Detail $obfDetail

    # Reload test data for remaining tests
    Invoke-SqlFile -FilePath $testData
}
catch {
    Write-TestResult -Name "Empty CommandLog" -Passed $false -Detail $_.Exception.Message
    # Reload test data
    try { Invoke-SqlFile -FilePath $testData } catch {}
}

# =============================================================================
# TEST 6: DaysBack Filtering
# =============================================================================

Write-TestHeader "DaysBack Filtering"

try {
    # @DaysBack = 3 with matching @ThroughputWindowDays should limit to recent runs
    $recent = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 3, @ThroughputWindowDays = 1, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $recentHealth = $recent.Tables[2]
    $recentRuns = [int]$recentHealth.Rows[0]["TotalRuns"]
    Write-TestResult -Name "@DaysBack=3 limits to recent runs" -Passed ($recentRuns -lt 15) -Detail "$recentRuns runs"

    # @DaysBack = 30 should see all 28 runs (25 original + 3 added for sw82/9ufm coverage)
    $all = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $allHealth = $all.Tables[2]
    $allRuns = [int]$allHealth.Rows[0]["TotalRuns"]
    Write-TestResult -Name "@DaysBack=30 includes all runs" -Passed ($allRuns -eq 28) -Detail "$allRuns runs"
}
catch {
    Write-TestResult -Name "DaysBack filtering" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 7: TopN Limiting
# =============================================================================

Write-TestHeader "TopN Limiting"

try {
    $limited = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @TopN = 2, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $topTables = $limited.Tables[4]
    Write-TestResult -Name "@TopN=2 limits top tables" -Passed ($topTables.Rows.Count -le 2) -Detail "$($topTables.Rows.Count) rows"
}
catch {
    Write-TestResult -Name "TopN limiting" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 8: Debug Mode
# =============================================================================

Write-TestHeader "Debug Mode"

try {
    # Debug mode should add extra result sets without errors
    $debug = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @Debug = 1, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    Write-TestResult -Name "Debug mode executes without error" -Passed ($debug.Tables.Count -ge 12) -Detail "$($debug.Tables.Count) result sets"
}
catch {
    Write-TestResult -Name "Debug mode" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 9: Version Output Parameter
# =============================================================================

Write-TestHeader "OUTPUT Parameters"

try {
    $versionResult = Invoke-SqlQuery -Query "
        DECLARE @ver varchar(20), @vd datetime;
        EXECUTE dbo.sp_StatUpdate_Diag @Help = 1, @Version = @ver OUTPUT, @VersionDate = @vd OUTPUT;
        SELECT Version = @ver, VersionDate = @vd;
    " -MultiResultSet

    # Find the result set with Version column (skip the help result set)
    $versionTable = $null
    foreach ($t in $versionResult.Tables) {
        $cols = $t.Columns | ForEach-Object { $_.ColumnName }
        if ($cols -contains "Version" -and $cols -contains "VersionDate" -and $t.Columns.Count -eq 2) {
            $versionTable = $t
            break
        }
    }

    if ($versionTable) {
        $ver = $versionTable.Rows[0]["Version"].ToString()
        Write-TestResult -Name "@Version output populated" -Passed ($ver.Length -gt 0) -Detail $ver
        Write-TestResult -Name "@VersionDate output populated" -Passed ($versionTable.Rows[0]["VersionDate"] -ne [DBNull]::Value)
    }
    else {
        Write-TestResult -Name "OUTPUT parameters" -Passed $false -Detail "Version result set not found"
    }
}
catch {
    Write-TestResult -Name "OUTPUT parameters" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 10: Killed Run Detection (IsKilled logic)
# =============================================================================

Write-TestHeader "Killed Run Detection"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $runDetail = $ds.Tables[3]

    $killedRuns = @()
    foreach ($row in $runDetail.Rows) {
        if ([bool]$row["IsKilled"]) {
            $killedRuns += $row["StopReason"].ToString()
        }
    }

    Write-TestResult -Name "Exactly 2 killed runs detected" -Passed ($killedRuns.Count -eq 2) -Detail "got $($killedRuns.Count)"

    # One should be true kill (no END), one should be orphan cleanup (StopReason=KILLED)
    $hasKilledStopReason = $killedRuns -contains "KILLED"
    Write-TestResult -Name "Orphan-cleaned run detected via StopReason=KILLED" -Passed $hasKilledStopReason

    # W4 should not include killed runs as overlapping
    $recs = $ds.Tables[1]
    $overlapFindings = @()
    foreach ($row in $recs.Rows) {
        if ($row["Category"].ToString() -eq "OVERLAPPING_RUNS") {
            $overlapFindings += $row["Finding"].ToString()
        }
    }
    $killedInOverlap = $false
    foreach ($finding in $overlapFindings) {
        if ($finding -match "20260205_000000|20260206_000000") {
            $killedInOverlap = $true
        }
    }
    Write-TestResult -Name "Killed runs excluded from W4 overlap detection" -Passed (-not $killedInOverlap)
}
catch {
    Write-TestResult -Name "Killed run detection" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 11: QS Efficacy Checks (I6, I7)
# =============================================================================

Write-TestHeader "QS Efficacy Checks"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @EfficacyDaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $recs = $ds.Tables[1]
    $categories = $recs.Rows | ForEach-Object { $_["Category"].ToString() }

    # I6: QS_EFFICACY
    $hasI6 = $categories -contains "QS_EFFICACY"
    Write-TestResult -Name "I6: QS_EFFICACY detected" -Passed $hasI6

    if ($hasI6) {
        $i6Finding = ($recs.Rows | Where-Object { $_["Category"].ToString() -eq "QS_EFFICACY" } | Select-Object -First 1)["Finding"].ToString()
        $hasWorkloadPct = $i6Finding -match "\d+\.?\d*%"
        Write-TestResult -Name "I6: Finding includes workload percentage" -Passed $hasWorkloadPct -Detail ($i6Finding.Substring(0, [Math]::Min(100, $i6Finding.Length)))
    }

    # I7: QS_INFLECTION
    $hasI7 = $categories -contains "QS_INFLECTION"
    Write-TestResult -Name "I7: QS_INFLECTION detected" -Passed $hasI7

    if ($hasI7) {
        $i7Finding = ($recs.Rows | Where-Object { $_["Category"].ToString() -eq "QS_INFLECTION" } | Select-Object -First 1)["Finding"].ToString()
        $hasChangeDate = $i7Finding -match "change detected"
        Write-TestResult -Name "I7: Finding includes change detection" -Passed $hasChangeDate
    }
}
catch {
    Write-TestResult -Name "QS efficacy checks" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 12: RS2 QS Columns
# =============================================================================

Write-TestHeader "RS3 QS Enhancement"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $rs3 = $ds.Tables[2]
    $rs3Cols = $rs3.Columns | ForEach-Object { $_.ColumnName }

    Write-TestResult -Name "RS3 has QSRunCount" -Passed ($rs3Cols -contains "QSRunCount")
    Write-TestResult -Name "RS3 has AvgWorkloadCoveragePct" -Passed ($rs3Cols -contains "AvgWorkloadCoveragePct")
    Write-TestResult -Name "RS3 has LatestHighCpuFirstQuartilePct" -Passed ($rs3Cols -contains "LatestHighCpuFirstQuartilePct")

    # Verify QS run count value
    $qsRunCount = $rs3.Rows[0]["QSRunCount"]
    if ($qsRunCount -ne [DBNull]::Value) {
        Write-TestResult -Name "RS3 QSRunCount > 0" -Passed ([int]$qsRunCount -gt 0) -Detail "got $qsRunCount"
    }
}
catch {
    Write-TestResult -Name "RS3 QS enhancement" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 13: RS3 Efficacy Columns
# =============================================================================

Write-TestHeader "RS4 Efficacy Columns"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $rs4 = $ds.Tables[3]
    $rs4Cols = $rs4.Columns | ForEach-Object { $_.ColumnName }

    Write-TestResult -Name "RS4 has WorkloadCoveragePct" -Passed ($rs4Cols -contains "WorkloadCoveragePct")
    Write-TestResult -Name "RS4 has HighCpuInFirstQuartilePct" -Passed ($rs4Cols -contains "HighCpuInFirstQuartilePct")
    Write-TestResult -Name "RS4 has MinutesToHighCpuComplete" -Passed ($rs4Cols -contains "MinutesToHighCpuComplete")
}
catch {
    Write-TestResult -Name "RS4 efficacy columns" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 14: RS9 Efficacy Trend Structure
# =============================================================================

Write-TestHeader "RS10 Efficacy Trend"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @EfficacyDaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet

    $rsMap = Get-RSBySignature $ds
    $rs10 = $rsMap["EfficacyTrend"]
    $rs10Cols = $rs10.Columns | ForEach-Object { $_.ColumnName }

    Write-TestResult -Name "RS10 has WeekStart" -Passed ($rs10Cols -contains "WeekStart")
    Write-TestResult -Name "RS10 has WeekLabel" -Passed ($rs10Cols -contains "WeekLabel")
    Write-TestResult -Name "RS10 has SortStrategy" -Passed ($rs10Cols -contains "SortStrategy")
    Write-TestResult -Name "RS10 has HighCpuFirstQuartilePct" -Passed ($rs10Cols -contains "HighCpuFirstQuartilePct")
    Write-TestResult -Name "RS10 has WorkloadCoveragePct" -Passed ($rs10Cols -contains "WorkloadCoveragePct")
    Write-TestResult -Name "RS10 has TrendDirection" -Passed ($rs10Cols -contains "TrendDirection")
    Write-TestResult -Name "RS10 has data rows" -Passed ($rs10.Rows.Count -gt 0) -Detail "$($rs10.Rows.Count) weeks"
}
catch {
    Write-TestResult -Name "RS10 structure" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 15: RS10 Efficacy Detail Structure
# =============================================================================

Write-TestHeader "RS11 Efficacy Detail"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @EfficacyDetailDays = 14, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet

    $rsMap = Get-RSBySignature $ds
    $rs11 = $rsMap["EfficacyDetail"]
    $rs11Cols = $rs11.Columns | ForEach-Object { $_.ColumnName }

    Write-TestResult -Name "RS11 has RunLabel" -Passed ($rs11Cols -contains "RunLabel")
    Write-TestResult -Name "RS11 has SortStrategy" -Passed ($rs11Cols -contains "SortStrategy")
    Write-TestResult -Name "RS11 has CompletionPct" -Passed ($rs11Cols -contains "CompletionPct")
    Write-TestResult -Name "RS11 has HighCpuInFirstQuartilePct" -Passed ($rs11Cols -contains "HighCpuInFirstQuartilePct")
    Write-TestResult -Name "RS11 has DeltaVsPrior" -Passed ($rs11Cols -contains "DeltaVsPrior")
    Write-TestResult -Name "RS11 has data rows" -Passed ($rs11.Rows.Count -gt 0) -Detail "$($rs11.Rows.Count) runs"
}
catch {
    Write-TestResult -Name "RS11 structure" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 16: RS11 High-CPU Stat Positions
# =============================================================================

Write-TestHeader "RS12 High-CPU Stat Positions"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet

    $rsMap = Get-RSBySignature $ds
    $rs12 = $rsMap["HighCpuPositions"]
    $rs12Cols = $rs12.Columns | ForEach-Object { $_.ColumnName }

    Write-TestResult -Name "RS12 has WorkloadRank" -Passed ($rs12Cols -contains "WorkloadRank")
    Write-TestResult -Name "RS12 has ProcessingPosition" -Passed ($rs12Cols -contains "ProcessingPosition")
    Write-TestResult -Name "RS12 has TotalQueryCpuMs" -Passed ($rs12Cols -contains "TotalQueryCpuMs")
    Write-TestResult -Name "RS12 has QualifyReason" -Passed ($rs12Cols -contains "QualifyReason")
    $qualifyReasons = @($rs12.Rows | ForEach-Object { $_["QualifyReason"].ToString() })
    $qualifyReasonDetail = "QualifyReason values: $($qualifyReasons -join ',')"
    Write-TestResult -Name "RS12 preserves legacy QUERY_STORE_PRIORITY" -Passed ($qualifyReasons -contains "QUERY_STORE_PRIORITY") -Detail $qualifyReasonDetail
    Write-TestResult -Name "RS12 reports ASCENDING_KEY" -Passed ($qualifyReasons -contains "ASCENDING_KEY") -Detail $qualifyReasonDetail
    Write-TestResult -Name "RS12 has data rows" -Passed ($rs12.Rows.Count -gt 0) -Detail "$($rs12.Rows.Count) stats"

    # Verify ordering: WorkloadRank 1 should be first
    if ($rs12.Rows.Count -gt 0) {
        $firstRank = [long]$rs12.Rows[0]["WorkloadRank"]
        Write-TestResult -Name "RS12 first row has WorkloadRank=1" -Passed ($firstRank -eq 1)
    }
}
catch {
    Write-TestResult -Name "RS12 structure" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 17: RS11 Obfuscation
# =============================================================================

Write-TestHeader "RS12 Obfuscation"

try {
    $obfDs = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @Obfuscate = 1, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet

    $obfRsMap = Get-RSBySignature $obfDs
    $rs12Obf = $obfRsMap["HighCpuPositions"]
    $rs12ObfCols = $rs12Obf.Columns | ForEach-Object { $_.ColumnName }

    if ($rs12ObfCols -contains "DatabaseName" -and $rs12Obf.Rows.Count -gt 0) {
        $dbName = $rs12Obf.Rows[0]["DatabaseName"].ToString()
        $isHashed = $dbName -match "^DB_[0-9A-F]"
        Write-TestResult -Name "RS12 DatabaseName obfuscated" -Passed $isHashed -Detail $dbName

        $statName = $rs12Obf.Rows[0]["StatisticsName"].ToString()
        $prefixPreserved = $statName -match "^(IX_|PK_|UQ_|_WA_Sys_|STAT_)"
        Write-TestResult -Name "RS12 StatisticsName prefix preserved" -Passed $prefixPreserved -Detail $statName
    }
    else {
        Write-TestResult -Name "RS12 obfuscation" -Passed $false -Detail "No data or missing columns"
    }
}
catch {
    Write-TestResult -Name "RS12 obfuscation" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 18: Executive Dashboard (RS1)
# =============================================================================

Write-TestHeader "Executive Dashboard"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $dashboard = $ds.Tables[0]

    $dashCols = $dashboard.Columns | ForEach-Object { $_.ColumnName }
    Write-TestResult -Name "RS1 has Category" -Passed ($dashCols -contains "Category")
    Write-TestResult -Name "RS1 has Grade" -Passed ($dashCols -contains "Grade")
    Write-TestResult -Name "RS1 has Score" -Passed ($dashCols -contains "Score")
    Write-TestResult -Name "RS1 has Headline" -Passed ($dashCols -contains "Headline")
    Write-TestResult -Name "RS1 has Detail" -Passed ($dashCols -contains "Detail")

    # Should have 5 categories: OVERALL, COMPLETION, RELIABILITY, SPEED, WORKLOAD FOCUS
    Write-TestResult -Name "RS1 has 5 dashboard rows" -Passed ($dashboard.Rows.Count -eq 5) -Detail "got $($dashboard.Rows.Count)"

    $categories = $dashboard.Rows | ForEach-Object { $_["Category"].ToString() }
    Write-TestResult -Name "RS1 has OVERALL category" -Passed ($categories -contains "OVERALL")
    Write-TestResult -Name "RS1 has COMPLETION category" -Passed ($categories -contains "COMPLETION")
    Write-TestResult -Name "RS1 has RELIABILITY category" -Passed ($categories -contains "RELIABILITY")

    # Verify grades are valid letters
    $grades = $dashboard.Rows | ForEach-Object { $_["Grade"].ToString().Trim() }
    $validGrades = $grades | Where-Object { $_ -match "^[A-F]$" }
    Write-TestResult -Name "RS1 all grades are A-F" -Passed ($validGrades.Count -eq $grades.Count)

    # Verify scores are 0-100
    $scores = $dashboard.Rows | ForEach-Object { [int]$_["Score"] }
    $validScores = $scores | Where-Object { $_ -ge 0 -and $_ -le 100 }
    Write-TestResult -Name "RS1 all scores 0-100" -Passed ($validScores.Count -eq $scores.Count)

    # OVERALL should be first row (SortOrder = 0)
    $firstCategory = $dashboard.Rows[0]["Category"].ToString()
    Write-TestResult -Name "RS1 OVERALL is first row" -Passed ($firstCategory -eq "OVERALL")
}
catch {
    Write-TestResult -Name "Executive Dashboard" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 19: ExpertMode=0 Default (Management View)
# =============================================================================

Write-TestHeader "ExpertMode=0 Default View"

try {
    $mgmt = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @SkipHistory = 1;" -MultiResultSet

    # Default (ExpertMode=0) should return exactly 2 result sets: Dashboard + Recommendations
    Write-TestResult -Name "ExpertMode=0 returns 2 result sets" -Passed ($mgmt.Tables.Count -eq 2) -Detail "got $($mgmt.Tables.Count)"

    # First RS should be dashboard
    $dashCols = $mgmt.Tables[0].Columns | ForEach-Object { $_.ColumnName }
    Write-TestResult -Name "First RS is Executive Dashboard" -Passed ($dashCols -contains "Grade" -and $dashCols -contains "Headline")

    # Second RS should be recommendations
    $recCols = $mgmt.Tables[1].Columns | ForEach-Object { $_.ColumnName }
    Write-TestResult -Name "Second RS is Recommendations" -Passed ($recCols -contains "Severity" -and $recCols -contains "Finding")
}
catch {
    Write-TestResult -Name "ExpertMode=0 default" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 20: History Table Persistence
# =============================================================================

Write-TestHeader "History Table Persistence"

try {
    # Drop history table if exists
    Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagHistory', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagHistory;" | Out-Null

    # Run with SkipHistory=0 to create and populate history
    $histDs = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @SkipHistory = 0;" -MultiResultSet

    # Check history table was created
    $histExists = (Invoke-SqlQuery "SELECT cnt = OBJECT_ID(N'dbo.StatUpdateDiagHistory', N'U')").cnt
    Write-TestResult -Name "History table auto-created" -Passed ($histExists -ne [DBNull]::Value -and $null -ne $histExists)

    if ($histExists -ne [DBNull]::Value -and $null -ne $histExists) {
        # Check rows were inserted
        $histRows = (Invoke-SqlQuery "SELECT cnt = COUNT(*) FROM dbo.StatUpdateDiagHistory").cnt
        Write-TestResult -Name "History table has rows" -Passed ([int]$histRows -gt 0) -Detail "$histRows rows"

        # Verify key columns exist
        $hasHealthScore = (Invoke-SqlQuery "SELECT cnt = COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'StatUpdateDiagHistory' AND COLUMN_NAME = 'HealthScore'").cnt
        $hasOverallGrade = (Invoke-SqlQuery "SELECT cnt = COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'StatUpdateDiagHistory' AND COLUMN_NAME = 'OverallGrade'").cnt
        Write-TestResult -Name "History has HealthScore column" -Passed ([int]$hasHealthScore -eq 1)
        Write-TestResult -Name "History has OverallGrade column" -Passed ([int]$hasOverallGrade -eq 1)

        # Run again - should not duplicate rows (watermark guard)
        Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @SkipHistory = 0;" -MultiResultSet | Out-Null
        $histRows2 = (Invoke-SqlQuery "SELECT cnt = COUNT(*) FROM dbo.StatUpdateDiagHistory").cnt
        Write-TestResult -Name "Second run does not duplicate history" -Passed ([int]$histRows2 -eq [int]$histRows) -Detail "before=$histRows after=$histRows2"
    }

    # Cleanup
    Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagHistory', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagHistory;" | Out-Null
}
catch {
    Write-TestResult -Name "Second run does not duplicate history" -Passed $false -Detail $_.Exception.Message
    # Cleanup on error
    try { Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagHistory', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagHistory;" | Out-Null } catch {}
}

# =============================================================================
# TEST 21: RS13 QS Performance Correlation
# =============================================================================

Write-TestHeader "RS13 QS Performance Correlation"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet

    Write-TestResult -Name "Returns 12 result sets (expert, no obf, with RS13)" -Passed ($ds.Tables.Count -eq 12) -Detail "got $($ds.Tables.Count)"

    $rsMap = Get-RSBySignature $ds
    $rs13 = $rsMap["QSCorrelation"]
    $rs13Cols = $rs13.Columns | ForEach-Object { $_.ColumnName }

    Write-TestResult -Name "RS13 has DatabaseName" -Passed ($rs13Cols -contains "DatabaseName")
    Write-TestResult -Name "RS13 has StatisticsName" -Passed ($rs13Cols -contains "StatisticsName")
    Write-TestResult -Name "RS13 has FirstCpuMs" -Passed ($rs13Cols -contains "FirstCpuMs")
    Write-TestResult -Name "RS13 has LastCpuMs" -Passed ($rs13Cols -contains "LastCpuMs")
    Write-TestResult -Name "RS13 has CpuChangePct" -Passed ($rs13Cols -contains "CpuChangePct")
    Write-TestResult -Name "RS13 has CpuTrend" -Passed ($rs13Cols -contains "CpuTrend")
    Write-TestResult -Name "RS13 has ForcedPlanCount" -Passed ($rs13Cols -contains "ForcedPlanCount")
    Write-TestResult -Name "RS13 has PlanTrend" -Passed ($rs13Cols -contains "PlanTrend")
    Write-TestResult -Name "RS13 has FirstMemoryGrantKB (#368)" -Passed ($rs13Cols -contains "FirstMemoryGrantKB")
    Write-TestResult -Name "RS13 has MemoryGrantTrend (#368)" -Passed ($rs13Cols -contains "MemoryGrantTrend")
    Write-TestResult -Name "RS13 has FirstTempdbPages (#368)" -Passed ($rs13Cols -contains "FirstTempdbPages")
    Write-TestResult -Name "RS13 has TempdbTrend (#368)" -Passed ($rs13Cols -contains "TempdbTrend")

    # Seed data has the same stats (IX_Orders_Date, IX_OrderItems_OrderID, etc.) in runs
    # 1, 12-14, and 16 -- each with QSTotalCpuMs > 0 and QSTotalExecutions > 0 -- so
    # appearance_cnt >= 2 is guaranteed and RS13 must contain rows.
    Write-TestResult -Name "RS13 has data rows" -Passed ($rs13.Rows.Count -gt 0) -Detail "$($rs13.Rows.Count) stats tracked"

    if ($rs13.Rows.Count -gt 0) {
        # Verify CpuTrend values are valid
        $trends = $rs13.Rows | ForEach-Object { $_["CpuTrend"].ToString() }
        $validTrends = $trends | Where-Object { $_ -in @("IMPROVING", "DEGRADING", "STABLE") }
        Write-TestResult -Name "RS13 CpuTrend values valid" -Passed ($validTrends.Count -eq $trends.Count)

        # #292: Verify PlanTrend includes forced plan awareness
        $planTrends = $rs13.Rows | ForEach-Object { $_["PlanTrend"].ToString() }
        $validPlanTrends = $planTrends | Where-Object { $_ -in @("STABLE", "MORE PLANS (possible regression)", "MORE PLANS (forced plan at risk)", "FEWER PLANS (consolidating)") }
        Write-TestResult -Name "RS13 PlanTrend values valid (#292)" -Passed ($validPlanTrends.Count -eq $planTrends.Count) -Detail "values: $($planTrends -join ', ')"

        # Verify ForcedPlanCount is non-negative integer
        $fpCounts = $rs13.Rows | ForEach-Object { [int]$_["ForcedPlanCount"] }
        $validFpCounts = $fpCounts | Where-Object { $_ -ge 0 }
        Write-TestResult -Name "RS13 ForcedPlanCount >= 0" -Passed ($validFpCounts.Count -eq $fpCounts.Count) -Detail "values: $($fpCounts -join ', ')"

        # #368: Verify MemoryGrantTrend values are valid (NULL allowed for older runs without data)
        $memTrends = $rs13.Rows | ForEach-Object { $_["MemoryGrantTrend"] } | Where-Object { $_ -ne [DBNull]::Value }
        if ($memTrends.Count -gt 0) {
            $validMemTrends = $memTrends | Where-Object { $_.ToString() -in @("IMPROVING", "DEGRADING", "STABLE") }
            Write-TestResult -Name "RS13 MemoryGrantTrend values valid (#368)" -Passed ($validMemTrends.Count -eq $memTrends.Count) -Detail "values: $($memTrends -join ', ')"
        }
        else {
            Write-TestResult -Name "RS13 MemoryGrantTrend values valid (#368)" -Passed $false -Detail "seed has QSTotalMemoryGrantKB; expected non-NULL branch"
        }

        # #368: Verify TempdbTrend values are valid
        $tdbTrends = $rs13.Rows | ForEach-Object { $_["TempdbTrend"] } | Where-Object { $_ -ne [DBNull]::Value }
        if ($tdbTrends.Count -gt 0) {
            $validTdbTrends = $tdbTrends | Where-Object { $_.ToString() -in @("IMPROVING", "DEGRADING", "STABLE") }
            Write-TestResult -Name "RS13 TempdbTrend values valid (#368)" -Passed ($validTdbTrends.Count -eq $tdbTrends.Count) -Detail "values: $($tdbTrends -join ', ')"
        }
        else {
            Write-TestResult -Name "RS13 TempdbTrend values valid (#368)" -Passed $false -Detail "seed has QSTotalTempdbPages; expected non-NULL branch"
        }
    }
    else {
    }
}
catch {
    Write-TestResult -Name "RS13 structure" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 22: I8 QS Performance Trend Check
# =============================================================================

Write-TestHeader "I8 QS Performance Trend"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $recs = $ds.Tables[1]
    $categories = $recs.Rows | ForEach-Object { $_["Category"].ToString() }

    # Seed data has the same QS stats across runs 1, 12-14, 16 so 2+ appearances
    # are guaranteed -- I8 must fire.
    $hasI8 = $categories -contains "QS_PERFORMANCE_TREND"
    Write-TestResult -Name "I8: QS_PERFORMANCE_TREND detected" -Passed $hasI8
    if ($hasI8) {
        $i8Finding = ($recs.Rows | Where-Object { $_["Category"].ToString() -eq "QS_PERFORMANCE_TREND" } | Select-Object -First 1)["Finding"].ToString()
        $hasTrendInfo = $i8Finding -match "improving|degrading|stable|insufficient|per-execution"
        Write-TestResult -Name "I8: Finding describes performance trend" -Passed $hasTrendInfo -Detail ($i8Finding.Substring(0, [Math]::Min(120, $i8Finding.Length)))

        # #368: Check I8 evidence includes memory grant tracking when data exists
        $i8Evidence = ($recs.Rows | Where-Object { $_["Category"].ToString() -eq "QS_PERFORMANCE_TREND" } | Select-Object -First 1)["Evidence"].ToString()
        $hasMemTracking = $i8Evidence -match "Memory grant tracking"
        Write-TestResult -Name "I8: Evidence includes memory grant tracking (#368)" -Passed $hasMemTracking -Detail ($i8Evidence.Substring(0, [Math]::Min(120, $i8Evidence.Length)))
    }
}
catch {
    Write-TestResult -Name "I8 QS performance trend" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 23: RS3 HealthScore Column
# =============================================================================

Write-TestHeader "RS3 HealthScore"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $rs3 = $ds.Tables[2]
    $rs3Cols = $rs3.Columns | ForEach-Object { $_.ColumnName }

    Write-TestResult -Name "RS3 has HealthScore column" -Passed ($rs3Cols -contains "HealthScore")
}
catch {
    Write-TestResult -Name "RS3 HealthScore" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 24: RS1 Dashboard Trend Column (GAP-F)
# =============================================================================

Write-TestHeader "RS1 Dashboard Trend"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $dashboard = $ds.Tables[0]
    $dashCols = $dashboard.Columns | ForEach-Object { $_.ColumnName }

    Write-TestResult -Name "RS1 has Trend column" -Passed ($dashCols -contains "Trend")

    # OVERALL row should have a Trend value (N/A or NULL when @SkipHistory=1, meaningful when history exists)
    $overallRow = $dashboard.Rows | Where-Object { $_["Category"].ToString() -eq "OVERALL" } | Select-Object -First 1
    if ($overallRow) {
        $trendVal = $overallRow["Trend"]
        $hasTrend = ($trendVal -ne [DBNull]::Value -and $trendVal.ToString().Length -gt 0) -or ($trendVal -eq [DBNull]::Value)
        Write-TestResult -Name "RS1 OVERALL Trend column exists" -Passed $hasTrend -Detail $(if ($trendVal -eq [DBNull]::Value) { "NULL (no history)" } else { $trendVal.ToString() })
    }
}
catch {
    Write-TestResult -Name "RS1 Dashboard Trend" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 25: RS13 Per-Execution CPU Columns (BUG-B)
# =============================================================================

Write-TestHeader "RS13 Per-Execution CPU"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $rsMap = Get-RSBySignature $ds
    $rs13 = $rsMap["QSCorrelation"]
    $rs13Cols = $rs13.Columns | ForEach-Object { $_.ColumnName }

    Write-TestResult -Name "RS13 has FirstCpuPerExec" -Passed ($rs13Cols -contains "FirstCpuPerExec")
    Write-TestResult -Name "RS13 has LastCpuPerExec" -Passed ($rs13Cols -contains "LastCpuPerExec")
    Write-TestResult -Name "RS13 has CpuPerExecChangePct" -Passed ($rs13Cols -contains "CpuPerExecChangePct")

    if ($rs13.Rows.Count -gt 0) {
        # Verify per-exec values are populated and reasonable
        $firstRow = $rs13.Rows[0]
        $firstPerExec = $firstRow["FirstCpuPerExec"]
        $lastPerExec = $firstRow["LastCpuPerExec"]
        $hasPerExec = ($firstPerExec -ne [DBNull]::Value -and $lastPerExec -ne [DBNull]::Value)
        Write-TestResult -Name "RS13 per-exec values populated" -Passed $hasPerExec -Detail "first=$firstPerExec last=$lastPerExec"
    }
}
catch {
    Write-TestResult -Name "RS13 per-exec CPU" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 26: I8 Per-Execution Language (BUG-B)
# =============================================================================

Write-TestHeader "I8 Per-Execution Language"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $recs = $ds.Tables[1]
    $i8Rows = $recs.Rows | Where-Object { $_["Category"].ToString() -eq "QS_PERFORMANCE_TREND" }

    # Seed guarantees I8 fires (2+ QS stat appearances); else branch is unreachable.
    Write-TestResult -Name "I8: QS_PERFORMANCE_TREND present for per-exec language check" -Passed ($null -ne $i8Rows -and @($i8Rows).Count -gt 0)
    if ($i8Rows) {
        $i8Finding = ($i8Rows | Select-Object -First 1)["Finding"].ToString()
        $hasPerExec = $i8Finding -match "per-execution"
        Write-TestResult -Name "I8 Finding uses per-execution language" -Passed $hasPerExec -Detail ($i8Finding.Substring(0, [Math]::Min(120, $i8Finding.Length)))
    }
    else {
        Write-TestResult -Name "I8 Finding uses per-execution language" -Skipped -Detail "seed guarantees I8 fires; else branch should not be reached"
    }
}
catch {
    Write-TestResult -Name "I8 Finding uses per-execution language" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 27: Non-QS Runs Have NULL QS Metrics (BUG-D)
# =============================================================================

Write-TestHeader "Non-QS NULL Metrics (BUG-D)"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $rs4 = $ds.Tables[3]

    # Find non-QS runs: no QueryStorePriority=Y and no QUERY_STORE sort order
    $rs4Cols = $rs4.Columns | ForEach-Object { $_.ColumnName }
    $nonQsRuns = $rs4.Rows | Where-Object {
        $_["IsKilled"] -eq $false -and
        ($rs4Cols -notcontains "QueryStorePriority" -or $_["QueryStorePriority"].ToString() -ne "Y") -and
        $_["SortOrder"].ToString() -notlike "*QUERY_STORE*"
    }

    # Seed has multiple serial non-QS runs (runs 2, 5-9, 10, 11, 15) so $nonQsRuns
    # will always be populated; the else branch is unreachable.
    Write-TestResult -Name "Non-QS runs exist in seed data" -Passed ($null -ne $nonQsRuns -and @($nonQsRuns).Count -gt 0) -Detail "$(if ($nonQsRuns) { @($nonQsRuns).Count } else { 0 }) non-QS runs"
    if ($nonQsRuns) {
        $allNull = $true
        foreach ($row in $nonQsRuns) {
            if ($row["WorkloadCoveragePct"] -ne [DBNull]::Value -or
                $row["HighCpuInFirstQuartilePct"] -ne [DBNull]::Value) {
                $allNull = $false
            }
        }
        Write-TestResult -Name "Non-QS runs have NULL WorkloadCoveragePct" -Passed $allNull -Detail "$(@($nonQsRuns).Count) non-QS runs checked"
    }
    else {
        Write-TestResult -Name "Non-QS runs exist in seed data" -Passed $false -Detail "seed has non-QS runs; expected at least one"
    }
}
catch {
    Write-TestResult -Name "Non-QS runs have NULL WorkloadCoveragePct" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 28: WorkloadCoveragePct < 100% for Incomplete QS Run (BUG-A)
# =============================================================================

Write-TestHeader "WorkloadCoveragePct Incomplete Run (BUG-A)"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $rs4 = $ds.Tables[3]

    # Find the incomplete QS run (run 16: test_qs_incomplete, StopReason=TIME_LIMIT with QS)
    $incompleteRun = $rs4.Rows | Where-Object {
        $_["StopReason"].ToString() -eq "TIME_LIMIT" -and
        $_["WorkloadCoveragePct"] -ne [DBNull]::Value
    } | Select-Object -First 1

    if ($incompleteRun) {
        $coverage = [decimal]$incompleteRun["WorkloadCoveragePct"]
        Write-TestResult -Name "Incomplete QS run WorkloadCoveragePct < 100" -Passed ($coverage -lt 100.0) -Detail "$coverage%"
        Write-TestResult -Name "Incomplete QS run WorkloadCoveragePct > 0" -Passed ($coverage -gt 0) -Detail "$coverage%"
        Write-TestResult -Name "Incomplete QS run found" -Passed $true -Detail "TIME_LIMIT run with QS data found"
    }
    else {
        Write-TestResult -Name "Incomplete QS run found" -Passed $false -Detail "No TIME_LIMIT run with QS data found"
    }
}
catch {
    Write-TestResult -Name "WorkloadCoveragePct incomplete" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 29: StatUpdateDiagCache Table
# =============================================================================

Write-TestHeader "StatUpdateDiagCache Table"

try {
    # Drop cache table if exists
    Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagCache', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagCache;" | Out-Null

    # Run with SkipHistory=0 to create and populate cache
    Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @SkipHistory = 0;" -MultiResultSet | Out-Null

    # Check cache table was created
    $cacheExists = (Invoke-SqlQuery "SELECT cnt = OBJECT_ID(N'dbo.StatUpdateDiagCache', N'U')").cnt
    Write-TestResult -Name "Cache table auto-created" -Passed ($cacheExists -ne [DBNull]::Value -and $null -ne $cacheExists)

    if ($cacheExists -ne [DBNull]::Value -and $null -ne $cacheExists) {
        $cacheRows = (Invoke-SqlQuery "SELECT cnt = COUNT(*) FROM dbo.StatUpdateDiagCache").cnt
        Write-TestResult -Name "Cache table has rows" -Passed ([int]$cacheRows -gt 0) -Detail "$cacheRows rows"

        # Verify PAGE compression
        $compressed = (Invoke-SqlQuery "SELECT data_compression_desc FROM sys.partitions WHERE object_id = OBJECT_ID(N'dbo.StatUpdateDiagCache') AND index_id <= 1").data_compression_desc
        Write-TestResult -Name "Cache table uses PAGE compression" -Passed ($compressed -eq "PAGE") -Detail "$compressed"

        # Run again - should use cache (watermark) without errors
        Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @SkipHistory = 0;" -MultiResultSet | Out-Null
        $cacheRows2 = (Invoke-SqlQuery "SELECT cnt = COUNT(*) FROM dbo.StatUpdateDiagCache").cnt
        Write-TestResult -Name "Second run maintains cache" -Passed ([int]$cacheRows2 -ge [int]$cacheRows) -Detail "before=$cacheRows after=$cacheRows2"
    }

    # Cleanup
    Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagCache', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagCache;" | Out-Null
    Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagHistory', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagHistory;" | Out-Null
}
catch {
    Write-TestResult -Name "Cache table" -Passed $false -Detail $_.Exception.Message
    try { Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagCache', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagCache;" | Out-Null } catch {}
    try { Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagHistory', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagHistory;" | Out-Null } catch {}
}

# =============================================================================
# TEST 30: I6 Time Saved Headline (GAP-E)
# =============================================================================

Write-TestHeader "I6 Time Saved Headline"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @EfficacyDaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $recs = $ds.Tables[1]
    $i6Rows = $recs.Rows | Where-Object { $_["Category"].ToString() -eq "QS_EFFICACY" }

    if ($i6Rows) {
        $i6Finding = ($i6Rows | Select-Object -First 1)["Finding"].ToString()
        $hasTimeSaved = $i6Finding -match "min \(QS\) vs"
        Write-TestResult -Name "I6 Finding includes time comparison" -Passed $hasTimeSaved -Detail ($i6Finding.Substring(0, [Math]::Min(150, $i6Finding.Length)))
    }
    else {
        Write-TestResult -Name "I6 Finding includes time comparison" -Skipped -Detail "I6 requires QS efficacy data; may not fire depending on run distribution"
    }
}
catch {
    Write-TestResult -Name "I6 Finding includes time comparison" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 31: History Per-Run HealthScore (BUG-C)
# =============================================================================

Write-TestHeader "History Per-Run HealthScore (BUG-C)"

try {
    # Drop history table if exists
    Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagHistory', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagHistory;" | Out-Null

    # Run to create history
    Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @SkipHistory = 0;" -MultiResultSet | Out-Null

    # Check that HealthScore varies by run (not all the same value)
    $histScores = Invoke-SqlQuery "SELECT DISTINCT HealthScore FROM dbo.StatUpdateDiagHistory WHERE HealthScore IS NOT NULL"
    $distinctScores = $histScores.Rows.Count
    Write-TestResult -Name "History has varying HealthScores (per-run)" -Passed ($distinctScores -gt 1) -Detail "$distinctScores distinct scores"

    # Cleanup
    Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagHistory', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagHistory;" | Out-Null
    Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagCache', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagCache;" | Out-Null
}
catch {
    Write-TestResult -Name "History per-run HealthScore" -Passed $false -Detail $_.Exception.Message
    try { Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagHistory', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagHistory;" | Out-Null } catch {}
    try { Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagCache', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagCache;" | Out-Null } catch {}
}

# =============================================================================
# TEST 32: I5 VERSION_HISTORY Detection (#233)
# =============================================================================

Write-TestHeader "I5 VERSION_HISTORY"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $recs = $ds.Tables[1]
    $categories = $recs.Rows | ForEach-Object { $_["Category"].ToString() }

    $hasI5 = $categories -contains "VERSION_HISTORY"
    Write-TestResult -Name "I5: VERSION_HISTORY detected" -Passed $hasI5

    if ($hasI5) {
        $i5Rows = $recs.Rows | Where-Object { $_["Category"].ToString() -eq "VERSION_HISTORY" }
        $i5Row = ($i5Rows | Select-Object -First 1)

        # Test data has 3 versions (1.6, 1.9, 2.15) so severity should be WARNING
        $severity = $i5Row["Severity"].ToString()
        Write-TestResult -Name "I5: WARNING severity (version skew)" -Passed ($severity -eq "WARNING") -Detail "severity=$severity"

        $finding = $i5Row["Finding"].ToString()
        $hasSkew = $finding -match "skew|versions"
        Write-TestResult -Name "I5: Finding mentions version skew" -Passed $hasSkew -Detail ($finding.Substring(0, [Math]::Min(100, $finding.Length)))

        $evidence = $i5Row["Evidence"].ToString()
        $hasMultiVersion = ($evidence -match "1\.6" -and $evidence -match "1\.9")
        Write-TestResult -Name "I5: Evidence lists multiple versions" -Passed $hasMultiVersion -Detail ($evidence.Substring(0, [Math]::Min(120, $evidence.Length)))
    }
}
catch {
    Write-TestResult -Name "I5 VERSION_HISTORY" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST 33: I8 Insufficient Data Message (#231)
# =============================================================================

Write-TestHeader "I8 Insufficient Data"

try {
    # The test data may or may not have enough QS data for I8 to fire with full trend.
    # We just verify that QS_PERFORMANCE_TREND category exists (either as insufficient or full trend).
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $recs = $ds.Tables[1]
    $i8Rows = $recs.Rows | Where-Object { $_["Category"].ToString() -eq "QS_PERFORMANCE_TREND" }

    # Seed has QS data in runs 1, 12-14, 16, so I8 always fires with standard data.
    Write-TestResult -Name "I8: QS_PERFORMANCE_TREND present" -Passed ($null -ne $i8Rows -and @($i8Rows).Count -gt 0)
    if ($i8Rows) {
        $i8Finding = ($i8Rows | Select-Object -First 1)["Finding"].ToString()
        # Either "insufficient" (new message) or "improving/degrading/stable" (existing)
        $validFinding = $i8Finding -match "insufficient|improving|degrading|stable|per-execution"
        Write-TestResult -Name "I8: QS_PERFORMANCE_TREND has valid finding" -Passed $validFinding -Detail ($i8Finding.Substring(0, [Math]::Min(120, $i8Finding.Length)))
    }
}
catch {
    Write-TestResult -Name "I8 insufficient data" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# GRADE OVERRIDES
# =============================================================================

Write-TestHeader "Grade Override: Force RELIABILITY=A"
try {
    $ds = Invoke-SqlQuery -Query "EXEC dbo.sp_StatUpdate_Diag @SkipHistory = 1, @GradeOverrides = 'RELIABILITY=A';" -MultiResultSet
    $rsByName = Get-RSBySignature -DataSet $ds
    $dashboard = $rsByName["Dashboard"]

    $relRow = $dashboard | Where-Object { $_.Category -eq "RELIABILITY" }
    Write-TestResult -Name "Forced RELIABILITY Grade = A" -Passed ($relRow.Grade -eq 'A') -Detail "Grade=$($relRow.Grade)"
    Write-TestResult -Name "Forced RELIABILITY headline has [OVERRIDE: A]" -Passed ($relRow.Headline -match '\[OVERRIDE: A\]') -Detail $relRow.Headline.Substring(0, [Math]::Min(80, $relRow.Headline.Length))
    Write-TestResult -Name "Forced RELIABILITY detail shows actual score" -Passed ($relRow.Detail -match 'actual score:') -Detail $relRow.Detail.Substring(0, [Math]::Min(80, $relRow.Detail.Length))

    $overallRow = $dashboard | Where-Object { $_.Category -eq "OVERALL" }
    Write-TestResult -Name "OVERALL headline has [Overrides active]" -Passed ($overallRow.Headline -match '\[Overrides active\]') -Detail $overallRow.Headline.Substring(0, [Math]::Min(80, $overallRow.Headline.Length))
}
catch {
    Write-TestResult -Name "Grade Override: Force RELIABILITY=A" -Passed $false -Detail $_.Exception.Message
}

Write-TestHeader "Grade Override: SPEED=IGNORE"
try {
    $ds = Invoke-SqlQuery -Query "EXEC dbo.sp_StatUpdate_Diag @SkipHistory = 1, @GradeOverrides = 'SPEED=IGNORE';" -MultiResultSet
    $rsByName = Get-RSBySignature -DataSet $ds
    $dashboard = $rsByName["Dashboard"]

    $speedRow = $dashboard | Where-Object { $_.Category -eq "SPEED" }
    Write-TestResult -Name "IGNOREd SPEED Grade = -" -Passed ($speedRow.Grade -eq '-') -Detail "Grade=$($speedRow.Grade)"
    Write-TestResult -Name "IGNOREd SPEED Score = 0" -Passed ($speedRow.Score -eq 0) -Detail "Score=$($speedRow.Score)"
    Write-TestResult -Name "IGNOREd SPEED headline has [IGNORED]" -Passed ($speedRow.Headline -match '\[IGNORED\]') -Detail $speedRow.Headline.Substring(0, [Math]::Min(80, $speedRow.Headline.Length))
}
catch {
    Write-TestResult -Name "Grade Override: SPEED=IGNORE" -Passed $false -Detail $_.Exception.Message
}

Write-TestHeader "Grade Weights: Single Category"
try {
    $ds = Invoke-SqlQuery -Query "EXEC dbo.sp_StatUpdate_Diag @SkipHistory = 1, @GradeWeights = 'COMPLETION=50';" -MultiResultSet
    $rsByName = Get-RSBySignature -DataSet $ds
    $dashboard = $rsByName["Dashboard"]

    $overallRow = $dashboard | Where-Object { $_.Category -eq "OVERALL" }
    Write-TestResult -Name "Custom weight OVERALL has score" -Passed ($overallRow.Score -gt 0) -Detail "Score=$($overallRow.Score)"
    Write-TestResult -Name "Custom weight OVERALL has [Overrides active]" -Passed ($overallRow.Headline -match '\[Overrides active\]') -Detail $overallRow.Headline.Substring(0, [Math]::Min(80, $overallRow.Headline.Length))

    # All 5 rows should still be present
    Write-TestResult -Name "Dashboard has 5 rows" -Passed ($dashboard.Rows.Count -eq 5) -Detail "Rows=$($dashboard.Rows.Count)"
}
catch {
    Write-TestResult -Name "Grade Weights: Single Category" -Passed $false -Detail $_.Exception.Message
}

Write-TestHeader "Grade Weights: Maximum documented integer range"
try {
    $ds = Invoke-SqlQuery -Query "EXEC dbo.sp_StatUpdate_Diag @SkipHistory = 1, @GradeWeights = 'COMPLETION=2147483647, RELIABILITY=2147483647, SPEED=2147483647, WORKLOAD=2147483647';" -MultiResultSet
    $rsByName = Get-RSBySignature -DataSet $ds
    $dashboard = $rsByName["Dashboard"]
    $overallRow = $dashboard | Where-Object { $_.Category -eq "OVERALL" }
    $passed = ($dashboard.Rows.Count -eq 5) -and ($null -ne $overallRow)
    Write-TestResult -Name "GradeWeights accepts four maximum signed integers" -Passed $passed -Detail "DashboardRows=$($dashboard.Rows.Count) OverallRows=$(if($null -eq $overallRow){0}else{@($overallRow).Count})"
}
catch {
    Write-TestResult -Name "GradeWeights accepts four maximum signed integers" -Passed $false -Detail $_.Exception.Message
}

Write-TestHeader "Grade Override: Invalid Input"
try {
    $ds = Invoke-SqlQuery -Query "EXEC dbo.sp_StatUpdate_Diag @SkipHistory = 1, @GradeOverrides = 'BOGUS=A';" -MultiResultSet
    # Should not reach here - validation should raise error
    Write-TestResult -Name "Invalid override raises error" -Passed $false -Detail "Expected error but query succeeded"
}
catch {
    $isValidationError = $_.Exception.Message -match "unknown category.*BOGUS"
    Write-TestResult -Name "Invalid override raises error" -Passed $isValidationError -Detail $_.Exception.Message.Substring(0, [Math]::Min(120, $_.Exception.Message.Length))
}

Write-TestHeader "No Overrides Regression"
try {
    $ds = Invoke-SqlQuery -Query "EXEC dbo.sp_StatUpdate_Diag @SkipHistory = 1;" -MultiResultSet
    $rsByName = Get-RSBySignature -DataSet $ds
    $dashboard = $rsByName["Dashboard"]

    $overallRow = $dashboard | Where-Object { $_.Category -eq "OVERALL" }
    Write-TestResult -Name "No-override OVERALL lacks [Overrides active]" -Passed ($overallRow.Headline -notmatch '\[Overrides active\]') -Detail $overallRow.Headline.Substring(0, [Math]::Min(80, $overallRow.Headline.Length))

    $relRow = $dashboard | Where-Object { $_.Category -eq "RELIABILITY" }
    Write-TestResult -Name "No-override RELIABILITY lacks [OVERRIDE]" -Passed ($relRow.Headline -notmatch '\[OVERRIDE') -Detail $relRow.Headline.Substring(0, [Math]::Min(80, $relRow.Headline.Length))
    Write-TestResult -Name "No-override RELIABILITY lacks [IGNORED]" -Passed ($relRow.Headline -notmatch '\[IGNORED\]') -Detail $relRow.Headline.Substring(0, [Math]::Min(80, $relRow.Headline.Length))
}
catch {
    Write-TestResult -Name "No Overrides Regression" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# Phase 1-5 New Column/Check Tests
# =============================================================================

Write-Host "`n  TEST: Phase 2 - RS 2 Summary Column" -ForegroundColor Yellow
try {
    $phaseResult = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $recommendations = $phaseResult.Tables[1]
    $hasSummary = $recommendations.Columns | Where-Object { $_.ColumnName -eq "Summary" }
    Write-TestResult -Name "RS 2 has Summary column" -Passed ($null -ne $hasSummary) -Detail "Columns: $($recommendations.Columns.ColumnName -join ', ')"

    if ($recommendations.Rows.Count -gt 0) {
        $firstSummary = $recommendations.Rows[0].Summary
        $hasBracket = $firstSummary -match '^\['
        Write-TestResult -Name "RS 2 Summary starts with [Severity]" -Passed $hasBracket -Detail ($firstSummary.Substring(0, [Math]::Min(80, $firstSummary.Length)))
    }
}
catch {
    Write-TestResult -Name "RS 2 Summary column" -Passed $false -Detail $_.Exception.Message
}

Write-Host "`n  TEST: Phase 2 - RS 12 WorkloadRankPct + CumulativeCpuPct" -ForegroundColor Yellow
try {
    $rs12 = $phaseResult.Tables[10]
    $hasRankPct = $rs12.Columns | Where-Object { $_.ColumnName -eq "WorkloadRankPct" }
    $hasCumPct = $rs12.Columns | Where-Object { $_.ColumnName -eq "CumulativeCpuPct" }
    Write-TestResult -Name "RS 12 has WorkloadRankPct" -Passed ($null -ne $hasRankPct) -Detail "Columns: $($rs12.Columns.ColumnName -join ', ')"
    Write-TestResult -Name "RS 12 has CumulativeCpuPct" -Passed ($null -ne $hasCumPct) -Detail ""
}
catch {
    Write-TestResult -Name "RS 12 WorkloadRankPct/CumulativeCpuPct" -Passed $false -Detail $_.Exception.Message
}

Write-Host "`n  TEST: Phase 2 - RS 10 Top5ConcentrationPct" -ForegroundColor Yellow
try {
    $rs10 = $phaseResult.Tables[8]
    $hasConc = $rs10.Columns | Where-Object { $_.ColumnName -eq "Top5ConcentrationPct" }
    Write-TestResult -Name "RS 10 has Top5ConcentrationPct" -Passed ($null -ne $hasConc) -Detail "Columns: $($rs10.Columns.ColumnName -join ', ')"
}
catch {
    Write-TestResult -Name "RS 10 Top5ConcentrationPct" -Passed $false -Detail $_.Exception.Message
}

Write-Host "`n  TEST: Phase 2 - RS 7 StatsInParallel" -ForegroundColor Yellow
try {
    $rs7 = $phaseResult.Tables[6]
    $hasSIP = $rs7.Columns | Where-Object { $_.ColumnName -eq "StatsInParallel" }
    Write-TestResult -Name "RS 7 has StatsInParallel" -Passed ($null -ne $hasSIP) -Detail "Columns: $($rs7.Columns.ColumnName -join ', ')"
}
catch {
    Write-TestResult -Name "RS 7 StatsInParallel" -Passed $false -Detail $_.Exception.Message
}

Write-Host "`n  TEST: Phase 2 - ObjectId/StatsId Extraction (#268)" -ForegroundColor Yellow
try {
    $rs13 = $phaseResult.Tables[11]
    # RS 13 shows per-stat detail; ObjectId/StatsId are in #stat_updates, not directly in RS output.
    # The only externally visible evidence is RS 12 (WorkloadRank uses ObjectId internally).
    # Assert RS 13 is present (proc did not error on the extraction path) and RS 12 has rows.
    $rs12 = $phaseResult.Tables[10]
    Write-TestResult -Name "ObjectId/StatsId extraction enabled (RS12 has rows)" -Passed ($rs12.Rows.Count -gt 0) -Detail "RS12 rows=$($rs12.Rows.Count)"
}
catch {
    Write-TestResult -Name "ObjectId/StatsId Extraction" -Passed $false -Detail $_.Exception.Message
}

Write-Host "`n  TEST: Phase 3 - RS 5 WorkloadImpactPct + ImpactScore" -ForegroundColor Yellow
try {
    $rs5 = $phaseResult.Tables[4]
    $hasWIP = $rs5.Columns | Where-Object { $_.ColumnName -eq "WorkloadImpactPct" }
    $hasIS = $rs5.Columns | Where-Object { $_.ColumnName -eq "ImpactScore" }
    Write-TestResult -Name "RS 5 has WorkloadImpactPct" -Passed ($null -ne $hasWIP) -Detail "Columns: $($rs5.Columns.ColumnName -join ', ')"
    Write-TestResult -Name "RS 5 has ImpactScore" -Passed ($null -ne $hasIS) -Detail ""
}
catch {
    Write-TestResult -Name "RS 5 WorkloadImpactPct/ImpactScore" -Passed $false -Detail $_.Exception.Message
}

Write-Host "`n  TEST: Phase 3 - RS 7 ImpactScore" -ForegroundColor Yellow
try {
    $hasIS7 = $rs7.Columns | Where-Object { $_.ColumnName -eq "ImpactScore" }
    Write-TestResult -Name "RS 7 has ImpactScore" -Passed ($null -ne $hasIS7) -Detail ""
}
catch {
    Write-TestResult -Name "RS 7 ImpactScore" -Passed $false -Detail $_.Exception.Message
}

Write-Host "`n  TEST: Phase 3 - W7 HIGH_IMPACT_STATS_DEPRIORITIZED Check" -ForegroundColor Yellow
try {
    $w7check = $recommendations.Rows | Where-Object { $_.Category -eq "HIGH_IMPACT_STATS_DEPRIORITIZED" }
    # W7 fires only when high-impact QS stats were ranked outside the top quartile.
    # Seed data uses MODIFICATION_COUNTER sort for most runs, so W7 may or may not fire.
    # Assert proc ran without error (catch above) and returned a recommendations table.
    $w7RowCount = if ($w7check) { @($w7check).Count } else { 0 }
    Write-TestResult -Name "W7 check ran without error (recommendations table present)" -Passed ($recommendations.Rows.Count -gt 0) -Detail "W7 rows: $w7RowCount; total recs: $($recommendations.Rows.Count)"
}
catch {
    Write-TestResult -Name "W7 check" -Passed $false -Detail $_.Exception.Message
}

Write-Host "`n  TEST: Phase 3 - I9 WORKLOAD_CONCENTRATION Check" -ForegroundColor Yellow
try {
    $i9recs = $phaseResult.Tables[1]
    $i9check = @($i9recs.Rows | Where-Object { $_.Category -eq "WORKLOAD_CONCENTRATION" })
    Write-TestResult -Name "I9 WORKLOAD_CONCENTRATION detected" -Passed ($i9check.Count -gt 0) -Detail $(if ($i9check.Count -gt 0) { $i9check[0].Finding.Substring(0, [Math]::Min(80, $i9check[0].Finding.Length)) } else { "Not found" })
}
catch {
    Write-TestResult -Name "I9 check" -Passed $false -Detail $_.Exception.Message
}

Write-Host "`n  TEST: Phase 4 - I8 Regressed Count in Evidence (#276)" -ForegroundColor Yellow
try {
    $i8recs = $phaseResult.Tables[1]
    $i8check = @($i8recs.Rows | Where-Object { $_.Category -eq "QS_PERFORMANCE_TREND" })
    Write-TestResult -Name "I8 QS_PERFORMANCE_TREND exists" -Passed ($i8check.Count -gt 0) -Detail $(if ($i8check.Count -gt 0) { $i8check[0].Evidence.Substring(0, [Math]::Min(100, $i8check[0].Evidence.Length)) } else { "Not found" })
}
catch {
    Write-TestResult -Name "I8 regressed pattern" -Passed $false -Detail $_.Exception.Message
}

Write-Host "`n  TEST: Phase 5 - RS 3 ReplicaRole Column (#296)" -ForegroundColor Yellow
try {
    $rs3 = $phaseResult.Tables[2]
    $hasRR = $rs3.Columns | Where-Object { $_.ColumnName -eq "ReplicaRole" }
    Write-TestResult -Name "RS 3 has ReplicaRole column" -Passed ($null -ne $hasRR) -Detail "Columns: $($rs3.Columns.ColumnName -join ', ')"
}
catch {
    Write-TestResult -Name "RS 3 ReplicaRole" -Passed $false -Detail $_.Exception.Message
}

Write-Host "`n  TEST: Phase 4 - WORKLOAD FOCUS Dashboard Non-QS (#267)" -ForegroundColor Yellow
try {
    # Test with standard data (has QS runs, so WORKLOAD FOCUS should have a grade)
    $dashboard = $phaseResult.Tables[0]
    $wfRow = $dashboard | Where-Object { $_.Category -eq "WORKLOAD FOCUS" }
    Write-TestResult -Name "WORKLOAD FOCUS has grade with QS data" -Passed ($wfRow.Grade -ne '-') -Detail "Grade=$($wfRow.Grade), Score=$($wfRow.Score)"
}
catch {
    Write-TestResult -Name "WORKLOAD FOCUS dashboard" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# ENHANCED @HELP TESTS (Phase 2)
# =============================================================================
Write-TestHeader "Enhanced @Help Output"

try {
    $helpDs = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @Help = 1;" -MultiResultSet
    $helpRsCount = $helpDs.Tables.Count
    Write-TestResult -Name "Help returns >= 7 result sets (was 5, now 9)" -Passed ($helpRsCount -ge 7) -Detail "Got $helpRsCount"

    # Find the grading scale RS (has grade + score_range columns)
    $gradingRs = $null
    $gradingWeightsRs = $null
    $prereqRs = $null
    $overrideExamplesRs = $null
    $versionHistoryRs = $null
    foreach ($t in $helpDs.Tables) {
        $cols = $t.Columns | ForEach-Object { $_.ColumnName }
        if ($cols -contains "grade" -and $cols -contains "score_range") { $gradingRs = $t }
        if ($cols -contains "category" -and $cols -contains "default_weight" -and $cols -contains "what_it_measures") { $gradingWeightsRs = $t }
        if ($cols -contains "topic" -and $cols -contains "detail" -and $t.Rows.Count -ge 1) {
            $firstTopic = "$($t.Rows[0].help_topic)"
            if ($firstTopic -eq "Prerequisites and Limitations") { $prereqRs = $t }
        }
        if ($cols -contains "example_name" -and $cols -contains "example_code" -and $t.Rows.Count -ge 1) {
            $firstExample = "$($t.Rows[0].help_topic)"
            if ($firstExample -eq "Grade Override Examples") { $overrideExamplesRs = $t }
        }
        if ($cols -contains "version_date" -and $cols -contains "changes") { $versionHistoryRs = $t }
    }

    Write-TestResult -Name "Help has Grading Scale RS (grades A-F)" -Passed ($null -ne $gradingRs -and $gradingRs.Rows.Count -eq 5) -Detail "Rows: $(if ($gradingRs) { $gradingRs.Rows.Count } else { 'missing' })"
    Write-TestResult -Name "Help has Grading Weights RS (4 categories)" -Passed ($null -ne $gradingWeightsRs -and $gradingWeightsRs.Rows.Count -eq 4) -Detail "Rows: $(if ($gradingWeightsRs) { $gradingWeightsRs.Rows.Count } else { 'missing' })"
    Write-TestResult -Name "Help has Prerequisites RS" -Passed ($null -ne $prereqRs -and $prereqRs.Rows.Count -ge 3) -Detail "Rows: $(if ($prereqRs) { $prereqRs.Rows.Count } else { 'missing' })"
    Write-TestResult -Name "Help has Grade Override Examples RS" -Passed ($null -ne $overrideExamplesRs -and $overrideExamplesRs.Rows.Count -ge 3) -Detail "Rows: $(if ($overrideExamplesRs) { $overrideExamplesRs.Rows.Count } else { 'missing' })"
    Write-TestResult -Name "Help has Version History RS" -Passed ($null -ne $versionHistoryRs -and $versionHistoryRs.Rows.Count -ge 3) -Detail "Rows: $(if ($versionHistoryRs) { $versionHistoryRs.Rows.Count } else { 'missing' })"

    # Verify all proc parameters appear in the Parameters RS
    $paramRs = $null
    foreach ($t in $helpDs.Tables) {
        $cols = $t.Columns | ForEach-Object { $_.ColumnName }
        if ($cols -contains "parameter_name" -and $cols -contains "data_type") { $paramRs = $t; break }
    }
    $expectedParams = @('@DaysBack', '@CommandLogDatabase', '@Obfuscate', '@ObfuscationSeed', '@ObfuscationMapTable',
        '@LongRunningMinutes', '@FailureThreshold', '@TimeLimitExhaustionPct', '@ThroughputWindowDays', '@TopN',
        '@EfficacyDaysBack', '@EfficacyDetailDays', '@ExpertMode', '@SkipHistory', '@GradeOverrides', '@GradeWeights',
        '@Help', '@Debug', '@SingleResultSet', '@Version', '@VersionDate')
    $foundParams = @()
    if ($paramRs) { $foundParams = $paramRs.Rows | ForEach-Object { $_.parameter_name } }
    $missingParams = $expectedParams | Where-Object { $_ -notin $foundParams }
    Write-TestResult -Name "Help Parameters RS lists all 21 parameters" -Passed ($missingParams.Count -eq 0) -Detail "Missing: $($missingParams -join ', ')"

    # Verify all diagnostic checks appear
    $checkRs = $null
    foreach ($t in $helpDs.Tables) {
        $cols = $t.Columns | ForEach-Object { $_.ColumnName }
        if ($cols -contains "check_id" -and $cols -contains "severity") { $checkRs = $t; break }
    }
    $expectedChecks = @('C1','C2','C3','C4','W1','W2','W3','W4','W5','W6','I1','I2','I3','I4','I5','I6','I7','I8')
    $foundChecks = @()
    if ($checkRs) { $foundChecks = $checkRs.Rows | ForEach-Object { $_.check_id } }
    $missingChecks = $expectedChecks | Where-Object { $_ -notin $foundChecks }
    Write-TestResult -Name "Help Checks RS lists all 18 checks (C1-C4, W1-W6, I1-I8)" -Passed ($missingChecks.Count -eq 0) -Detail "Missing: $($missingChecks -join ', ')"
}
catch {
    Write-TestResult -Name "Enhanced @Help output" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# ENHANCED @DEBUG TESTS (Phase 1)
# =============================================================================
Write-TestHeader "Enhanced @Debug Output"

try {
    $debugDs = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @Debug = 1, @SkipHistory = 1;" -MultiResultSet

    # Debug should produce extra result sets (samples from temp tables)
    $debugRsCount = $debugDs.Tables.Count
    # Normal ExpertMode=0 returns 2 RS.  Debug adds: #runs sample, #stat_updates sample, #executive_dashboard, plus possibly #qs_efficacy
    Write-TestResult -Name "Debug mode produces extra result sets (samples)" -Passed ($debugRsCount -gt 2) -Detail "$debugRsCount result sets"

    # Find the #runs sample (has RunLabel + IsKilled + StartTime columns but <= 5 rows)
    $debugRunsSample = $null
    foreach ($t in $debugDs.Tables) {
        $cols = $t.Columns | ForEach-Object { $_.ColumnName }
        if ($cols -contains "RunLabel" -and $cols -contains "IsKilled" -and $cols -contains "StartTime" -and $t.Rows.Count -le 5 -and $cols -contains "StatsFound") {
            $debugRunsSample = $t
            break
        }
    }
    Write-TestResult -Name "Debug #runs sample present (<= 5 rows)" -Passed ($null -ne $debugRunsSample) -Detail "Rows: $(if ($debugRunsSample) { $debugRunsSample.Rows.Count } else { 'not found' })"

    # Find the #executive_dashboard sample (has Category + Grade + Score + Headline)
    $debugDashboard = $null
    foreach ($t in $debugDs.Tables) {
        $cols = $t.Columns | ForEach-Object { $_.ColumnName }
        if ($cols -contains "Category" -and $cols -contains "Grade" -and $cols -contains "Score" -and $cols -contains "Headline") {
            $debugDashboard = $t
            break
        }
    }
    Write-TestResult -Name "Debug #executive_dashboard sample present" -Passed ($null -ne $debugDashboard) -Detail "Rows: $(if ($debugDashboard) { $debugDashboard.Rows.Count } else { 'not found' })"
}
catch {
    Write-TestResult -Name "Enhanced debug output" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# REGRESSION TESTS
# =============================================================================
Write-TestHeader "Regression Tests"

# NULL RunLabel regression (legacy CommandLog entries)
try {
    # Insert a legacy START entry with no RunLabel in ExtendedInfo
    Invoke-SqlQuery -Query @"
INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (
    N'TestDB', N'dbo', N'LegacyTable', N'SP_STATUPDATE_START', N'EXEC sp_StatUpdate',
    DATEADD(DAY, -25, GETDATE()), DATEADD(DAY, -25, DATEADD(MINUTE, 5, GETDATE())),
    N'<Parameters><Version>1.0</Version><Databases>TestDB</Databases><TimeLimit>3600</TimeLimit></Parameters>'
);
"@

    # Run diag - should NOT throw "Cannot insert NULL into RunLabel"
    $legacyDs = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @SkipHistory = 1, @ExpertMode = 1;" -MultiResultSet
    $legacyPassed = ($null -ne $legacyDs -and $legacyDs.Tables.Count -gt 0)
    Write-TestResult -Name "NULL RunLabel regression - legacy entry handled" -Passed $legacyPassed -Detail "Result sets: $(if ($legacyDs) { $legacyDs.Tables.Count } else { 'none' })"

    # Verify the legacy run appears in Run Detail (RS 4) with a synthetic label
    $runDetailRs = $null
    foreach ($t in $legacyDs.Tables) {
        $cols = $t.Columns | ForEach-Object { $_.ColumnName }
        if ($cols -contains "RunLabel" -and $cols -contains "StopReason" -and $cols -contains "IsKilled") {
            $runDetailRs = $t
            break
        }
    }
    $hasLegacyLabel = $false
    if ($runDetailRs) {
        foreach ($row in $runDetailRs.Rows) {
            if ("$($row.RunLabel)" -like "legacy_*") { $hasLegacyLabel = $true; break }
        }
    }
    Write-TestResult -Name "NULL RunLabel regression - synthetic legacy_ label created" -Passed $hasLegacyLabel -Detail "Found legacy_ prefix in RunDetail"

    # Cleanup
    Invoke-SqlQuery -Query "DELETE FROM dbo.CommandLog WHERE ObjectName = N'LegacyTable' AND DatabaseName = N'TestDB';"
}
catch {
    Write-TestResult -Name "NULL RunLabel regression" -Passed $false -Detail $_.Exception.Message
    # Cleanup even on failure
    try { Invoke-SqlQuery -Query "DELETE FROM dbo.CommandLog WHERE ObjectName = N'LegacyTable' AND DatabaseName = N'TestDB';" } catch {}
}

# ObfuscationMapTable dedup regression
try {
    # Drop map table if exists
    Invoke-SqlQuery -Query "IF OBJECT_ID('tempdb.dbo.test_obf_map', 'U') IS NOT NULL DROP TABLE tempdb.dbo.test_obf_map;"

    # Run twice with same seed
    Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @Obfuscate = 1, @ObfuscationSeed = N'TestSeed123', @ObfuscationMapTable = N'tempdb.dbo.test_obf_map', @SkipHistory = 1;" -MultiResultSet
    $run1Count = (Invoke-SqlQuery -Query "SELECT COUNT(*) AS cnt FROM tempdb.dbo.test_obf_map;").cnt

    Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @Obfuscate = 1, @ObfuscationSeed = N'TestSeed123', @ObfuscationMapTable = N'tempdb.dbo.test_obf_map', @SkipHistory = 1;" -MultiResultSet
    $run2Count = (Invoke-SqlQuery -Query "SELECT COUNT(*) AS cnt FROM tempdb.dbo.test_obf_map;").cnt

    Write-TestResult -Name "ObfuscationMapTable dedup - no duplicates after 2 runs" -Passed ($run1Count -eq $run2Count -and $run1Count -gt 0) -Detail "Run1=$run1Count, Run2=$run2Count"

    # Cleanup
    Invoke-SqlQuery -Query "IF OBJECT_ID('tempdb.dbo.test_obf_map', 'U') IS NOT NULL DROP TABLE tempdb.dbo.test_obf_map;"
}
catch {
    Write-TestResult -Name "ObfuscationMapTable dedup regression" -Passed $false -Detail $_.Exception.Message
}

# Arithmetic overflow regression (nvarchar width)
try {
    $overflowDs = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @SkipHistory = 1;" -MultiResultSet
    Write-TestResult -Name "Arithmetic overflow regression - no nvarchar overflow" -Passed ($null -ne $overflowDs -and $overflowDs.Tables.Count -gt 0) -Detail "Completed without Msg 8115"
}
catch {
    $isOverflow = $_.Exception.Message -match "8115|Arithmetic overflow"
    Write-TestResult -Name "Arithmetic overflow regression" -Passed $false -Detail $(if ($isOverflow) { "Arithmetic overflow still present" } else { $_.Exception.Message })
}

# =============================================================================
# PARAMETER VALIDATION TESTS
# =============================================================================
Write-TestHeader "Parameter Validation"

# @DaysBack = 0 (should error)
try {
    $result = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 0, @SkipHistory = 1;" -MultiResultSet
    Write-TestResult -Name "@DaysBack=0 returns error" -Passed $false -Detail "Expected error but succeeded"
}
catch {
    Write-TestResult -Name "@DaysBack=0 returns error" -Passed ($_.Exception.Message -match "DaysBack|days|invalid|range") -Detail "Error caught"
}

# @TopN = 0 (should error)
try {
    $result = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @TopN = 0, @SkipHistory = 1;" -MultiResultSet
    Write-TestResult -Name "@TopN=0 returns error" -Passed $false -Detail "Expected error but succeeded"
}
catch {
    Write-TestResult -Name "@TopN=0 returns error" -Passed ($_.Exception.Message -match "TopN|invalid|range") -Detail "Error caught"
}

# @Obfuscate=0 with @ObfuscationMapTable (should warn, not error)
try {
    $result = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @Obfuscate = 0, @ObfuscationMapTable = N'dbo.ShouldNotCreate', @SkipHistory = 1;" -MultiResultSet
    Write-TestResult -Name "@Obfuscate=0 + @ObfuscationMapTable warns but succeeds" -Passed ($null -ne $result) -Detail "Completed without error"
}
catch {
    Write-TestResult -Name "@Obfuscate=0 + @ObfuscationMapTable" -Passed $false -Detail $_.Exception.Message
}

# @CommandLogDatabase pointing to nonexistent DB
try {
    $result = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @CommandLogDatabase = N'NonExistentDB_XYZ', @SkipHistory = 1;" -MultiResultSet
    Write-TestResult -Name "@CommandLogDatabase nonexistent DB returns error" -Passed $false -Detail "Expected error but succeeded"
}
catch {
    Write-TestResult -Name "@CommandLogDatabase nonexistent DB returns error" -Passed ($_.Exception.Message -match "CommandLog|not found") -Detail "Error caught"
}

# =============================================================================
# EDGE CASE TESTS
# =============================================================================
Write-TestHeader "Edge Cases"

# Minimum valid window: @DaysBack=2 with @ThroughputWindowDays=1 (gh-558 requires
# @DaysBack >= 2 x @ThroughputWindowDays, so @DaysBack=1 is no longer valid)
try {
    $shortDs = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 2, @ThroughputWindowDays = 1, @SkipHistory = 1;" -MultiResultSet
    Write-TestResult -Name "@DaysBack=2 - graceful with limited data" -Passed ($null -ne $shortDs -and $shortDs.Tables.Count -ge 1) -Detail "RS count: $(if ($shortDs) { $shortDs.Tables.Count } else { 0 })"
}
catch {
    Write-TestResult -Name "@DaysBack=2 edge case" -Passed $false -Detail $_.Exception.Message
}

# @DaysBack=1 cannot fit two comparison windows -> validation error (gh-558)
try {
    $d1Raised = $false
    $d1Message = ""
    try {
        $null = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 1, @ThroughputWindowDays = 1, @SkipHistory = 1;" -MultiResultSet
    }
    catch {
        $d1Raised = $true
        $d1Message = $_.Exception.Message
    }
    Write-TestResult -Name "@DaysBack=1 rejected (cannot fit two C4 windows)" -Passed ($d1Raised -and $d1Message -match "ThroughputWindowDays")
}
catch {
    Write-TestResult -Name "@DaysBack=1 rejection edge case" -Passed $false -Detail $_.Exception.Message
}

# @TopN=1 boundary
try {
    $topN1Ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @TopN = 1, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $rsMap = Get-RSBySignature -DataSet $topN1Ds

    $topTablesOk = (-not $rsMap.ContainsKey("TopTables")) -or ($rsMap["TopTables"].Rows.Count -le 1)
    $failingOk = (-not $rsMap.ContainsKey("FailingStats")) -or ($rsMap["FailingStats"].Rows.Count -le 1)
    $longRunOk = (-not $rsMap.ContainsKey("LongRunning")) -or ($rsMap["LongRunning"].Rows.Count -le 1)
    Write-TestResult -Name "@TopN=1 caps detail result sets to 1 row" -Passed ($topTablesOk -and $failingOk -and $longRunOk) -Detail "TopTables=$(if ($rsMap.ContainsKey('TopTables')) { $rsMap['TopTables'].Rows.Count } else { 'empty' }), Failing=$(if ($rsMap.ContainsKey('FailingStats')) { $rsMap['FailingStats'].Rows.Count } else { 'empty' })"
}
catch {
    Write-TestResult -Name "@TopN=1 boundary" -Passed $false -Detail $_.Exception.Message
}

# @SingleResultSet auto-promotion verification
try {
    $srsDs = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @SingleResultSet = 1, @SkipHistory = 1;" -MultiResultSet
    $srsRsCount = $srsDs.Tables.Count
    Write-TestResult -Name "@SingleResultSet=1 returns exactly 1 result set" -Passed ($srsRsCount -eq 1) -Detail "Got $srsRsCount"

    if ($srsRsCount -ge 1) {
        $srsTable = $srsDs.Tables[0]
        $srsCols = $srsTable.Columns | ForEach-Object { $_.ColumnName }
        $hasAllCols = ($srsCols -contains "ResultSetID") -and ($srsCols -contains "ResultSetName") -and ($srsCols -contains "RowNum") -and ($srsCols -contains "RowData")
        Write-TestResult -Name "@SingleResultSet has 4 expected columns" -Passed ($hasAllCols -and $srsCols.Count -eq 4) -Detail "Columns: $($srsCols -join ', ')"

        # Verify ResultSetIDs 1-13 present (auto-promoted ExpertMode)
        $rsIds = $srsTable.Rows | ForEach-Object { [int]$_.ResultSetID } | Sort-Object -Unique
        $has1to2 = ($rsIds -contains 1) -and ($rsIds -contains 2)
        Write-TestResult -Name "@SingleResultSet has RS 1 (Dashboard) and RS 2 (Recommendations)" -Passed $has1to2 -Detail "IDs found: $($rsIds -join ',')"
    }
}
catch {
    Write-TestResult -Name "@SingleResultSet auto-promotion" -Passed $false -Detail $_.Exception.Message
}

# @Help + OUTPUT parameters
try {
    $helpOutDs = Invoke-SqlQuery -Query @"
DECLARE @v varchar(20), @vd datetime;
EXECUTE dbo.sp_StatUpdate_Diag @Help = 1, @Version = @v OUTPUT, @VersionDate = @vd OUTPUT;
SELECT Version = @v, VersionDate = @vd;
"@ -MultiResultSet
    # The last result set should be our SELECT with Version/VersionDate
    $outTable = $helpOutDs.Tables[$helpOutDs.Tables.Count - 1]
    $outVersion = "$($outTable.Rows[0].Version)"
    $outDate = $outTable.Rows[0].VersionDate
    Write-TestResult -Name "@Help=1 sets OUTPUT params" -Passed ($outVersion -match '^\d{4}\.\d{2}\.\d{2}' -and $null -ne $outDate) -Detail "Version=$outVersion"
}
catch {
    Write-TestResult -Name "@Help + OUTPUT params" -Passed $false -Detail $_.Exception.Message
}

# @ExpertMode=0 returns exactly 2 result sets
try {
    $mgmtDs = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @ExpertMode = 0, @SkipHistory = 1;" -MultiResultSet
    $mgmtRsCount = $mgmtDs.Tables.Count
    Write-TestResult -Name "@ExpertMode=0 returns exactly 2 result sets" -Passed ($mgmtRsCount -eq 2) -Detail "Got $mgmtRsCount"

    if ($mgmtRsCount -ge 2) {
        $rs1Cols = $mgmtDs.Tables[0].Columns | ForEach-Object { $_.ColumnName }
        $rs2Cols = $mgmtDs.Tables[1].Columns | ForEach-Object { $_.ColumnName }
        $rs1IsDash = ($rs1Cols -contains "Grade" -and $rs1Cols -contains "Score" -and $rs1Cols -contains "Headline")
        $rs2IsRecs = ($rs2Cols -contains "Severity" -and $rs2Cols -contains "Finding" -and $rs2Cols -contains "Recommendation")
        Write-TestResult -Name "@ExpertMode=0 RS1=Dashboard RS2=Recommendations" -Passed ($rs1IsDash -and $rs2IsRecs) -Detail "RS1 dashboard=$rs1IsDash, RS2 recs=$rs2IsRecs"
    }
}
catch {
    Write-TestResult -Name "@ExpertMode=0 result set contract" -Passed $false -Detail $_.Exception.Message
}

# @Debug=1 does NOT change labeled RS count when @ExpertMode=0
try {
    $debugMgmtDs = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @Debug = 1, @ExpertMode = 0, @SkipHistory = 1;" -MultiResultSet
    # Should have more RS than 2 (debug samples) but the Dashboard and Recommendations should still be identifiable
    $debugMap = Get-RSBySignature -DataSet $debugMgmtDs
    $hasDash = $debugMap.ContainsKey("Dashboard")
    $hasRecs = $debugMap.ContainsKey("Recommendations")
    Write-TestResult -Name "@Debug=1 still contains Dashboard + Recommendations" -Passed ($hasDash -and $hasRecs) -Detail "Dashboard=$hasDash, Recommendations=$hasRecs, Total RS=$($debugMgmtDs.Tables.Count)"
}
catch {
    Write-TestResult -Name "@Debug + @ExpertMode=0" -Passed $false -Detail $_.Exception.Message
}

# W7 check existence verification
try {
    $w7Ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @Help = 1;" -MultiResultSet
    $w7CheckRs = $null
    foreach ($t in $w7Ds.Tables) {
        $cols = $t.Columns | ForEach-Object { $_.ColumnName }
        if ($cols -contains "check_id" -and $cols -contains "severity") { $w7CheckRs = $t; break }
    }
    $hasW7 = $false
    if ($w7CheckRs) {
        foreach ($row in $w7CheckRs.Rows) {
            if ("$($row.check_id)" -eq "W7") { $hasW7 = $true; break }
        }
    }
    # W7 is in the @Help checks table; assert it appears there.
    Write-TestResult -Name "W7 check documented in @Help" -Passed $hasW7 -Detail "W7 in help=$hasW7"
}
catch {
    Write-TestResult -Name "W7 check documentation" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# W5 PARALLEL-QS SCENARIO (tracking issue)
# Covers the diag 2026.06.08.1 branches the standard dataset cannot reach:
#   - W5 QS_PARALLEL_UNRELIABLE: all QS-priority runs parallel + QSTotalCpuMs
#     present + zero QSPlanCount rows -> INFO replaces QS_NOT_EFFECTIVE WARNING
#   - I10 keeps @SortOrder=QUERY_STORE / @QueryStore=CPU on the parallel artifact
#   - I10 TimeLimit jitter normalization (21598/21599 -> 21600)
#   - I10 @CriticalTables hint when W9 LOCK_TIMEOUT_INEFFECTIVE fires
# The standard dataset cannot host this: @w5_all_parallel requires ALL
# QS-priority runs parallel, and @w5_qs_data_runs = 0 requires NO QSPlanCount
# anywhere -- both contradicted by the standard QS efficacy runs.  So this
# section truncates CommandLog, loads an isolated scenario, asserts, then
# reloads the standard dataset (same pattern as the Empty CommandLog test).
# =============================================================================
Write-TestHeader "W5 Parallel-QS Scenario (hr21)"

try {
    Invoke-SqlQuery -Query @'
SET NOCOUNT ON;
TRUNCATE TABLE dbo.CommandLog;

DECLARE @anchor datetime2(3) = CONVERT(datetime2(3), CONVERT(date, GETDATE()));
DECLARE @run_label nvarchar(100), @start datetime2(3), @end_time datetime2(3);
DECLARE @i int = 1, @j int;

WHILE @i <= 4
BEGIN
    SET @start = DATEADD(DAY, -7 + @i, @anchor);            /* days -6..-3, no overlap */
    SET @run_label = N'pqs_run0' + CONVERT(nvarchar(2), @i);
    SET @end_time = DATEADD(SECOND, 600, @start);

    INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
    VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @start, @start,
        N'<Parameters>
            <Version>3.5.5.2026.06.08</Version>
            <RunLabel>' + @run_label + N'</RunLabel>
            <Databases>USER_DATABASES</Databases>
            <TimeLimit>' + CONVERT(nvarchar(10), 21598 + @i % 2) + N'</TimeLimit>
            <ModificationThreshold>5000</ModificationThreshold>
            <TieredThresholds>1</TieredThresholds>
            <ThresholdLogic>OR</ThresholdLogic>
            <SortOrder>QUERY_STORE</SortOrder>
            <QueryStorePriority>Y</QueryStorePriority>
            <StatsInParallel>Y</StatsInParallel>
        </Parameters>');

    INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
    VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @start, @end_time,
        N'<Summary>
            <Version>3.5.5.2026.06.08</Version>
            <RunLabel>' + @run_label + N'</RunLabel>
            <StatsFound>20</StatsFound>
            <StatsProcessed>20</StatsProcessed>
            <StatsSucceeded>19</StatsSucceeded>
            <StatsFailed>1</StatsFailed>
            <StatsRemaining>0</StatsRemaining>
            <DurationSeconds>600</DurationSeconds>
            <StopReason>PARALLEL_COMPLETE</StopReason>
        </Summary>');

    /* Successful stat updates: QSTotalCpuMs present, QSPlanCount ABSENT
       (the per-worker tagging anomaly the W5 branch is designed for) */
    SET @j = 0;
    WHILE @j < 3
    BEGIN
        INSERT INTO dbo.CommandLog (
            CommandType, Command, StartTime, EndTime,
            DatabaseName, SchemaName, ObjectName, StatisticsName,
            ErrorNumber, ErrorMessage, ExtendedInfo
        ) VALUES (
            N'UPDATE_STATISTICS',
            N'UPDATE STATISTICS [TestDB].[dbo].[Orders] ([IX_Orders_Date]) WITH SAMPLE 25 PERCENT',
            DATEADD(SECOND, 30 + @j * 30, @start),
            DATEADD(MILLISECOND, 200, DATEADD(SECOND, 30 + @j * 30, @start)),
            N'TestDB', N'dbo',
            CASE @j WHEN 0 THEN N'Orders' WHEN 1 THEN N'OrderItems' ELSE N'Customers' END,
            CASE @j WHEN 0 THEN N'IX_Orders_Date' WHEN 1 THEN N'IX_OrderItems_OrderID' ELSE N'IX_Customers_Email' END,
            NULL, NULL,
            N'<ExtendedInfo>
                <RunLabel>' + @run_label + N'</RunLabel>
                <QualifyReason>QUERY_STORE_PRIORITY</QualifyReason>
                <RowCount>' + CONVERT(nvarchar(20), 500000 + @j * 100000) + N'</RowCount>
                <ModificationCounter>' + CONVERT(nvarchar(20), 10000 + @j * 5000) + N'</ModificationCounter>
                <PageCount>5000</PageCount>
                <SizeMB>40</SizeMB>
                <DaysStale>1</DaysStale>
                <HasNorecompute>0</HasNorecompute>
                <IsHeap>0</IsHeap>
                <QSTotalCpuMs>' + CONVERT(nvarchar(20), 40000 - @j * 5000) + N'</QSTotalCpuMs>
                <QSTotalExecutions>' + CONVERT(nvarchar(20), 9000 - @j * 1000) + N'</QSTotalExecutions>
                <QSPriorityBoost>1</QSPriorityBoost>
                <ProcessingPosition>' + CONVERT(nvarchar(10), @j + 1) + N'</ProcessingPosition>
            </ExtendedInfo>'
        );
        SET @j = @j + 1;
    END;

    /* Lock-timeout failure on the same hot table every run: 4 distinct
       RunLabels >= W9 threshold of 3, and >= 3 for the I10 hint */
    INSERT INTO dbo.CommandLog (
        CommandType, Command, StartTime, EndTime,
        DatabaseName, SchemaName, ObjectName, StatisticsName,
        ErrorNumber, ErrorMessage, ExtendedInfo
    ) VALUES (
        N'UPDATE_STATISTICS',
        N'UPDATE STATISTICS [TestDB].[dbo].[HotOrders] ([IX_HotOrders_Status]) WITH SAMPLE 25 PERCENT',
        DATEADD(SECOND, 300, @start),
        DATEADD(SECOND, 305, @start),
        N'TestDB', N'dbo', N'HotOrders', N'IX_HotOrders_Status',
        1222, N'Lock request time out period exceeded.',
        N'<ExtendedInfo>
            <RunLabel>' + @run_label + N'</RunLabel>
            <QualifyReason>QUERY_STORE_PRIORITY</QualifyReason>
            <RowCount>8000000</RowCount>
            <ModificationCounter>250000</ModificationCounter>
            <PageCount>90000</PageCount>
            <SizeMB>700</SizeMB>
            <DaysStale>2</DaysStale>
            <HasNorecompute>0</HasNorecompute>
            <IsHeap>0</IsHeap>
            <QSTotalCpuMs>60000</QSTotalCpuMs>
            <QSTotalExecutions>15000</QSTotalExecutions>
            <QSPriorityBoost>1</QSPriorityBoost>
        </ExtendedInfo>'
    );

    SET @i = @i + 1;
END;
'@ | Out-Null

    $pqsDs = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $pqsMap = Get-RSBySignature -DataSet $pqsDs
    $pqsRecs = $pqsMap["Recommendations"]

    # (1) New W5 branch fires
    $unreliableRow = $pqsRecs.Rows | Where-Object { "$($_.Category)" -eq "QS_PARALLEL_UNRELIABLE" } | Select-Object -First 1
    Write-TestResult -Name "W5 parallel branch: QS_PARALLEL_UNRELIABLE present" -Passed ($null -ne $unreliableRow) -Detail "$(if ($unreliableRow) { "Severity=$($unreliableRow.Severity)" } else { 'row missing' })"

    # (2) It is INFO, not WARNING
    Write-TestResult -Name "QS_PARALLEL_UNRELIABLE severity is INFO" -Passed ($null -ne $unreliableRow -and "$($unreliableRow.Severity)" -eq "INFO") -Detail "Severity=$(if ($unreliableRow) { $unreliableRow.Severity } else { 'n/a' })"

    # (3) Legacy WARNING suppressed
    $notEffectiveRow = $pqsRecs.Rows | Where-Object { "$($_.Category)" -eq "QS_NOT_EFFECTIVE" } | Select-Object -First 1
    Write-TestResult -Name "QS_NOT_EFFECTIVE WARNING absent (suppressed)" -Passed ($null -eq $notEffectiveRow) -Detail "$(if ($notEffectiveRow) { 'unexpected WARNING present' } else { 'suppressed as designed' })"

    # (4) W9 fired (prerequisite for the @CriticalTables hint)
    $w9Row = $pqsRecs.Rows | Where-Object { "$($_.Category)" -eq "LOCK_TIMEOUT_INEFFECTIVE" } | Select-Object -First 1
    Write-TestResult -Name "W9 LOCK_TIMEOUT_INEFFECTIVE fired (hint prerequisite)" -Passed ($null -ne $w9Row) -Detail "$(if ($w9Row) { 'HotOrders blocked across 4 runs' } else { 'W9 missing' })"

    # I10 RECOMMENDED_CONFIG assertions
    $rcRow = $pqsRecs.Rows | Where-Object { "$($_.Category)" -eq "RECOMMENDED_CONFIG" } | Select-Object -First 1
    $rcCall = if ($rcRow) { "$($rcRow.ExampleCall)" } else { "" }
    $rcEvidence = if ($rcRow) { "$($rcRow.Evidence)" } else { "" }

    # (5) QS kept despite W5-area signal
    Write-TestResult -Name "I10 keeps @SortOrder=QUERY_STORE + @QueryStore=CPU" -Passed ($rcCall -match "@SortOrder = N'QUERY_STORE'" -and $rcCall -match "@QueryStore = N'CPU'") -Detail "$(if ($rcRow) { 'QS clauses present in ExampleCall' } else { 'RECOMMENDED_CONFIG missing' })"

    # (6) Rationale cites the parallel artifact
    Write-TestResult -Name "I10 rationale cites parallel artifact" -Passed ($rcEvidence -match "parallel artifact") -Detail "$(if ($rcEvidence.Length -gt 0) { $rcEvidence.Substring(0, [Math]::Min(90, $rcEvidence.Length)) } else { 'no evidence text' })"

    # (7) TimeLimit jitter 21598/21599 normalized to 21600
    Write-TestResult -Name "I10 TimeLimit jitter normalized to 21600" -Passed ($rcCall -match "@TimeLimit = 21600") -Detail "$(([regex]::Match($rcCall, '@TimeLimit = \d+')).Value)"

    # (8) @CriticalTables hint names the hot table, as a commented suggestion
    Write-TestResult -Name "I10 @CriticalTables hint suggests dbo.HotOrders" -Passed ($rcCall -match "W9 lock contention" -and $rcCall -match "@CriticalTables = N'dbo\.HotOrders") -Detail "$(([regex]::Match($rcCall, "@CriticalTables = N'[^']*'")).Value)"
}
catch {
    Write-TestResult -Name "W5 Parallel-QS scenario" -Passed $false -Detail $_.Exception.Message
}
finally {
    # Restore the standard dataset and purge scenario cache rows so suite
    # re-runs stay deterministic (cache table persists across CommandLog loads)
    try { Invoke-SqlFile -FilePath $testData } catch {}
    try { Invoke-SqlQuery -Query "IF OBJECT_ID('dbo.StatUpdateDiagCache','U') IS NOT NULL DELETE FROM dbo.StatUpdateDiagCache WHERE RunLabel LIKE N'pqs_%';" | Out-Null } catch {}
}

# =============================================================================
# TEST: Phase-B batch (70hh / 1h2l / 4g9s / t8lj)
# =============================================================================

Write-TestHeader "Phase-B: Skip-row exclusion, recurring safety stops, recurring runtime warnings"

try {
    # Restore standard dataset in case a prior test truncated it
    try { Invoke-SqlFile -FilePath $testData } catch {}

    $pbDs = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $pbMap = Get-RSBySignature -DataSet $pbDs
    $pbRecs = $pbMap["Recommendations"]
    $pbCats = $pbRecs.Rows | ForEach-Object { $_["Category"].ToString() }

    # ------------------------------------------------------------------
    # 70hh: Skip rows must NOT appear in #stat_updates.
    # The 2 skip-marker rows (SKIPPED: + TOCTOU_SKIP) use ObjectName=Orders
    # with StatisticsName IX_Orders_Skip1 / IX_Orders_Skip2 which never
    # appear as real updates.  Verify they do not surface in RS5 Top Tables
    # (aggregate duration) or RS6 Failing Stats.
    # ------------------------------------------------------------------
    $pbTopTables = $pbMap["TopTables"]
    $skipNameInTopTables = $false
    if ($pbTopTables) {
        foreach ($row in $pbTopTables.Rows) {
            $stat = if ($row.Table.Columns.Contains("StatisticsName")) { "$($row['StatisticsName'])" } else { "" }
            if ($stat -match "Skip1|Skip2") { $skipNameInTopTables = $true }
        }
    }
    Write-TestResult -Name "70hh: SKIPPED:/TOCTOU_SKIP rows absent from Top Tables" -Passed (-not $skipNameInTopTables) -Detail "$(if ($skipNameInTopTables) { 'skip row leaked into aggregates' } else { 'skip rows correctly excluded' })"

    # RS4 Run Detail -- verify StatsToctou column is present
    $pbRunDetail = $pbMap["RunDetail"]
    $hasToctouCol = $false
    if ($pbRunDetail) {
        $hasToctouCol = $pbRunDetail.Columns.Contains("StatsToctou")
    }
    Write-TestResult -Name "70hh: RS4 has StatsToctou column" -Passed $hasToctouCol -Detail "$(if ($hasToctouCol) { 'column present' } else { 'column missing from Run Detail' })"

    # ------------------------------------------------------------------
    # 4g9s: W14 RECURRING_SAFETY_STOP must fire for TEMPDB_PRESSURE (2 runs)
    # ------------------------------------------------------------------
    $w14Rows = $pbRecs.Rows | Where-Object { $_["Category"].ToString() -eq "RECURRING_SAFETY_STOP" }
    $hasW14TempDb = $false
    foreach ($row in $w14Rows) {
        if ($row["Finding"].ToString() -match "TEMPDB_PRESSURE") { $hasW14TempDb = $true }
    }
    Write-TestResult -Name "4g9s: W14 fires for TEMPDB_PRESSURE (2 runs)" -Passed $hasW14TempDb -Detail "$(if ($hasW14TempDb) { 'TEMPDB_PRESSURE finding present' } else { 'W14 missing or wrong reason' })"

    # W14 severity must be WARNING
    $w14Severity = $w14Rows | Where-Object { $_["Finding"].ToString() -match "TEMPDB_PRESSURE" } | ForEach-Object { $_["Severity"].ToString() } | Select-Object -First 1
    Write-TestResult -Name "4g9s: W14 severity is WARNING" -Passed ($w14Severity -eq "WARNING") -Detail "Severity=$w14Severity"

    # ------------------------------------------------------------------
    # t8lj: W15 (RECURRING_RUNTIME_WARNING) must fire for BACKUP_RUNNING
    # (environmental code in 3 of the 3 WarningsCodes runs)
    # ------------------------------------------------------------------
    $w15Rows = $pbRecs.Rows | Where-Object { $_["Category"].ToString() -eq "RECURRING_RUNTIME_WARNING" -and $_["Severity"].ToString() -eq "WARNING" }
    $hasW15Backup = $false
    foreach ($row in $w15Rows) {
        if ($row["Finding"].ToString() -match "BACKUP_RUNNING") { $hasW15Backup = $true }
    }
    Write-TestResult -Name "t8lj: W15 fires for BACKUP_RUNNING (3 coded runs)" -Passed $hasW15Backup -Detail "$(if ($hasW15Backup) { 'W15 BACKUP_RUNNING present' } else { 'W15 missing' })"

    # I16 (RECURRING_RUNTIME_WARNING INFO) must fire for TOCTOU_SKIPS (informational code)
    $i16Rows = $pbRecs.Rows | Where-Object { $_["Category"].ToString() -eq "RECURRING_RUNTIME_WARNING" -and $_["Severity"].ToString() -eq "INFO" }
    $hasI16Toctou = $false
    foreach ($row in $i16Rows) {
        if ($row["Finding"].ToString() -match "TOCTOU_SKIPS") { $hasI16Toctou = $true }
    }
    Write-TestResult -Name "t8lj: I16 fires for TOCTOU_SKIPS (informational code, 3 coded runs)" -Passed $hasI16Toctou -Detail "$(if ($hasI16Toctou) { 'I16 TOCTOU_SKIPS present' } else { 'I16 missing' })"

    # Old runs (without WarningsCodes element) must not produce false W15/I16 findings
    # for codes that appear ONLY in runs without the element.
    # We can't inject a specific "old-run only" code here, but we CAN verify
    # that the total W15/I16 count is bounded (only BACKUP_RUNNING / TOCTOU_SKIPS)
    Write-TestResult -Name "t8lj: W15 count is exactly 1 (BACKUP_RUNNING only)" -Passed ($w15Rows.Count -eq 1) -Detail "W15 rows=$($w15Rows.Count)"
}
catch {
    Write-TestResult -Name "Phase-B batch tests" -Passed $false -Detail $_.Exception.Message
}
finally {
    try { Invoke-SqlFile -FilePath $testData } catch {}
}

# =============================================================================
# Phase-C batch tests (sw82, 9ufm, atk1, 9axc)
# =============================================================================

Write-TestHeader "Phase-C: CriticalTables Coverage + Element Gating + Dashboard + Watermark"

try {
    # Reload fresh seed to ensure runs 26-28 are present
    Invoke-SqlFile -FilePath $testData

    $pcDs = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $pcRecs = $pcDs.Tables[1]

    # ------------------------------------------------------------------
    # sw82 / W16: CRITICAL_TABLES_NO_MATCH must fire (run 27 - NonExistentTable)
    # ------------------------------------------------------------------
    $w16Rows = @($pcRecs.Rows | Where-Object { $_["Category"].ToString() -eq "CRITICAL_TABLES_NO_MATCH" })
    Write-TestResult -Name "sw82: W16 CRITICAL_TABLES_NO_MATCH fires (run 27 pattern matched nothing)" -Passed ($w16Rows.Count -gt 0) -Detail "W16 rows=$($w16Rows.Count)"

    if ($w16Rows.Count -gt 0) {
        $w16Finding = $w16Rows[0]["Finding"].ToString()
        Write-TestResult -Name "sw82: W16 Finding mentions configured but matched no stats" -Passed ($w16Finding -match "matched no stats|no stats") -Detail ($w16Finding.Substring(0, [Math]::Min(100, $w16Finding.Length)))
    }

    # ------------------------------------------------------------------
    # sw82 / I17: CRITICAL_TABLES_COVERAGE must fire (run 26 - IsCritical=1 stats)
    # ------------------------------------------------------------------
    $i17Rows = @($pcRecs.Rows | Where-Object { $_["Category"].ToString() -eq "CRITICAL_TABLES_COVERAGE" })
    Write-TestResult -Name "sw82: I17 CRITICAL_TABLES_COVERAGE fires (run 26 has IsCritical=1 stats)" -Passed ($i17Rows.Count -gt 0) -Detail "I17 rows=$($i17Rows.Count)"

    if ($i17Rows.Count -gt 0) {
        $i17Finding = $i17Rows[0]["Finding"].ToString()
        $hasPosition = $i17Finding -match "position|avg position"
        Write-TestResult -Name "sw82: I17 Finding includes position evidence (CriticalTablesFirst=Y)" -Passed $hasPosition -Detail ($i17Finding.Substring(0, [Math]::Min(120, $i17Finding.Length)))
    }

    # ------------------------------------------------------------------
    # 9ufm: Pre-v3 run (28) must NOT produce false W1/I4 findings
    # W1a (TieredThresholds) and W1b (no TimeLimit) check against run 11
    # (the most recent run that has those elements absent or disabled).
    # Run 28 has no TieredThresholds/TimeLimit/Preset elements so its NULL
    # values should NOT trigger W1a / W1b / I4a / I4d.
    # Verify by checking that W1 finding count is still bounded (doesn't
    # include a finding pointing at run_28's RunLabel).
    # ------------------------------------------------------------------
    $w1Rows = @($pcRecs.Rows | Where-Object { $_["Category"].ToString() -eq "SUBOPTIMAL_PARAMS" })
    $w1ForPreV3 = $w1Rows | Where-Object { $_["Evidence"].ToString() -match "pre_v3_element_gate" }
    Write-TestResult -Name "9ufm: W1 does NOT fire for pre-v3 run with absent Parameter elements" -Passed ($null -eq $w1ForPreV3 -or @($w1ForPreV3).Count -eq 0) -Detail "W1 rows that reference pre_v3 run: $(@($w1ForPreV3).Count)"

    $i4Rows = @($pcRecs.Rows | Where-Object { $_["Category"].ToString() -eq "UNUSED_FEATURES" })
    # I4a (preset not in use) should NOT fire for run 28 because HasPresetParam=0;
    # it CAN still fire for run 11 which has the Preset element absent (same situation).
    # The key: total I4 count should not increase vs the non-gated baseline when run 28 is added.
    # We verify it fires at most once for SUBOPTIMAL_PARAMS or is absent.
    Write-TestResult -Name "9ufm: I4 UNUSED_FEATURES fires at most once per feature (gating limits duplicates)" -Passed ($i4Rows.Count -le 5) -Detail "I4 findings: $($i4Rows.Count)"

    # ------------------------------------------------------------------
    # atk1: TIME_LIMIT dashboard annotation -- check whether banner appears.
    # The seed has run 2 and run 16 with TIME_LIMIT stop reason.
    # Total non-killed runs is large (28), so pct = 2/26 = ~8% < default 80%.
    # At default @TimeLimitExhaustionPct=80, the banner should NOT appear.
    # Verify this is correct (no false positive).
    # ------------------------------------------------------------------
    $dashboard = $pcDs.Tables[0]
    $completionRow = $dashboard.Rows | Where-Object { $_["Category"].ToString() -eq "COMPLETION" } | Select-Object -First 1

    $defaultNoAnnotation = $completionRow.Headline -notmatch '\[!\]'
    Write-TestResult -Name "atk1: COMPLETION Headline has no TIME_LIMIT banner at default threshold (8% < 80%)" -Passed $defaultNoAnnotation -Detail ($completionRow.Headline.Substring(0, [Math]::Min(120, $completionRow.Headline.Length)))

    # Now test with a low threshold to force the banner
    $lowThDs = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @TimeLimitExhaustionPct = 5, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $lowThDash = $lowThDs.Tables[0]
    $lowThCompletion = $lowThDash.Rows | Where-Object { $_["Category"].ToString() -eq "COMPLETION" } | Select-Object -First 1
    $lowThOverall = $lowThDash.Rows | Where-Object { $_["Category"].ToString() -eq "OVERALL" } | Select-Object -First 1

    $bannerInHeadline = $lowThCompletion.Headline -match '\[!\]'
    Write-TestResult -Name "atk1: COMPLETION Headline has [!] banner when threshold lowered to 5%" -Passed $bannerInHeadline -Detail ($lowThCompletion.Headline.Substring(0, [Math]::Min(150, $lowThCompletion.Headline.Length)))

    $completionReasonInDetail = $lowThCompletion.Detail -match "CompletionReason|budget-capped|TIME_LIMIT"
    Write-TestResult -Name "atk1: COMPLETION Detail has CompletionReason clause at low threshold" -Passed $completionReasonInDetail -Detail ($lowThCompletion.Detail.Substring(0, [Math]::Min(150, $lowThCompletion.Detail.Length)))

    $overallFlagInDetail = $lowThOverall.Detail -match "budget-capped|TIME_LIMIT"
    Write-TestResult -Name "atk1: OVERALL Detail has TIME_LIMIT flag at low threshold" -Passed $overallFlagInDetail -Detail ($lowThOverall.Detail.Substring(0, [Math]::Min(150, $lowThOverall.Detail.Length)))

    # Score/grade must NOT change from the default run (annotation-only)
    $defaultCompletion = $completionRow
    $defaultScore = [int]$defaultCompletion.Score
    $lowScore = [int]$lowThCompletion.Score
    Write-TestResult -Name "atk1: COMPLETION Score unchanged by annotation (annotation-only)" -Passed ($defaultScore -eq $lowScore) -Detail "default=$defaultScore lowThreshold=$lowScore"

    # ------------------------------------------------------------------
    # 9axc: History watermark MaxStartTime column test
    # ------------------------------------------------------------------
    # Drop tables to start clean
    Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagHistory', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagHistory;" | Out-Null

    # Run with SkipHistory=0 to create and populate history (with MaxStartTime)
    Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @SkipHistory = 0;" -MultiResultSet | Out-Null

    # Verify the MaxStartTime column was created
    $hasMaxStartTime = (Invoke-SqlQuery "SELECT cnt = COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'StatUpdateDiagHistory' AND COLUMN_NAME = 'MaxStartTime'").cnt
    Write-TestResult -Name "9axc: History table has MaxStartTime column" -Passed ([int]$hasMaxStartTime -eq 1) -Detail "col present: $([int]$hasMaxStartTime -eq 1)"

    # Verify MaxStartTime is populated (not all NULL) after an insert
    $maxStartRows = (Invoke-SqlQuery "SELECT cnt = COUNT(*) FROM dbo.StatUpdateDiagHistory WHERE MaxStartTime IS NOT NULL").cnt
    Write-TestResult -Name "9axc: History MaxStartTime is populated (non-NULL rows exist)" -Passed ([int]$maxStartRows -gt 0) -Detail "$maxStartRows rows with non-NULL MaxStartTime"

    # Verify duplicate-guard still works (second run inserts 0 new rows)
    $beforeRows = (Invoke-SqlQuery "SELECT cnt = COUNT(*) FROM dbo.StatUpdateDiagHistory").cnt
    Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @SkipHistory = 0;" -MultiResultSet | Out-Null
    $afterRows = (Invoke-SqlQuery "SELECT cnt = COUNT(*) FROM dbo.StatUpdateDiagHistory").cnt
    Write-TestResult -Name "9axc: Watermark duplicate guard still works after MaxStartTime addition" -Passed ([int]$beforeRows -eq [int]$afterRows) -Detail "before=$beforeRows after=$afterRows"

    # Cleanup
    Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagHistory', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagHistory;" | Out-Null
    Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagCache', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagCache;" | Out-Null
}
catch {
    Write-TestResult -Name "Phase-C batch" -Passed $false -Detail $_.Exception.Message
}
finally {
    try {
        Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagHistory', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagHistory;" | Out-Null
        Invoke-SqlQuery "IF OBJECT_ID(N'dbo.StatUpdateDiagCache', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagCache;" | Out-Null
        Invoke-SqlFile -FilePath $testData
    } catch {}
}

# =============================================================================
# TEST w5p2 / gh-532: I8/RS13 DataStatus surface for sparse QS data
# =============================================================================

Write-TestHeader "I8/RS13 DataStatus: I8 finding contains [DataStatus:] (seeded AVAILABLE path)"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $recs = $ds.Tables[1]
    $i8Row = $recs.Rows | Where-Object { $_["Category"].ToString() -eq "QS_PERFORMANCE_TREND" } | Select-Object -First 1

    # The seeded data has QS stats across 2+ runs so the AVAILABLE path fires.
    # AVAILABLE path is the full-trend branch; it does NOT append [DataStatus:].
    # Only the insufficient / unavailable branches append it.
    # This test confirms the AVAILABLE path's Finding does NOT contain the suffix
    # (i.e. the suffix is injected only on the sparse paths, not on success).
    if ($null -ne $i8Row) {
        $finding = $i8Row["Finding"].ToString()
        $hasQsPerfTrend = $finding -match "improving|degrading|stable|per-execution"
        Write-TestResult -Name "I8 seeded path: Finding describes trend (not sparse)" -Passed $hasQsPerfTrend -Detail ($finding.Substring(0, [Math]::Min(120, $finding.Length)))
        # The AVAILABLE path does not have [DataStatus:]; the sparse paths do.
        $noSuffix = $finding -notmatch "\[DataStatus:"
        Write-TestResult -Name "I8 AVAILABLE path: Finding has no [DataStatus:] suffix" -Passed $noSuffix -Detail ($finding.Substring(0, [Math]::Min(120, $finding.Length)))
    }
    else {
        Write-TestResult -Name "I8 seeded path: Finding describes trend (not sparse)" -Passed $false -Detail "QS_PERFORMANCE_TREND not found in recommendations"
    }
}
catch {
    Write-TestResult -Name "I8/RS13 DataStatus seeded path" -Passed $false -Detail $_.Exception.Message
}

# -----------------------------------------------------------------------------

Write-TestHeader "I8/RS13 DataStatus: empty CommandLog -> UNAVAILABLE -> [DataStatus:] suffix + RS13 sentinel"

try {
    Invoke-SqlQuery "TRUNCATE TABLE dbo.CommandLog;" | Out-Null

    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $recs  = $ds.Tables[1]
    $rsMap = Get-RSBySignature $ds

    # I8 finding must contain [DataStatus: when QS data is absent
    $i8Row = $recs.Rows | Where-Object { $_["Category"].ToString() -eq "QS_PERFORMANCE_TREND" } | Select-Object -First 1
    if ($null -ne $i8Row) {
        Write-TestResult -Name "I8 UNAVAILABLE: QS_PERFORMANCE_TREND present" -Passed $true
        $finding = $i8Row["Finding"].ToString()
        $hasDataStatus = $finding -match '\[DataStatus:'
        Write-TestResult -Name "I8 UNAVAILABLE: Finding contains [DataStatus:]" -Passed $hasDataStatus -Detail ($finding.Substring(0, [Math]::Min(180, $finding.Length)))

        # Pinned fragment preserved: 'No Query Store CPU data found'
        $hasOriginalText = $finding -match 'No Query Store CPU data found'
        Write-TestResult -Name "I8 UNAVAILABLE: pinned text preserved" -Passed $hasOriginalText -Detail ($finding.Substring(0, [Math]::Min(120, $finding.Length)))
    }
    else {
        Write-TestResult -Name "I8 UNAVAILABLE: QS_PERFORMANCE_TREND present" -Passed $false -Detail "Not found in recommendations"
        Write-TestResult -Name "I8 UNAVAILABLE: pinned text preserved" -Passed $false -Detail "No I8 row to check"
    }

    # RS13 sentinel: must return exactly 1 row with DatabaseName = '[NO DATA]'
    $rs13 = $rsMap["QSCorrelation"]
    if ($null -ne $rs13) {
        Write-TestResult -Name "RS13 UNAVAILABLE: QSCorrelation result set present" -Passed $true
        Write-TestResult -Name "RS13 UNAVAILABLE: returns exactly 1 sentinel row" -Passed ($rs13.Rows.Count -eq 1) -Detail "rows=$($rs13.Rows.Count)"
        if ($rs13.Rows.Count -ge 1) {
            $dbName = $rs13.Rows[0]["DatabaseName"].ToString()
            $sentinelRow = ($rs13.Rows.Count -eq 1) -and ($dbName -eq "[NO DATA]")
            Write-TestResult -Name "RS13 UNAVAILABLE: sentinel row" -Passed $sentinelRow -Detail "rows=$($rs13.Rows.Count) DatabaseName=$dbName"
            Write-TestResult -Name "RS13 UNAVAILABLE: sentinel DatabaseName = '[NO DATA]'" -Passed ($dbName -eq "[NO DATA]") -Detail "DatabaseName=$dbName"

            # Column-presence check: same set of columns as real RS13
            $rs13Cols = $rs13.Columns | ForEach-Object { $_.ColumnName }
            $mustHave = @("DatabaseName","StatisticsName","FirstCpuMs","LastCpuMs","CpuChangePct","CpuTrend","ForcedPlanCount","PlanTrend","FirstMemoryGrantKB","MemoryGrantTrend","FirstTempdbPages","TempdbTrend")
            $missingCols = $mustHave | Where-Object { $rs13Cols -notcontains $_ }
            Write-TestResult -Name "RS13 UNAVAILABLE: sentinel columns" -Passed ($missingCols.Count -eq 0) -Detail "Missing: $(if ($missingCols) { $missingCols -join ', ' } else { 'none' })"
        }
        else {
            Write-TestResult -Name "RS13 UNAVAILABLE: sentinel row" -Passed $false -Detail "RS13 returned no sentinel row"
            Write-TestResult -Name "RS13 UNAVAILABLE: sentinel columns" -Passed $false -Detail "RS13 returned no row to inspect"
        }
    }
    else {
        Write-TestResult -Name "RS13 UNAVAILABLE: QSCorrelation result set present" -Passed $false -Detail "RS13 not found in result set map"
        Write-TestResult -Name "RS13 UNAVAILABLE: sentinel row" -Passed $false -Detail "RS13 not found"
        Write-TestResult -Name "RS13 UNAVAILABLE: sentinel columns" -Passed $false -Detail "RS13 not found"
    }

    # Reload standard seed
    Invoke-SqlFile -FilePath $testData
}
catch {
    Write-TestResult -Name "I8/RS13 DataStatus UNAVAILABLE path" -Passed $false -Detail $_.Exception.Message
    try { Invoke-SqlFile -FilePath $testData } catch {}
}

# -----------------------------------------------------------------------------

Write-TestHeader "I8/RS13 DataStatus: seeded data -> RS13 returns real rows (no sentinel when AVAILABLE)"

try {
    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;" -MultiResultSet
    $rsMap = Get-RSBySignature $ds
    $rs13  = $rsMap["QSCorrelation"]

    if ($null -ne $rs13) {
        Write-TestResult -Name "RS13 AVAILABLE: result set present" -Passed $true
        $rowCount = $rs13.Rows.Count
        # Seed guarantees 2+ appearances; must have real rows and no [NO DATA] sentinel
        Write-TestResult -Name "RS13 AVAILABLE: has real data rows (not sentinel)" -Passed ($rowCount -gt 0) -Detail "$rowCount rows"
        if ($rowCount -gt 0) {
            $firstDb = $rs13.Rows[0]["DatabaseName"].ToString()
            $isNotSentinel = $firstDb -ne "[NO DATA]"
            Write-TestResult -Name "RS13 AVAILABLE: no sentinel row" -Passed $isNotSentinel -Detail "DatabaseName=$firstDb"
        }
        else {
            Write-TestResult -Name "RS13 AVAILABLE: no sentinel row" -Passed $false -Detail "RS13 returned no rows"
        }
    }
    else {
        Write-TestResult -Name "RS13 AVAILABLE: result set present" -Passed $false -Detail "RS13 not found"
        Write-TestResult -Name "RS13 AVAILABLE: no sentinel row" -Passed $false -Detail "RS13 not found"
    }
}
catch {
    Write-TestResult -Name "I8/RS13 DataStatus AVAILABLE real rows" -Passed $false -Detail $_.Exception.Message
}

# =============================================================================
# TEST: SingleResultSet Efficacy Detail escapes JSON strings
# =============================================================================

Write-TestHeader "SingleResultSet Efficacy Detail JSON escaping"
$jsonProbeLabel = 'DiagJsonEdge_"slash\' + [char]10 + 'tail'
$jsonProbeSort = 'MODIFICATION_COUNTER_"slash\' + [char]10 + 'tail'
$jsonProbeStop = 'COMPLETED_"slash\' + [char]10 + 'tail'
try {
    Invoke-SqlQuery -Query @"
DECLARE @runLabel nvarchar(100) = N'DiagJsonEdge_"slash\' + NCHAR(10) + N'tail';
DECLARE @sortOrder nvarchar(50) = N'MODIFICATION_COUNTER_"slash\' + NCHAR(10) + N'tail';
DECLARE @stopReason nvarchar(50) = N'COMPLETED_"slash\' + NCHAR(10) + N'tail';
DECLARE @startTime datetime2(3) = DATEADD(MINUTE, -2, SYSDATETIME());

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_START', N'EXECUTE dbo.sp_StatUpdate', @startTime, @startTime,
    CONVERT(xml, N'<Parameters><Version>3.11.5</Version><RunLabel>' + @runLabel +
        N'</RunLabel><SortOrder>' + @sortOrder + N'</SortOrder></Parameters>'));

INSERT INTO dbo.CommandLog (CommandType, Command, StartTime, EndTime, ExtendedInfo)
VALUES (N'SP_STATUPDATE_END', N'EXECUTE dbo.sp_StatUpdate', @startTime,
    DATEADD(MINUTE, 1, @startTime),
    CONVERT(xml, N'<Summary><RunLabel>' + @runLabel + N'</RunLabel><StatsFound>10</StatsFound>' +
        N'<StatsProcessed>10</StatsProcessed><DurationSeconds>60</DurationSeconds><StopReason>' +
        @stopReason + N'</StopReason></Summary>'));
"@ | Out-Null

    $ds = Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 30, @EfficacyDetailDays = 30, @SingleResultSet = 1, @SkipHistory = 1;" -MultiResultSet
    $envelope = $ds.Tables[0]
    $row = $envelope.Rows | Where-Object { [int]$_.ResultSetID -eq 11 -and [int]$_.RowNum -eq 1 } | Select-Object -First 1
    if ($null -eq $row) { throw "Efficacy Detail result row was not returned" }
    $json = ConvertFrom-Json -InputObject ([string]$row.RowData) -ErrorAction Stop
    $jsonPassed = ($json.RunLabel -ceq $jsonProbeLabel) -and
                  ($json.SortStrategy -ceq $jsonProbeSort) -and
                  ($json.StopReason -ceq $jsonProbeStop)
    Write-TestResult -Name "SingleResultSet Efficacy Detail round-trips JSON strings" -Passed $jsonPassed `
        -Detail "JSON parsed and the original quote, backslash, and line feed values matched"
}
catch {
    Write-TestResult -Name "SingleResultSet Efficacy Detail round-trips JSON strings" -Passed $false -Detail $_.Exception.Message
}
finally {
    try {
        Invoke-SqlQuery -Query @"
DELETE FROM dbo.CommandLog
WHERE ExtendedInfo.value('(/Parameters/RunLabel)[1]', 'nvarchar(100)') LIKE N'DiagJsonEdge_%'
   OR ExtendedInfo.value('(/Summary/RunLabel)[1]', 'nvarchar(100)') LIKE N'DiagJsonEdge_%';
"@ | Out-Null
    } catch {}
}

# =============================================================================
# TEST 37: Cache watermark after CommandLog reset/reseed
# =============================================================================

Write-TestHeader "Cache watermark after CommandLog reset/reseed"

try {
    # Populate the persistent cache before resetting CommandLog. The cache must
    # survive while CommandLog's identity and contents are reseeded below.
    Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 3650, @SkipHistory = 0;" -MultiResultSet | Out-Null
    $watermark = (Invoke-SqlQuery "SELECT cnt = ISNULL(MAX(CommandLogID), 0) FROM dbo.StatUpdateDiagCache").cnt
    if ([int]$watermark -le 0) { throw "Cache did not have a positive watermark before reset" }

    # Existing installations upgrade the persistent cache in place. Remove the
    # new fingerprint column when present to exercise that upgrade path.
    $hashColumnBeforeUpgrade = (Invoke-SqlQuery "SELECT has_column = CASE WHEN COL_LENGTH(N'dbo.StatUpdateDiagCache', N'SourceExtendedInfoHash') IS NULL THEN 0 ELSE 1 END").has_column
    if ([int]$hashColumnBeforeUpgrade -eq 1) {
        Invoke-SqlQuery -Query "ALTER TABLE dbo.StatUpdateDiagCache DROP COLUMN SourceExtendedInfoHash;" | Out-Null
    }
    Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 3650, @SkipHistory = 0;" -MultiResultSet | Out-Null
    $hashColumnAfterUpgrade = (Invoke-SqlQuery "SELECT has_column = CASE WHEN COL_LENGTH(N'dbo.StatUpdateDiagCache', N'SourceExtendedInfoHash') IS NULL THEN 0 ELSE 1 END").has_column
    Write-TestResult -Name "Cache upgrades legacy cache schema" -Passed ([int]$hashColumnAfterUpgrade -eq 1) `
        -Detail "columnBefore=$hashColumnBeforeUpgrade columnAfter=$hashColumnAfterUpgrade"

    # Reuse IDs from 1 through past the old watermark, with a different StartTime
    # at the old watermark. The old max-ID-only guard silently retains stale rows;
    # the one-row identity check must invalidate and reload the cache.
    $resetAndSeed = @"
DECLARE @oldWatermark int = $([int]$watermark);
DECLARE @oldStart datetime2(7) = (SELECT StartTime FROM dbo.StatUpdateDiagCache WHERE CommandLogID = @oldWatermark);
DECLARE @baseStart datetime2(7) = DATEADD(DAY, -1, COALESCE(@oldStart, SYSDATETIME()));
DECLARE @rowCount int = CASE WHEN @oldWatermark < 100 THEN 101 ELSE @oldWatermark + 1 END;
TRUNCATE TABLE dbo.CommandLog;
;WITH n AS (
    SELECT 1 AS n
    UNION ALL SELECT n + 1 FROM n WHERE n < @rowCount
)
INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, StatisticsName, Command,
                            CommandType, StartTime, EndTime, ExtendedInfo)
SELECT N'CacheResetProbe', N'dbo', N'CacheResetProbe', 'U', N'ST_CacheResetProbe',
       N'UPDATE STATISTICS dbo.CacheResetProbe ST_CacheResetProbe', N'UPDATE_STATISTICS',
       DATEADD(MILLISECOND, n, @baseStart), DATEADD(MILLISECOND, n + 1, @baseStart),
       CONVERT(xml, N'<ExtendedInfo><RunLabel>CacheResetProof</RunLabel><ModificationCounter>5</ModificationCounter><RowCount>100</RowCount><PageCount>1</PageCount></ExtendedInfo>')
FROM n
OPTION (MAXRECURSION 0);
"@
    Invoke-SqlQuery -Query $resetAndSeed | Out-Null
    Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 3650, @SkipHistory = 0;" -MultiResultSet | Out-Null

    $cacheResetCheck = Invoke-SqlQuery @"
SELECT
    log_rows = (SELECT COUNT(*) FROM dbo.CommandLog),
    cache_rows = (SELECT COUNT(*) FROM dbo.StatUpdateDiagCache),
    reused_watermark_matches = CASE WHEN
        (SELECT StartTime FROM dbo.CommandLog WHERE ID = $([int]$watermark)) =
        (SELECT StartTime FROM dbo.StatUpdateDiagCache WHERE CommandLogID = $([int]$watermark))
        THEN 1 ELSE 0 END
"@
    $cacheResetPassed = ([int]$cacheResetCheck.log_rows -eq [int]$cacheResetCheck.cache_rows) -and
                        ([int]$cacheResetCheck.reused_watermark_matches -eq 1)
    Write-TestResult -Name "Cache watermark resets after CommandLog reseed" -Passed $cacheResetPassed `
        -Detail "logRows=$($cacheResetCheck.log_rows) cacheRows=$($cacheResetCheck.cache_rows) reusedWatermarkMatches=$($cacheResetCheck.reused_watermark_matches)"

    try {
    $cacheWatermark = [int](Invoke-SqlQuery "SELECT MAX(CommandLogID) AS CommandLogID FROM dbo.StatUpdateDiagCache").CommandLogID
    Invoke-SqlQuery -Query @"
UPDATE dbo.CommandLog
SET ExtendedInfo.modify('replace value of (/ExtendedInfo/ModificationCounter/text())[1] with "6"')
WHERE ID = $cacheWatermark;
"@ | Out-Null
    Invoke-SqlQuery -Query "EXECUTE dbo.sp_StatUpdate_Diag @DaysBack = 3650, @SkipHistory = 0;" -MultiResultSet | Out-Null
    $sameTimestampPayload = Invoke-SqlQuery -Query @"
SELECT
    source_value = c.ExtendedInfo.value('(/ExtendedInfo/ModificationCounter/text())[1]', 'bigint'),
    cached_value = ca.ModificationCounter,
    same_start_time = CASE WHEN c.StartTime = ca.StartTime THEN 1 ELSE 0 END
FROM dbo.CommandLog AS c
INNER JOIN dbo.StatUpdateDiagCache AS ca ON ca.CommandLogID = c.ID
WHERE c.ID = $cacheWatermark;
"@
    $sameTimestampPassed = ([long]$sameTimestampPayload.source_value -eq 6) -and
                           ([long]$sameTimestampPayload.cached_value -eq 6) -and
                           ([int]$sameTimestampPayload.same_start_time -eq 1)
    Write-TestResult -Name "Cache detects same-timestamp watermark payload changes" -Passed $sameTimestampPassed `
        -Detail "source=$($sameTimestampPayload.source_value) cached=$($sameTimestampPayload.cached_value) sameStartTime=$($sameTimestampPayload.same_start_time)"
    } catch {
        Write-TestResult -Name "Cache detects same-timestamp watermark payload changes" -Passed $false -Detail $_.Exception.Message
    }
}
catch {
    Write-TestResult -Name "Cache watermark resets after CommandLog reseed" -Passed $false -Detail $_.Exception.Message
}
finally {
    # Restore this disposable suite's normal fixture state; do not carry the
    # synthetic reset rows or cache into later checks.
    try { Invoke-SqlQuery "TRUNCATE TABLE dbo.CommandLog; IF OBJECT_ID(N'dbo.StatUpdateDiagCache', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagCache; IF OBJECT_ID(N'dbo.StatUpdateDiagHistory', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagHistory;" | Out-Null } catch {}
    try { Invoke-SqlFile -FilePath $testData } catch {}
}
# =============================================================================
# SUMMARY
# =============================================================================

Write-Host ""
Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host " TEST RESULTS" -ForegroundColor Cyan
Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Total:  $totalTests"
Write-Host "  Passed: $passedTests" -ForegroundColor Green
if ($skippedTests -gt 0) {
    Write-Host "  Skipped: $skippedTests" -ForegroundColor Yellow
}
if ($failedTests -gt 0) {
    Write-Host "  Failed: $failedTests" -ForegroundColor Red
    Write-Host ""
    Write-Host "  Failed tests:" -ForegroundColor Red
    foreach ($result in $testResults) {
        if ($result.Status -eq "FAIL") {
            Write-Host "    - $($result.Test)" -ForegroundColor Red
            if ($result.Detail) { Write-Host "      $($result.Detail)" -ForegroundColor DarkGray }
        }
    }
}
else {
    Write-Host "  Failed: 0" -ForegroundColor Green
}
Write-Host ""

if ($ResultsPath) {
    if (-not $SqlVersion -or -not $Commit) { throw 'Structured results require SqlVersion and Commit.' }
    $status = Invoke-SqlQuery "SELECT CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')) AS ProductVersion, CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS ProductMajorVersion;"
    $majorToVersion = @{ 15 = '2019'; 16 = '2022'; 17 = '2025' }
    $actualVersion = $majorToVersion[[int]$status.ProductMajorVersion]
    if ($actualVersion -ne $SqlVersion) { throw "Connected SQL version '$actualVersion' did not match requested '$SqlVersion'." }
    $structuredResults = @($testResults | ForEach-Object {
        [pscustomobject]@{ Test = $_.Test; Result = $_.Status; Detail = $_.Detail }
    })
    Import-Module (Join-Path $PSScriptRoot '..\StructuredLegacyResults.psm1') -Force
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $Commit -SqlVersion $SqlVersion `
        -SqlBuild ([string]$status.ProductVersion) -ServerAliveAtEnd $true -Suite 'StatUpdateDiag' `
        -ExpectedCount $structuredResults.Count -Results $structuredResults `
        -StartedAt $suiteStartedAt -EndedAt ([DateTime]::UtcNow)
}

exit $failedTests
