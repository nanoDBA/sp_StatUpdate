# Test-AvailabilityGroupSupport.ps1
# Tests AVAILABILITY_GROUP_DATABASES keyword and AG secondary exclusion
# Requires: Docker, ag_docker_setup containers running

param(
    [string]$PrimaryHost = "localhost",
    [int]$PrimaryPort = 2500,
    [string]$SecondaryHost = "localhost",
    [int]$SecondaryPort = 2600,
    [string]$Password = $env:SPSTATUPDATE_TEST_SQL_PASSWORD,
    [string]$LogDir = $null,
    [switch]$SkipDeploy,
    [switch]$StartContainers
)

$ErrorActionPreference = "Continue"
if (-not $Password) { throw 'Set SPSTATUPDATE_TEST_SQL_PASSWORD or pass -Password.' }
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

if (-not $LogDir) {
    $LogDir = Join-Path $PSScriptRoot "ag_test_logs_$timestamp"
}

# Create log directory
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

$summaryLog = Join-Path $LogDir "00_SUMMARY.txt"
$results = @()

function Write-Log {
    param([string]$Message, [string]$Color = "White")
    $ts = Get-Date -Format "HH:mm:ss"
    Write-Host "[$ts] $Message" -ForegroundColor $Color
    "[$ts] $Message" | Out-File -FilePath $summaryLog -Append -Encoding UTF8
}

function Invoke-SqlCmd {
    param(
        [string]$Container,
        [string]$Query,
        [string]$Database = "master"
    )

    $result = docker exec $Container /opt/mssql-tools18/bin/sqlcmd `
        -S localhost -U sa -P $Password -C -d $Database `
        -Q "$Query" 2>&1

    return $result
}

function Test-Scenario {
    param(
        [string]$Name,
        [string]$Container,
        [string]$Query,
        [string]$ExpectedPattern,
        [string]$LogFile
    )

    Write-Log "  Running: $Name" "Cyan"

    $output = Invoke-SqlCmd -Container $Container -Query $Query
    $output | Out-File -FilePath (Join-Path $LogDir "$LogFile.log") -Encoding UTF8

    $passed = $false
    if ($ExpectedPattern) {
        $passed = ($output | Out-String) -match $ExpectedPattern
    } else {
        $passed = $LASTEXITCODE -eq 0
    }

    $status = if ($passed) { "PASS" } else { "FAIL" }
    $color = if ($passed) { "Green" } else { "Red" }

    Write-Log "  [$status] $Name" $color

    return @{
        Name = $Name
        Container = $Container
        Passed = $passed
        LogFile = "$LogFile.log"
    }
}

# ============================================================
# HEADER
# ============================================================
Write-Log "============================================================"
Write-Log "sp_StatUpdate Availability Group Test Suite"
Write-Log "Started: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Log "Log Directory: $LogDir"
Write-Log "============================================================"
Write-Log ""

# ============================================================
# START CONTAINERS (if requested)
# ============================================================
if ($StartContainers) {
    Write-Log "Starting AG containers..." "Yellow"
    Push-Location $PSScriptRoot
    docker-compose up --build -d 2>&1 | Out-File -FilePath (Join-Path $LogDir "docker_startup.log") -Encoding UTF8
    Pop-Location

    Write-Log "Waiting 60 seconds for AG initialization..."
    Start-Sleep -Seconds 60
}

# ============================================================
# VERIFY AG IS RUNNING
# ============================================================
Write-Log "Verifying AG status..." "Yellow"

$agStatus = Invoke-SqlCmd -Container "db1" -Query @"
SELECT ag.name AS AGName, ar.replica_server_name, ars.role_desc
FROM sys.availability_groups ag
JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id
LEFT JOIN sys.dm_hadr_availability_replica_states ars ON ar.replica_id = ars.replica_id;
"@

$agStatus | Out-File -FilePath (Join-Path $LogDir "01_ag_status.log") -Encoding UTF8

if (($agStatus | Out-String) -match "PRIMARY" -and ($agStatus | Out-String) -match "SECONDARY") {
    Write-Log "  [OK] AG1 is running with PRIMARY and SECONDARY replicas" "Green"
} else {
    Write-Log "  [ERROR] AG not properly configured. Run with -StartContainers" "Red"
    Write-Log "AG Status Output:" "Yellow"
    $agStatus | ForEach-Object { Write-Log "    $_" }
    exit 1
}

# ============================================================
# DEPLOY sp_StatUpdate
# ============================================================
if (-not $SkipDeploy) {
    Write-Log ""
    Write-Log "Deploying sp_StatUpdate to containers..." "Yellow"

    $spPath = Join-Path $PSScriptRoot "..\..\src\sp_StatUpdate.sql"
    $spPath = (Resolve-Path $spPath).Path

    # Deploy to db1 (primary)
    docker cp $spPath "db1:/tmp/sp_StatUpdate.sql" 2>&1 | Out-Null
    $deployResult1 = docker exec db1 /opt/mssql-tools18/bin/sqlcmd `
        -S localhost -U sa -P $Password -C -d master `
        -i /tmp/sp_StatUpdate.sql 2>&1

    if ($LASTEXITCODE -eq 0 -or ($deployResult1 | Out-String) -match "Command.*completed") {
        Write-Log "  [OK] Deployed to db1 (primary)" "Green"
    } else {
        Write-Log "  [WARN] Deploy to db1 may have issues" "Yellow"
    }
    $deployResult1 | Out-File -FilePath (Join-Path $LogDir "02_deploy_db1.log") -Encoding UTF8

    # Deploy to db2 (secondary)
    docker cp $spPath "db2:/tmp/sp_StatUpdate.sql" 2>&1 | Out-Null
    $deployResult2 = docker exec db2 /opt/mssql-tools18/bin/sqlcmd `
        -S localhost -U sa -P $Password -C -d master `
        -i /tmp/sp_StatUpdate.sql 2>&1

    if ($LASTEXITCODE -eq 0 -or ($deployResult2 | Out-String) -match "Command.*completed") {
        Write-Log "  [OK] Deployed to db2 (secondary)" "Green"
    } else {
        Write-Log "  [WARN] Deploy to db2 may have issues" "Yellow"
    }
    $deployResult2 | Out-File -FilePath (Join-Path $LogDir "02_deploy_db2.log") -Encoding UTF8
}

# ============================================================
# TEST SCENARIOS
# ============================================================
Write-Log ""
Write-Log "Running AG test scenarios..." "Yellow"
Write-Log ""

# Test 1: AVAILABILITY_GROUP_DATABASES on PRIMARY
Write-Log ">>> TEST 1: AVAILABILITY_GROUP_DATABASES on PRIMARY <<<" "Cyan"
$results += Test-Scenario `
    -Name "AG keyword finds Sales database on primary" `
    -Container "db1" `
    -Query @"
EXEC dbo.sp_StatUpdate
    @Databases = N'AVAILABILITY_GROUP_DATABASES',
    @ModificationThreshold = 0,
    @LogToTable = N'N',
    @Execute = N'N',
    @Debug = 1;
"@ `
    -ExpectedPattern "Databases:\s+1 selected|Scanning database: Sales" `
    -LogFile "03_primary_ag_keyword"

Write-Log ""

# Test 2: AVAILABILITY_GROUP_DATABASES on SECONDARY (should exclude)
Write-Log ">>> TEST 2: AVAILABILITY_GROUP_DATABASES on SECONDARY <<<" "Cyan"
$results += Test-Scenario `
    -Name "AG databases excluded on secondary replica" `
    -Container "db2" `
    -Query @"
EXEC dbo.sp_StatUpdate
    @Databases = N'AVAILABILITY_GROUP_DATABASES',
    @ModificationThreshold = 0,
    @LogToTable = N'N',
    @Execute = N'N',
    @Debug = 1;
"@ `
    -ExpectedPattern "AG Warning|excluded.*secondary|Databases:\s+0 selected|No databases matched|AG secondary replicas" `
    -LogFile "04_secondary_ag_keyword"

Write-Log ""

# Test 3: ALL_DATABASES on PRIMARY (should include AG database)
Write-Log ">>> TEST 3: ALL_DATABASES on PRIMARY <<<" "Cyan"
$results += Test-Scenario `
    -Name "ALL_DATABASES includes AG database on primary" `
    -Container "db1" `
    -Query @"
EXEC dbo.sp_StatUpdate
    @Databases = N'ALL_DATABASES',
    @ModificationThreshold = 0,
    @LogToTable = N'N',
    @Execute = N'N',
    @Debug = 1;
"@ `
    -ExpectedPattern "Scanning database: Sales" `
    -LogFile "05_primary_all_databases"

Write-Log ""

# Test 4: ALL_DATABASES on SECONDARY (should exclude AG database)
Write-Log ">>> TEST 4: ALL_DATABASES on SECONDARY <<<" "Cyan"
$results += Test-Scenario `
    -Name "ALL_DATABASES excludes AG database on secondary" `
    -Container "db2" `
    -Query @"
EXEC dbo.sp_StatUpdate
    @Databases = N'ALL_DATABASES',
    @ModificationThreshold = 0,
    @LogToTable = N'N',
    @Execute = N'N',
    @Debug = 1;
"@ `
    -ExpectedPattern "AG Warning|excluded.*secondary" `
    -LogFile "06_secondary_all_databases"

Write-Log ""

# Test 5: Direct Sales database on SECONDARY (should be excluded)
Write-Log ">>> TEST 5: Direct AG database reference on SECONDARY <<<" "Cyan"
$results += Test-Scenario `
    -Name "Direct AG database excluded on secondary" `
    -Container "db2" `
    -Query @"
EXEC dbo.sp_StatUpdate
    @Databases = N'Sales',
    @ModificationThreshold = 0,
    @LogToTable = N'N',
    @Execute = N'N',
    @Debug = 1;
"@ `
    -ExpectedPattern "AG Warning|excluded.*secondary|Databases:\s+0 selected|No databases matched|AG secondary replicas" `
    -LogFile "07_secondary_direct_ag_db"

Write-Log ""

# Test 6: Execute on PRIMARY (actually update stats)
Write-Log ">>> TEST 6: Execute UPDATE STATISTICS on PRIMARY <<<" "Cyan"
$results += Test-Scenario `
    -Name "UPDATE STATISTICS executes on primary AG database" `
    -Container "db1" `
    -Query @"
EXEC dbo.sp_StatUpdate
    @Databases = N'Sales',
    @ModificationThreshold = 0,
    @LogToTable = N'N',
    @Execute = N'Y',
    @Debug = 1;
"@ `
    -ExpectedPattern "UPDATE STATISTICS|Statistics updated|completed successfully" `
    -LogFile "08_primary_execute"

# ============================================================
# SUMMARY
# ============================================================
Write-Log ""
Write-Log "============================================================"
Write-Log "TEST SUMMARY"
Write-Log "============================================================"

$passed = ($results | Where-Object { $_.Passed }).Count
$failed = ($results | Where-Object { -not $_.Passed }).Count
$total = $results.Count

Write-Log ""
Write-Log "Results: $passed passed, $failed failed, $total total"
Write-Log ""

foreach ($r in $results) {
    $status = if ($r.Passed) { "[PASS]" } else { "[FAIL]" }
    $color = if ($r.Passed) { "Green" } else { "Red" }
    Write-Log "  $status $($r.Name)" $color
}

Write-Log ""
Write-Log "Log files saved to: $LogDir"
Write-Log ""

# List log files
Get-ChildItem $LogDir -Filter "*.log" | ForEach-Object {
    Write-Log "  - $($_.Name) ($([math]::Round($_.Length/1KB, 1)) KB)"
}

Write-Log ""
Write-Log "============================================================"
Write-Log "AG Test Suite Completed: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Log "============================================================"

# Return results for pipeline use
return @{
    Passed = $passed
    Failed = $failed
    Total = $total
    LogDir = $LogDir
    Results = $results
}
