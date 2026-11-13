[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateSet('2019','2022','2025')][string]$Version,
    [string]$ProviderScript = $env:SPSTATUPDATE_TEST_PROVIDER,
    [int]$ReadyTimeoutSec = 300,
    [string]$ResultsDirectory = '',
    [string]$OrchestrationId = $env:SPSTATUPDATE_ORCHESTRATION_ID
)

$ErrorActionPreference = 'Stop'
$scriptDir = $PSScriptRoot
$repoRoot = Split-Path -Parent $scriptDir
$python = (Get-Command python -ErrorAction Stop).Source
$initializeScript = Join-Path $scriptDir 'Initialize-ReleaseGateBase.ps1'
$defaultProvider = Join-Path $scriptDir 'providers/DockerSqlProvider.psm1'
if (-not $ProviderScript) { $ProviderScript = $defaultProvider }
if (-not (Test-Path -LiteralPath $ProviderScript -PathType Leaf)) { throw 'The selected SQL test provider was not found.' }
Import-Module -Name $ProviderScript -Force
$startCommand = Get-Command Start-SpStatUpdateSqlTestEnvironment -ErrorAction Stop
$stopCommand = Get-Command Stop-SpStatUpdateSqlTestEnvironment -ErrorAction Stop

$nestedCommentCheck = Join-Path $repoRoot '.github/scripts/check_sql_nested_comments.py'
$nestedCommentTests = Join-Path $repoRoot '.github/scripts/test_check_sql_nested_comments.py'
$diagContractCheck = Join-Path $scriptDir 'diag_commandlog_contract.py'
$diagContractTests = Join-Path $scriptDir 'test_diag_commandlog_contract.py'
$toolingFingerprint = (& $python (Join-Path $scriptDir 'tooling_fingerprint.py')).Trim()
if ($LASTEXITCODE -ne 0 -or $toolingFingerprint -notmatch '^[0-9a-f]{64}$') { throw 'Could not fingerprint the test tooling at run start.' }
$runId = [guid]::NewGuid().ToString('N')
$startedAt = [DateTimeOffset]::Now
if ($ResultsDirectory) { $resultDirectory = [IO.Path]::GetFullPath($ResultsDirectory) }
else { $resultDirectory = Join-Path ([IO.Path]::GetTempPath()) "sp_StatUpdate-$Version-$runId" }
[void][IO.Directory]::CreateDirectory($resultDirectory)

$suiteScripts = [ordered]@{
    Compile = Join-Path $scriptDir 'Test-ProcCompiles.ps1'
    QSCacheKey = Join-Path $scriptDir 'Test-QSCacheKey.ps1'
    EarlyEndRows = Join-Path $scriptDir 'Test-EarlyEndRows.ps1'
    AutoUpdatePersistence = Join-Path $scriptDir 'Test-AutoUpdatePersistence.ps1'
    AwkwardNamesCaseSensitive = Join-Path $scriptDir 'Test-AwkwardNamesCaseSensitive.ps1'
    QueryStoreWorkloadSortOrder = Join-Path $scriptDir 'Test-QueryStoreWorkloadSortOrder.ps1'
    HelpContent = Join-Path $scriptDir 'Test-HelpParameterContract.ps1'
    ParamSweep = Join-Path $scriptDir 'Test-ParamSweep.ps1'
    QSEnrichmentSafety = Join-Path $scriptDir 'Test-QSEnrichmentSafety.ps1'
    V3Bugs = Join-Path $scriptDir 'Test-V3Bugs.ps1'
    CriticalTables = Join-Path $scriptDir 'Test-CriticalTables.ps1'
    DenialAndGuards = Join-Path $scriptDir 'Test-DenialAndGuards.ps1'
    DirectStringCSV = Join-Path $scriptDir 'Test-DirectStringCSV.ps1'
    ExcludeTablesWildcards = Join-Path $scriptDir 'Test-ExcludeTablesWildcards.ps1'
    FilteredStats = Join-Path $scriptDir 'Test-FilteredStats.ps1'
    WhatIfNorecompute = Join-Path $scriptDir 'Test-WhatIfNorecompute.ps1'
    StopReasonsCoverage = Join-Path $scriptDir 'Test-StopReasonsCoverage.ps1'
    OutputParamCoverage = Join-Path $scriptDir 'Test-OutputParamCoverage.ps1'
    PresetStopByTime = Join-Path $scriptDir 'Test-PresetStopByTime.ps1'
    LockTimeout = Join-Path $scriptDir 'Test-LockTimeout.ps1'
    MopUpPassE2E = Join-Path $scriptDir 'Test-MopUpPassE2E.ps1'
    V3QAGaps = Join-Path $scriptDir 'Test-V3QAGaps.ps1'
    V3QAGaps2 = Join-Path $scriptDir 'Test-V3QAGaps2.ps1'
    QueryStoreStates = Join-Path $scriptDir 'Test-QueryStoreStates.ps1'
    SortOrderCoverage = Join-Path $scriptDir 'Test-SortOrderCoverage.ps1'
    QSForcedBaselineAndWaitCpu = Join-Path $scriptDir 'Test-QSForcedBaselineAndWaitCpu.ps1'
    AbortOnIntegrityError = Join-Path $scriptDir 'Test-AbortOnIntegrityError.ps1'
    FreshStatQualification = Join-Path $scriptDir 'Test-FreshStatQualification.ps1'
    WarningsContract = Join-Path $scriptDir 'Test-WarningsContract.ps1'
    ParameterBehavior = Join-Path $scriptDir 'Test-ParameterBehavior.ps1'
    V3Regression = Join-Path $scriptDir 'Test-V3Regressions.ps1'
    SizeBoundaries = Join-Path $scriptDir 'Test-SizeBoundaries.ps1'
    StatUpdateDiag = Join-Path $scriptDir 'diag/Test-StatUpdateDiag.ps1'
    DiagChecksCoverage = Join-Path $scriptDir 'diag/Test-DiagChecksCoverage.ps1'
    DiagTargetChecks = Join-Path $scriptDir 'diag/Test-DiagTargetChecks.ps1'
}
$requiredCounts = @{
    QSCacheKey = 14
    HelpContent = 2
    ParamSweep = 46
    QSEnrichmentSafety = 7
    V3Bugs = 16
    StopReasonsCoverage = 11
    OutputParamCoverage = 36
    PresetStopByTime = 7
    LockTimeout = 4
    MopUpPassE2E = 6
    V3QAGaps = 16
    V3QAGaps2 = 13
    QueryStoreStates = 7
    SortOrderCoverage = 8
    QSForcedBaselineAndWaitCpu = 6
    AbortOnIntegrityError = 5
    FreshStatQualification = 4
    WarningsContract = 5
    StatUpdateDiag = 293
    DiagChecksCoverage = 12
    DiagTargetChecks = 7
}

$previousPassword = $env:SPSTATUPDATE_TEST_SQL_PASSWORD
$environment = $null
$failure = $null
try {
    Push-Location $repoRoot
    try {
        & $python $nestedCommentTests
        if ($LASTEXITCODE -ne 0) { throw 'Nested-comment regression samples failed.' }
        & $python $nestedCommentCheck
        if ($LASTEXITCODE -ne 0) { throw 'A shipped SQL file contains a nested block comment.' }
        & $python $diagContractTests
        if ($LASTEXITCODE -ne 0) { throw 'CommandLog contract checker regression tests failed.' }
        & $python $diagContractCheck
        if ($LASTEXITCODE -ne 0) { throw 'CommandLog writer, Diag reader, and synthetic history contracts differ.' }
    } finally { Pop-Location }

    $environment = & $startCommand -Version $Version -RunId $runId -ReadyTimeoutSec $ReadyTimeoutSec
    if (-not $environment -or -not $environment.Server -or -not $environment.Port -or -not $environment.Password) {
        throw 'The SQL test provider returned incomplete connection details.'
    }
    $Server = [string]$environment.Server
    $Port = [int]$environment.Port
    $Password = [string]$environment.Password
    $env:SPSTATUPDATE_TEST_SQL_PASSWORD = $Password

    $ssmsResult = $null
    $ssmsCommand = Get-Command Invoke-SpStatUpdateSsmsCheck -ErrorAction SilentlyContinue
    if ($ssmsCommand) {
        $ssmsResult = Join-Path $resultDirectory 'ssms-report.json'
        & $ssmsCommand -RepositoryRoot $repoRoot -Version $Version -Server $Server -Port $Port -Password $Password -OutputPath $ssmsResult
        if (-not (Test-Path -LiteralPath $ssmsResult -PathType Leaf)) { throw 'The maintainer SSMS provider did not emit its result file.' }
    }

    $resultFiles = [System.Collections.Generic.List[string]]::new()
    $suiteNames = @($suiteScripts.Keys)
    $shuffledOrder = @($suiteNames | Sort-Object { Get-Random })
    if ([string]::Join('|', $shuffledOrder) -eq [string]::Join('|', $suiteNames)) {
        $shuffledOrder = @($suiteNames[-1]) + @($suiteNames[0..($suiteNames.Count - 2)])
    }
    foreach ($suiteName in $shuffledOrder) {
        $initPath = Join-Path $resultDirectory "setup-$suiteName.json"
        & pwsh -NoProfile -File $initializeScript -Server $Server -Version $Version -PortOverride $Port -TimingPath $initPath
        if ($LASTEXITCODE -ne 0) { throw "Base rebuild failed before $suiteName." }

        $outputPath = Join-Path $resultDirectory "1-$suiteName.json"
        $script = $suiteScripts[$suiteName]
        if (-not (Test-Path -LiteralPath $script -PathType Leaf)) { throw "Declared suite file is missing: $suiteName." }
        $args = @('-Server', $Server)
        if ($suiteName -eq 'StatUpdateDiag') {
            $args = @('-Server', "$Server,$Port", '-SqlVersion', $Version, '-Commit', (& git -C $repoRoot rev-parse HEAD).Trim())
        } elseif ($suiteName -in @('DiagChecksCoverage', 'DiagTargetChecks')) {
            $args = @('-Server', $Server, '-Port', $Port, '-Password', $Password, '-SqlVersion', $Version, '-Commit', (& git -C $repoRoot rev-parse HEAD).Trim())
        } elseif ($suiteName -eq 'QSForcedBaselineAndWaitCpu') {
            $args += @('-Port', $Port)
        } elseif ($suiteName -eq 'Compile') {
            $args += @('-PortOverride', $Port, '-Password', $Password)
        } elseif ($suiteName -in @('DenialAndGuards', 'ExcludeTablesWildcards', 'FilteredStats', 'WhatIfNorecompute',
                'StopReasonsCoverage', 'OutputParamCoverage', 'PresetStopByTime', 'LockTimeout',
                'QueryStoreStates', 'SortOrderCoverage', 'AbortOnIntegrityError', 'FreshStatQualification', 'WarningsContract')) {
            $args += @('-Version', $Version)
        } elseif ($suiteName -in @('MopUpPassE2E', 'V3QAGaps', 'V3QAGaps2')) {
            $args += @('-Versions', $Version)
        } else {
            $args += @('-Versions', $Version)
        }
        if ($suiteName -in @('QSCacheKey', 'ParamSweep', 'V3Bugs', 'CriticalTables', 'DenialAndGuards',
                'DirectStringCSV', 'ExcludeTablesWildcards', 'FilteredStats', 'WhatIfNorecompute',
                'MopUpPassE2E', 'V3QAGaps', 'V3QAGaps2', 'QueryStoreStates',
                'AbortOnIntegrityError', 'WarningsContract')) {
            $args += '-SkipSetup'
        }
        $args += @('-ResultsPath', $outputPath)
        & pwsh -NoProfile -File $script @args
        if ($LASTEXITCODE -ne 0) { Write-Warning "$suiteName process returned exit code $LASTEXITCODE; result rows decide the verdict." }
        if (-not (Test-Path -LiteralPath $outputPath -PathType Leaf)) { throw "$suiteName did not emit its declared results list." }
        if ($requiredCounts.ContainsKey($suiteName)) {
            $record = Get-Content -LiteralPath $outputPath -Raw | ConvertFrom-Json
            if ($record.suites.Count -ne 1 -or [int]$record.suites[0].expected_count -ne $requiredCounts[$suiteName]) {
                throw "$suiteName emitted an unexpected structured check count."
            }
        }
        $resultFiles.Add($outputPath)
    }

    $commit = (& git -C $repoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not identify the tested source commit.' }
    $endedAt = [DateTimeOffset]::Now
    $manifest = Join-Path $resultDirectory 'suite-paths.json'
    ConvertTo-Json -InputObject $resultFiles.ToArray() -Compress | Set-Content -LiteralPath $manifest -Encoding utf8
    $runOutput = Join-Path $resultDirectory 'run-results.json'
    $persistArgs = @(
        (Join-Path $scriptDir 'persist_release_run.py'), '--version', $Version,
        '--commit', $commit, '--run-id', $runId, '--tooling-sha256', $toolingFingerprint,
        '--pass-count', '1', '--started-at', $startedAt.ToString('o'), '--ended-at', $endedAt.ToString('o'),
        '--result-manifest', $manifest, '--output', $runOutput
    )
    if ($OrchestrationId) { $persistArgs += @('--orchestration-id', $OrchestrationId) }
    if ($ssmsResult) { $persistArgs += @('--ssms-result', $ssmsResult) }
    & $python @persistArgs
    if ($LASTEXITCODE -ne 0) { throw 'Local result validation or verdict failed.' }
    Write-Host "run_id=$runId test_fingerprint=$toolingFingerprint commit=$commit results=$runOutput"
} catch {
    $failure = $_
    $message = [string]$_.Exception.Message
    if ($Password) { $message = $message.Replace($Password, '***') }
    Write-Error -Message "Release gate stopped: $message" -ErrorAction Continue
    throw
} finally {
    if ($environment) {
        try { & $stopCommand -Environment $environment }
        catch { Write-Error -Message 'Test provider cleanup failed.' -ErrorAction Continue; if (-not $failure) { $failure = $_ } }
    }
    if ($null -eq $previousPassword) { Remove-Item Env:\SPSTATUPDATE_TEST_SQL_PASSWORD -ErrorAction SilentlyContinue }
    else { $env:SPSTATUPDATE_TEST_SQL_PASSWORD = $previousPassword }
}
