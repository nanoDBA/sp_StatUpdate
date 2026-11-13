[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Server,
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [Parameter(Mandatory)][ValidateSet('2019','2022','2025')][string]$Version,
    [string]$TimingPath = '',
    [int]$PortOverride = 0
)

$ErrorActionPreference = 'Stop'
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }
$port = if ($PortOverride -gt 0) { $PortOverride } else { @{ '2019' = 1434; '2022' = 1432; '2025' = 1431 }[$Version] }
$repoRoot = Split-Path -Parent $PSScriptRoot
$connectionString = "Server=$Server,$port;Database=master;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Connection Timeout=10;Pooling=false"
$connection = [System.Data.SqlClient.SqlConnection]::new($connectionString)

function Invoke-SqlFile {
    param([System.Data.SqlClient.SqlConnection]$Connection, [string]$Path)
    $sql = [System.IO.File]::ReadAllText($Path)
    $batches = [regex]::Split($sql, '(?mi)^\s*GO\s*$') | Where-Object { $_.Trim().Length -gt 0 }
    $batchNumber = 0
    foreach ($batch in $batches) {
        $batchNumber++
        $command = $Connection.CreateCommand()
        $command.CommandText = $batch
        $command.CommandTimeout = 300
        try {
            $reader = $command.ExecuteReader()
            while (-not $reader.IsClosed) {
                while ($reader.Read()) { }
                if (-not $reader.NextResult()) { break }
            }
        } catch {
            $cause = $_.Exception
            while ($cause.InnerException) { $cause = $cause.InnerException }
            $detail = $cause.Message.Replace($Password, '[redacted]')
            throw "SQL setup failed in $(Split-Path -Leaf $Path), batch $batchNumber ($($cause.GetType().Name)): $detail"
        } finally {
            if ($reader) { $reader.Dispose() }
            $command.Dispose()
        }
    }
}

 $timing = [ordered]@{ base_rebuild_ms = 0; install_proc_ms = 0; install_diag_ms = 0 }
 $timer = [System.Diagnostics.Stopwatch]::new()
try {
    $connection.Open()
    $timer.Restart()
    Invoke-SqlFile -Connection $connection -Path (Join-Path $PSScriptRoot 'setup_multiversion_db.sql')
    $timer.Stop()
    $timing.base_rebuild_ms = $timer.ElapsedMilliseconds
    # setup_multiversion_db.sql ends in StatUpdateTest, but the shipped procedures
    # belong in master. Keep deployment context explicit on the shared connection.
    $connection.ChangeDatabase('master')
    $timer.Restart()
    Invoke-SqlFile -Connection $connection -Path (Join-Path $repoRoot 'sp_StatUpdate.sql')
    $timer.Stop()
    $timing.install_proc_ms = $timer.ElapsedMilliseconds
    $timer.Restart()
    Invoke-SqlFile -Connection $connection -Path (Join-Path $repoRoot 'sp_StatUpdate_Diag.sql')
    $timer.Stop()
    $timing.install_diag_ms = $timer.ElapsedMilliseconds
    $timing.install_procs_total_ms = [long]$timing.install_proc_ms + [long]$timing.install_diag_ms
    Write-Output 'base_database=rebuilt scripts=installed'
} finally {
    $connection.Dispose()
    if ($TimingPath) {
        $timing | ConvertTo-Json -Depth 3 | Set-Content -LiteralPath $TimingPath -Encoding utf8
    }
}
