function Get-SpStatUpdatePort {
    param([Parameter(Mandatory)][ValidateSet('2019','2022','2025')][string]$Version)
    @{ '2019' = 1434; '2022' = 1432; '2025' = 1431 }[$Version]
}

function New-SpStatUpdatePassword {
    $bytes = [Security.Cryptography.RandomNumberGenerator]::GetBytes(24)
    ([Convert]::ToBase64String($bytes) + 'Aa1!')
}

function Start-SpStatUpdateSqlTestEnvironment {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][ValidateSet('2019','2022','2025')][string]$Version,
        [Parameter(Mandatory)][ValidatePattern('^[0-9a-f]{32}$')][string]$RunId,
        [int]$ReadyTimeoutSec = 300,
        [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { $env:MSSQL_SA_PASSWORD })
    )

    $docker = Get-Command docker -ErrorAction Stop
    if (-not $Password) { $Password = New-SpStatUpdatePassword }
    $port = Get-SpStatUpdatePort -Version $Version
    $name = "spstatupdate-$Version-$RunId"
    $image = "mcr.microsoft.com/mssql/server:$Version-latest"
    $labelKey = 'io.spstatupdate.test-run'
    $arguments = @(
        'run', '--detach', '--name', $name,
        '--label', "$labelKey=$RunId",
        '--label', 'io.spstatupdate.managed=true',
        '--memory', '3g',
        '--cpus', '2',
        '--env', 'ACCEPT_EULA=Y',
        '--env', 'MSSQL_MEMORY_LIMIT_MB=2048',
        '--env', "MSSQL_SA_PASSWORD=$Password",
        '--publish', ("127.0.0.1:{0}:1433" -f $port),
        $image
    )
    $output = & $docker.Source @arguments 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $output) {
        throw "Docker could not start the SQL Server $Version test container. Check Docker Desktop and whether local port $port is free."
    }
    $containerId = ([string]$output | Select-Object -Last 1).Trim()
    if ($containerId -notmatch '^[0-9a-f]{12,64}$') { throw 'Docker returned an invalid test container id.' }
    $environment = [pscustomobject]@{
        RunId = $RunId
        Version = $Version
        ContainerId = $containerId
        ContainerName = $name
        Server = '127.0.0.1'
        Port = $port
        Password = $Password
        Image = $image
    }

    try {
        $deadline = [DateTimeOffset]::Now.AddSeconds($ReadyTimeoutSec)
        $lastConnectionError = ''
        $ready = $false
        while ([DateTimeOffset]::Now -lt $deadline) {
            $connection = $null
            try {
                $builder = [System.Data.SqlClient.SqlConnectionStringBuilder]::new()
                $builder.DataSource = "$($environment.Server),$($environment.Port)"
                $builder.InitialCatalog = 'master'
                $builder.UserID = 'sa'
                $builder.Password = $Password
                $builder.Encrypt = $false
                $builder.TrustServerCertificate = $true
                $builder.ConnectTimeout = 5
                $connection = [System.Data.SqlClient.SqlConnection]::new($builder.ConnectionString)
                $connection.Open()
                $command = $connection.CreateCommand()
                $command.CommandText = 'SELECT 1'
                [void]$command.ExecuteScalar()
                $ready = $true
                break
            } catch {
                $lastConnectionError = $_.Exception.Message
                Start-Sleep -Seconds 5
            } finally {
                if ($connection) { $connection.Dispose() }
            }
        }
        if (-not $ready) {
            $state = @(& $docker.Source inspect --format '{{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} error={{.State.Error}}' $containerId 2>&1)
            $logs = @(& $docker.Source logs --tail 80 $containerId 2>&1)
            $details = @("Last login error: $lastConnectionError") + $state + $logs
            $diagnostic = (($details | ForEach-Object { [string]$_ }) -join [Environment]::NewLine).Replace($Password, '***')
            throw "SQL Server $Version did not accept logins within $ReadyTimeoutSec seconds.`nContainer startup diagnostics:`n$diagnostic"
        }
        Write-Host "sql_provider=docker version=$Version image=$image endpoint=127.0.0.1,$port"
        return $environment
    } catch {
        Stop-SpStatUpdateSqlTestEnvironment -Environment $environment
        throw
    }
}

function Stop-SpStatUpdateSqlTestEnvironment {
    [CmdletBinding()]
    param([Parameter(Mandatory)]$Environment)

    $docker = Get-Command docker -ErrorAction Stop
    $labelKey = 'io.spstatupdate.test-run'
    $format = "{{ index .Config.Labels `"$labelKey`" }}"
    $label = & $docker.Source inspect --format $format $Environment.ContainerId 2>$null
    if ($LASTEXITCODE -ne 0) { return }
    if (([string]$label).Trim() -ne $Environment.RunId) {
        throw 'Refusing to remove a container without this run ownership label.'
    }
    & $docker.Source rm --force $Environment.ContainerId 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not remove this run labeled SQL Server container.' }
    Write-Host "sql_provider_cleanup=complete run=$($Environment.RunId)"
}

Export-ModuleMember -Function Start-SpStatUpdateSqlTestEnvironment,Stop-SpStatUpdateSqlTestEnvironment
