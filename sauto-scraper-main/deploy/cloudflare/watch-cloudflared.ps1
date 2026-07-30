param(
  [Parameter(Mandatory = $false)]
  [string]$ServiceName = "cloudflared",

  [Parameter(Mandatory = $false)]
  [string]$TunnelName = "sauto-api-autoidx",

  [Parameter(Mandatory = $false)]
  [string]$CloudflaredPath = "c:\scraper\tools\cloudflared\cloudflared.exe",

  [Parameter(Mandatory = $false)]
  [string]$ConfigPath = "$env:USERPROFILE\.cloudflared\config.yml",

  [Parameter(Mandatory = $false)]
  [string[]]$HealthUrls = @(
    "https://autoidx.cz",
    "https://www.autoidx.cz",
    "https://api.autoidx.cz/api/health"
  ),

  [Parameter(Mandatory = $false)]
  [int]$IntervalSec = 30,

  [Parameter(Mandatory = $false)]
  [int]$MaxFailures = 2,

  [Parameter(Mandatory = $false)]
  [switch]$RunOnce,

  [Parameter(Mandatory = $false)]
  [string]$LogPath = "c:\scraper\Sauto_Scrapper\sauto-scraper-main\logs\cloudflared-watchdog.log"
)

$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
  $PSNativeCommandUseErrorActionPreference = $false
}

function Write-Log {
  param([string]$Level, [string]$Message)
  $line = "{0} [{1}] {2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Level.ToUpperInvariant(), $Message
  Write-Host $line
  $dir = Split-Path -Parent $LogPath
  if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }
  Add-Content -Path $LogPath -Value $line
}

function Resolve-Cloudflared {
  $cmd = Get-Command cloudflared -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }
  if (Test-Path $CloudflaredPath) { return $CloudflaredPath }
  throw "cloudflared not found in PATH or at $CloudflaredPath"
}

function Ensure-ServiceRunning {
  param([string]$Name)
  $svc = Get-Service -Name $Name -ErrorAction SilentlyContinue
  if (-not $svc) { return $false }
  if ($svc.Status -ne "Running") {
    Write-Log "warn" "Service '$Name' is '$($svc.Status)'. Attempting start."
    try {
      Start-Service -Name $Name -ErrorAction Stop
      Start-Sleep -Seconds 2
      $svc = Get-Service -Name $Name -ErrorAction SilentlyContinue
      if ($svc -and $svc.Status -eq "Running") {
        Write-Log "info" "Service '$Name' started successfully."
        return $true
      }
      Write-Log "error" "Service '$Name' failed to start."
      return $false
    } catch {
      Write-Log "error" "Start-Service failed for '$Name': $($_.Exception.Message)"
      return $false
    }
  }
  return $true
}

function Ensure-ProcessRunning {
  param([string]$ExePath, [string]$CfgPath, [string]$Name)
  $procs = Get-CimInstance Win32_Process -Filter "name='cloudflared.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match [regex]::Escape("tunnel run $Name") }
  if ($procs) { return $true }
  if (-not (Test-Path $CfgPath)) {
    Write-Log "error" "Config file not found: $CfgPath"
    return $false
  }
  Write-Log "warn" "No named tunnel process found. Starting detached cloudflared process."
  try {
    Start-Process -FilePath $ExePath -ArgumentList @("--config", $CfgPath, "tunnel", "run", $Name) | Out-Null
    Start-Sleep -Seconds 2
    $check = Get-CimInstance Win32_Process -Filter "name='cloudflared.exe'" -ErrorAction SilentlyContinue |
      Where-Object { $_.CommandLine -match [regex]::Escape("tunnel run $Name") }
    if ($check) {
      Write-Log "info" "cloudflared process started for tunnel '$Name'."
      return $true
    }
    Write-Log "error" "cloudflared process did not start for tunnel '$Name'."
    return $false
  } catch {
    Write-Log "error" "Failed to start cloudflared process: $($_.Exception.Message)"
    return $false
  }
}

function Test-Endpoints {
  param([string[]]$Urls)
  foreach ($url in $Urls) {
    try {
      $resp = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 15
      if ($resp.StatusCode -lt 200 -or $resp.StatusCode -ge 500) {
        Write-Log "warn" "Health check status $($resp.StatusCode) for $url"
        return $false
      }
    } catch {
      Write-Log "warn" "Health check failed for $url : $($_.Exception.Message)"
      return $false
    }
  }
  return $true
}

function Restart-Cloudflared {
  param([string]$Name, [string]$ExePath, [string]$CfgPath, [string]$Tunnel)
  Write-Log "warn" "Attempting cloudflared restart sequence."
  $svc = Get-Service -Name $Name -ErrorAction SilentlyContinue
  if ($svc) {
    try {
      Restart-Service -Name $Name -Force -ErrorAction Stop
      Start-Sleep -Seconds 3
      Write-Log "info" "Restart-Service for '$Name' succeeded."
      return
    } catch {
      Write-Log "error" "Restart-Service failed: $($_.Exception.Message)"
    }
  }
  try {
    Get-Process cloudflared -ErrorAction SilentlyContinue | ForEach-Object {
      Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 1
  } catch {
    Write-Log "warn" "Process cleanup warning: $($_.Exception.Message)"
  }
  $null = Ensure-ProcessRunning -ExePath $ExePath -CfgPath $CfgPath -Name $Tunnel
}

$resolvedCloudflared = Resolve-Cloudflared
Write-Log "info" "Watchdog started. Interval=${IntervalSec}s MaxFailures=$MaxFailures RunOnce=$($RunOnce.IsPresent)"
$failureCount = 0
while ($true) {
  $serviceOk = Ensure-ServiceRunning -Name $ServiceName
  if (-not $serviceOk) {
    $null = Ensure-ProcessRunning -ExePath $resolvedCloudflared -CfgPath $ConfigPath -Name $TunnelName
  }
  $processOk = Ensure-ProcessRunning -ExePath $resolvedCloudflared -CfgPath $ConfigPath -Name $TunnelName
  $healthOk = Test-Endpoints -Urls $HealthUrls
  if ($processOk -and $healthOk) {
    if ($failureCount -gt 0) { Write-Log "info" "Tunnel recovered and checks are passing again." }
    $failureCount = 0
  } else {
    $failureCount++
    Write-Log "warn" "Watchdog failure count: $failureCount/$MaxFailures"
    if ($failureCount -ge $MaxFailures) {
      Restart-Cloudflared -Name $ServiceName -ExePath $resolvedCloudflared -CfgPath $ConfigPath -Tunnel $TunnelName
      $failureCount = 0
    }
  }
  if ($RunOnce) {
    Write-Log "info" "RunOnce enabled. Exiting watchdog."
    break
  }
  Start-Sleep -Seconds $IntervalSec
}