param(
  [Parameter(Mandatory = $false)]
  [string]$Zone = "autoidx.cz",

  [Parameter(Mandatory = $false)]
  [string]$ApiHostname = "api.autoidx.cz",

  [Parameter(Mandatory = $false)]
  [string]$TunnelName = "sauto-api-autoidx",

  [Parameter(Mandatory = $false)]
  [string]$CloudflaredPath = "c:\scraper\tools\cloudflared\cloudflared.exe"
)

$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
  $PSNativeCommandUseErrorActionPreference = $false
}

function Resolve-Cloudflared {
  $cmd = Get-Command cloudflared -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }
  if (Test-Path $CloudflaredPath) { return $CloudflaredPath }
  throw "cloudflared not found. Expected in PATH or at $CloudflaredPath"
}

function Ensure-ApiEnv {
  param([string]$RepoRoot, [string]$ZoneName)

  $apiDir = Join-Path $RepoRoot "web-api"
  $envFile = Join-Path $apiDir ".env"
  $envExample = Join-Path $apiDir ".env.example"

  if (-not (Test-Path $envFile)) {
    if (Test-Path $envExample) {
      Copy-Item $envExample $envFile
    } else {
      Set-Content -Path $envFile -Value "" -Encoding UTF8
    }
  }

  $lines = @()
  if (Test-Path $envFile) {
    $lines = Get-Content $envFile
  }

  $origins = @(
    "https://$ZoneName",
    "https://app.$ZoneName",
    "http://localhost:5173",
    "http://127.0.0.1:5173"
  ) -join ","

  $updated = $false
  for ($i = 0; $i -lt $lines.Count; $i++) {
    if ($lines[$i] -match '^\s*CORS_ALLOW_ORIGINS\s*=') {
      $lines[$i] = "CORS_ALLOW_ORIGINS=$origins"
      $updated = $true
      break
    }
  }

  if (-not $updated) {
    $lines += "CORS_ALLOW_ORIGINS=$origins"
  }

  Set-Content -Path $envFile -Value ($lines -join "`r`n") -Encoding UTF8
}

function Ensure-Tunnel {
  param(
    [string]$Cloudflared,
    [string]$Name,
    [string]$Hostname
  )

  Write-Host "[1/7] Verifying Cloudflare auth..."
  $listRaw = & $Cloudflared --loglevel error tunnel list --output json 2>$null
  if ($LASTEXITCODE -ne 0) {
    Write-Host "Not authenticated for required zone, opening login..."
    & $Cloudflared --loglevel error tunnel login
    $listRaw = & $Cloudflared --loglevel error tunnel list --output json 2>$null
    if ($LASTEXITCODE -ne 0) {
      throw "Cloudflare login failed or wrong account selected."
    }
  }

  $listText = ($listRaw | Out-String)
  $tunnels = @()
  try {
    $tunnels = $listText | ConvertFrom-Json
  } catch {
    throw "Unable to parse tunnel list output."
  }

  $t = $tunnels | Where-Object { $_.name -eq $Name } | Select-Object -First 1

  if (-not $t) {
    Write-Host "[2/7] Creating named tunnel: $Name"
    & $Cloudflared --loglevel error tunnel create $Name
    if ($LASTEXITCODE -ne 0) {
      throw "Tunnel creation failed for '$Name'."
    }

    $listText = ((& $Cloudflared --loglevel error tunnel list --output json 2>$null) | Out-String)
    $tunnels = $listText | ConvertFrom-Json
    $t = $tunnels | Where-Object { $_.name -eq $Name } | Select-Object -First 1
    if (-not $t) {
      throw "Tunnel '$Name' not found after creation."
    }
  } else {
    Write-Host "[2/7] Tunnel exists: $Name"
  }

  $cfDir = Join-Path $env:USERPROFILE ".cloudflared"
  if (-not (Test-Path $cfDir)) {
    New-Item -ItemType Directory -Path $cfDir | Out-Null
  }

  $credPath = Join-Path $cfDir ("{0}.json" -f $t.id)
  if (-not (Test-Path $credPath)) {
    throw "Credentials file missing for tunnel '$Name': $credPath. Re-run login and tunnel create."
  }

  Write-Host "[3/7] Creating or validating DNS route: $Hostname"
  $routeOutput = (& $Cloudflared --loglevel error tunnel route dns $Name $Hostname 2>&1 | Out-String)
  if ($LASTEXITCODE -ne 0 -and $routeOutput -notmatch "already|exist|CNAME") {
    throw "Failed to create DNS route for $Hostname. Output: $routeOutput"
  }

  $configPath = Join-Path $cfDir "config.yml"
  $cfg = @"
tunnel: $Name
credentials-file: $credPath

ingress:
  - hostname: $Hostname
    service: http://127.0.0.1:8000
  - service: http_status:404
"@
  Set-Content -Path $configPath -Value $cfg -Encoding UTF8

  return @{
    TunnelId = $t.id
    ConfigPath = $configPath
  }
}

function Ensure-Backend {
  param([string]$RepoRoot)
  Write-Host "[4/7] Starting backend..."
  & (Join-Path $RepoRoot "deploy\backend\run-backend-windows.ps1")
  if ($LASTEXITCODE -ne 0) {
    throw "Backend start script failed."
  }
}

function Ensure-TunnelRunning {
  param([string]$Cloudflared, [string]$TunnelName, [string]$ConfigPath, [string]$RepoRoot)

  Write-Host "[5/7] Ensuring tunnel process is running..."

  $existing = Get-CimInstance Win32_Process -Filter "name='cloudflared.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match "tunnel run $TunnelName" }

  if ($existing) {
    Write-Host "Tunnel already running (PID=$($existing[0].ProcessId))."
    return
  }

  $logDir = Join-Path $RepoRoot "logs"
  if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
  }

  $outLog = Join-Path $logDir "cloudflared.out.log"
  $errLog = Join-Path $logDir "cloudflared.err.log"

  $proc = Start-Process -FilePath $Cloudflared `
    -ArgumentList @("--config", $ConfigPath, "tunnel", "run", $TunnelName) `
    -RedirectStandardOutput $outLog `
    -RedirectStandardError $errLog `
    -PassThru

  Write-Host "Tunnel started (PID=$($proc.Id))."
}

function Verify-Live {
  param([string]$Hostname)

  Write-Host "[6/7] Waiting for DNS propagation..."
  Start-Sleep -Seconds 4

  $healthUrl = "https://$Hostname/api/health"
  for ($i = 1; $i -le 15; $i++) {
    try {
      $resp = Invoke-WebRequest -Uri $healthUrl -UseBasicParsing -TimeoutSec 12
      Write-Host "[7/7] LIVE OK: $healthUrl"
      Write-Host $resp.Content
      return
    }
    catch {
      Start-Sleep -Seconds 4
    }
  }

  Write-Host "DNS or tunnel may still be propagating. Check later: $healthUrl"
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$cloudflared = Resolve-Cloudflared

Write-Host "Using cloudflared: $cloudflared"
Ensure-ApiEnv -RepoRoot $repoRoot -ZoneName $Zone
Ensure-Backend -RepoRoot $repoRoot

$tunnelInfo = Ensure-Tunnel -Cloudflared $cloudflared -Name $TunnelName -Hostname $ApiHostname
Ensure-TunnelRunning -Cloudflared $cloudflared -TunnelName $TunnelName -ConfigPath $tunnelInfo.ConfigPath -RepoRoot $repoRoot
Verify-Live -Hostname $ApiHostname
