param(
  [Parameter(Mandatory = $false)]
  [switch]$WithWatchdog
)

$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
  $PSNativeCommandUseErrorActionPreference = $false
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$webApiDir = Join-Path $repoRoot "web-api"
$webUiDistDir = Join-Path $repoRoot "web-ui\dist"
$venvPython = "c:\scraper\.venv\Scripts\python.exe"
$cloudflared = "c:\scraper\tools\cloudflared\cloudflared.exe"
$cloudflaredConfig = Join-Path $env:USERPROFILE ".cloudflared\config.yml"
$tunnelName = "sauto-api-autoidx"
$watchdogScript = Join-Path $repoRoot "deploy\cloudflare\watch-cloudflared.ps1"
$watchdogLog = Join-Path $repoRoot "logs\cloudflared-watchdog-api.log"

function Write-Info([string]$Message) {
  Write-Host "[INFO] $Message"
}

function Stop-MatchingProcess {
  param([string]$Name, [string]$Pattern)

  Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
    Where-Object {
      $_.Name -eq $Name -and $_.CommandLine -match $Pattern
    } |
    ForEach-Object {
      Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }
}

function Keep-OnlyFirstProcess {
  param([string]$Name, [string]$Pattern)

  $matches = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
    Where-Object {
      $_.Name -eq $Name -and $_.CommandLine -match $Pattern
    } |
    Sort-Object ProcessId

  if (-not $matches -or $matches.Count -le 1) {
    return
  }

  $keep = $matches[0].ProcessId
  $matches |
    Where-Object { $_.ProcessId -ne $keep } |
    ForEach-Object {
      Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }
}

function Wait-HttpOk {
  param(
    [string]$Url,
    [int]$Attempts = 20,
    [int]$SleepSec = 2
  )

  for ($i = 1; $i -le $Attempts; $i++) {
    try {
      $resp = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 8
      if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500) {
        return $true
      }
    } catch {
      Start-Sleep -Seconds $SleepSec
      continue
    }
    Start-Sleep -Seconds $SleepSec
  }

  return $false
}

function Wait-HttpStatus {
  param(
    [string]$Url,
    [int[]]$ExpectedStatuses,
    [int]$Attempts = 30,
    [int]$SleepSec = 2
  )

  for ($i = 1; $i -le $Attempts; $i++) {
    try {
      $resp = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 10
      if ($resp.StatusCode -in $ExpectedStatuses) {
        return $resp
      }
      Write-Info ("Waiting for {0}: got HTTP {1} (attempt {2}/{3})" -f $Url, $resp.StatusCode, $i, $Attempts)
    } catch {
      $code = $null
      try { $code = [int]$_.Exception.Response.StatusCode.value__ } catch {}
      if ($code) {
        Write-Info ("Waiting for {0}: got HTTP {1} (attempt {2}/{3})" -f $Url, $code, $i, $Attempts)
      } else {
        Write-Info ("Waiting for {0}: transient error '{1}' (attempt {2}/{3})" -f $Url, $_.Exception.Message, $i, $Attempts)
      }
    }
    Start-Sleep -Seconds $SleepSec
  }

  throw "Endpoint did not reach expected status at $Url"
}

function Invoke-EndpointCheck {
  param([hashtable]$Check)

  $methodValue = if ($Check.ContainsKey("Method") -and $null -ne $Check.Method) { $Check.Method } else { "GET" }
  $urlValue = if ($Check.ContainsKey("Url") -and $null -ne $Check.Url) { $Check.Url } else { "" }
  $method = ("{0}" -f $methodValue).ToUpperInvariant()
  $url = "{0}" -f $urlValue
  if (-not $url) {
    throw "Endpoint check is missing URL."
  }

  $requestArgs = @{
    Uri = $url
    Method = $method
    UseBasicParsing = $true
    TimeoutSec = 12
  }

  if ($Check.ContainsKey("BodyJson") -and $null -ne $Check.BodyJson) {
    $requestArgs["ContentType"] = "application/json"
    $requestArgs["Body"] = [string]$Check.BodyJson
  }

  $status = $null
  try {
    $resp = Invoke-WebRequest @requestArgs
    $status = [int]$resp.StatusCode
  } catch {
    try { $status = [int]$_.Exception.Response.StatusCode.value__ } catch {}
    if (-not $status) {
      throw "Endpoint check failed for ${method} ${url}: $($_.Exception.Message)"
    }
  }

  $expected = @($Check.ExpectedStatuses | ForEach-Object { [int]$_ })
  if ($status -eq 501 -and $url -match "/api/") {
    throw "Routing broken for ${method} ${url}: got 501 (API likely routed to static server)."
  }
  if ($expected.Count -gt 0 -and $status -notin $expected) {
    throw "Unexpected status for ${method} ${url}: got $status, expected [$($expected -join ', ')]."
  }

  $labelValue = if ($Check.ContainsKey("Name") -and $null -ne $Check.Name) { $Check.Name } else { "check" }
  $label = "{0}" -f $labelValue
  Write-Info ("Smoke OK: {0} {1} -> {2} ({3})" -f $method, $url, $status, $label)
}

function Assert-EndpointMatrix {
  param([hashtable[]]$Checks)

  foreach ($check in $Checks) {
    Invoke-EndpointCheck -Check $check
  }
}

function Ensure-Prereqs {
  if (-not (Test-Path $venvPython)) {
    throw "Python not found: $venvPython"
  }
  if (-not (Test-Path $cloudflared)) {
    throw "cloudflared not found: $cloudflared"
  }
  if (-not (Test-Path $webUiDistDir)) {
    throw "Frontend build folder missing: $webUiDistDir"
  }

  $credPath = Join-Path $env:USERPROFILE ".cloudflared\a376e6bf-f9ac-439d-86fa-f3108e5494ad.json"
  if (-not (Test-Path $credPath)) {
    throw "Tunnel credentials not found: $credPath"
  }

  $cfg = @"
tunnel: $tunnelName
credentials-file: $credPath

ingress:
  - hostname: api.autoidx.cz
    service: http://127.0.0.1:8000

  - hostname: autoidx.cz
    path: /api/*
    service: http://127.0.0.1:8000
  - hostname: autoidx.cz
    service: http://127.0.0.1:5173

  - hostname: www.autoidx.cz
    path: /api/*
    service: http://127.0.0.1:8000
  - hostname: www.autoidx.cz
    service: http://127.0.0.1:5173

  - hostname: app.autoidx.cz
    path: /api/*
    service: http://127.0.0.1:8000
  - hostname: app.autoidx.cz
    service: http://127.0.0.1:5173

  - service: http_status:404
"@

  Set-Content -Path $cloudflaredConfig -Value $cfg -Encoding UTF8
}
function Ensure-DnsRoutes {
  & $cloudflared tunnel route dns -f $tunnelName "api.autoidx.cz" | Out-Null
  & $cloudflared tunnel route dns -f $tunnelName "autoidx.cz" | Out-Null
  & $cloudflared tunnel route dns -f $tunnelName "www.autoidx.cz" | Out-Null
  & $cloudflared tunnel route dns -f $tunnelName "app.autoidx.cz" | Out-Null
}

Write-Info "Stopping previous processes..."
Stop-MatchingProcess -Name "cloudflared.exe" -Pattern "tunnel run $tunnelName"
Stop-MatchingProcess -Name "python.exe" -Pattern "-m uvicorn app:app --host 127.0.0.1 --port 8000"
Stop-MatchingProcess -Name "python.exe" -Pattern "-m http\.server 5173 --bind 127.0.0.1"
Stop-MatchingProcess -Name "powershell.exe" -Pattern "watch-cloudflared\.ps1"

Write-Info "Checking prerequisites and writing tunnel config..."
Ensure-Prereqs
Ensure-DnsRoutes

Write-Info "Starting backend on 127.0.0.1:8000..."
Start-Process -FilePath $venvPython -ArgumentList @("-m", "uvicorn", "app:app", "--host", "127.0.0.1", "--port", "8000") -WorkingDirectory $webApiDir -WindowStyle Hidden | Out-Null

if (-not (Wait-HttpOk -Url "http://127.0.0.1:8000/api/health" -Attempts 20 -SleepSec 2)) {
  throw "Backend failed health check on http://127.0.0.1:8000/api/health"
}

Write-Info "Starting frontend static server on 127.0.0.1:5173..."
Start-Process -FilePath $venvPython -ArgumentList @("-m", "http.server", "5173", "--bind", "127.0.0.1") -WorkingDirectory $webUiDistDir -WindowStyle Hidden | Out-Null

if (-not (Wait-HttpOk -Url "http://127.0.0.1:5173" -Attempts 20 -SleepSec 2)) {
  throw "Frontend failed health check on http://127.0.0.1:5173"
}

Write-Info "Starting cloudflared tunnel..."
Start-Process -FilePath $cloudflared -ArgumentList @("--config", $cloudflaredConfig, "tunnel", "run", $tunnelName) -RedirectStandardOutput "c:\scraper\cloudflared-live.out.log" -RedirectStandardError "c:\scraper\cloudflared-live.err.log" -WindowStyle Hidden | Out-Null
Start-Sleep -Seconds 3

# If an external service/task started duplicates, dedupe only cloudflared/watchdog.
# Do not dedupe python.exe here: uvicorn/http.server can have parent+worker processes.
Keep-OnlyFirstProcess -Name "cloudflared.exe" -Pattern "tunnel run $tunnelName"

Write-Info "Verifying public endpoints..."
$apiResp = Wait-HttpStatus -Url "https://api.autoidx.cz/api/health" -ExpectedStatuses @(200) -Attempts 40 -SleepSec 2
$rootResp = Wait-HttpStatus -Url "https://autoidx.cz" -ExpectedStatuses @(200) -Attempts 30 -SleepSec 2
$wwwResp = Wait-HttpStatus -Url "https://www.autoidx.cz" -ExpectedStatuses @(200) -Attempts 30 -SleepSec 2

Write-Info ("API: {0}" -f $apiResp.StatusCode)
Write-Info ("AUTOIDX: {0}" -f $rootResp.StatusCode)
Write-Info ("WWW: {0}" -f $wwwResp.StatusCode)

Write-Info "Running full endpoint smoke matrix..."
$loginProbe = @{ email = "routing-check@autoidx.invalid"; password = "wrong-pass" } | ConvertTo-Json
$smokeChecks = @(
  @{ Name = "api host health"; Method = "GET"; Url = "https://api.autoidx.cz/api/health"; ExpectedStatuses = @(200) }
  @{ Name = "root host"; Method = "GET"; Url = "https://autoidx.cz"; ExpectedStatuses = @(200) }
  @{ Name = "www host"; Method = "GET"; Url = "https://www.autoidx.cz"; ExpectedStatuses = @(200) }
  @{ Name = "app host"; Method = "GET"; Url = "https://app.autoidx.cz"; ExpectedStatuses = @(200) }

  @{ Name = "root api health"; Method = "GET"; Url = "https://autoidx.cz/api/health"; ExpectedStatuses = @(200) }
  @{ Name = "www api health"; Method = "GET"; Url = "https://www.autoidx.cz/api/health"; ExpectedStatuses = @(200) }
  @{ Name = "app api health"; Method = "GET"; Url = "https://app.autoidx.cz/api/health"; ExpectedStatuses = @(200) }

  @{ Name = "api files on api host"; Method = "GET"; Url = "https://api.autoidx.cz/api/results/files"; ExpectedStatuses = @(200) }
  @{ Name = "api files on root host"; Method = "GET"; Url = "https://autoidx.cz/api/results/files"; ExpectedStatuses = @(200) }
  @{ Name = "api files on www host"; Method = "GET"; Url = "https://www.autoidx.cz/api/results/files"; ExpectedStatuses = @(200) }
  @{ Name = "api files on app host"; Method = "GET"; Url = "https://app.autoidx.cz/api/results/files"; ExpectedStatuses = @(200) }

  @{ Name = "auth guard root"; Method = "GET"; Url = "https://autoidx.cz/api/proxy/config"; ExpectedStatuses = @(401) }
  @{ Name = "auth guard www"; Method = "GET"; Url = "https://www.autoidx.cz/api/proxy/config"; ExpectedStatuses = @(401) }
  @{ Name = "auth guard app"; Method = "GET"; Url = "https://app.autoidx.cz/api/proxy/config"; ExpectedStatuses = @(401) }

  @{ Name = "login route root"; Method = "POST"; Url = "https://autoidx.cz/api/auth/login"; ExpectedStatuses = @(200, 400, 401, 409, 422); BodyJson = $loginProbe }
  @{ Name = "login route www"; Method = "POST"; Url = "https://www.autoidx.cz/api/auth/login"; ExpectedStatuses = @(200, 400, 401, 409, 422); BodyJson = $loginProbe }
  @{ Name = "login route app"; Method = "POST"; Url = "https://app.autoidx.cz/api/auth/login"; ExpectedStatuses = @(200, 400, 401, 409, 422); BodyJson = $loginProbe }
)
Assert-EndpointMatrix -Checks $smokeChecks

if ($WithWatchdog) {
  Write-Info "Starting watchdog..."
  Start-Process -FilePath "powershell.exe" -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $watchdogScript, "-TunnelName", $tunnelName, "-HealthUrls", "https://api.autoidx.cz/api/health", "-LogPath", $watchdogLog) -WindowStyle Hidden | Out-Null
  Keep-OnlyFirstProcess -Name "powershell.exe" -Pattern "watch-cloudflared\.ps1"
}

Write-Info "Startup complete."
