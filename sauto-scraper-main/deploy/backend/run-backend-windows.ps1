param(
  [Parameter(Mandatory = $false)]
  [int]$Port = 8000,

  [Parameter(Mandatory = $false)]
  [string]$BindHost = "127.0.0.1"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$ApiDir = Join-Path $Root "web-api"
$PyExe = "c:/scraper/.venv/Scripts/python.exe"
$LogDir = Join-Path $Root "logs"
$OutLog = Join-Path $LogDir "backend.out.log"
$ErrLog = Join-Path $LogDir "backend.err.log"

if (-not (Test-Path $PyExe)) {
  throw "Python executable not found: $PyExe"
}

if (-not (Test-Path $LogDir)) {
  New-Item -ItemType Directory -Path $LogDir | Out-Null
}

$listener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
  Select-Object -First 1
if ($listener) {
  Write-Host "Port $Port is already in use by PID $($listener.OwningProcess)."
  try {
    $resp = Invoke-WebRequest -Uri "http://$BindHost`:$Port/api/health" -UseBasicParsing -TimeoutSec 5
    Write-Host "Backend already healthy: $($resp.Content)"
    exit 0
  }
  catch {
    Write-Host "Port is occupied but health failed, stopping existing listener..."
    Stop-Process -Id $listener.OwningProcess -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1
  }
}

Set-Location $ApiDir
$proc = Start-Process -FilePath $PyExe \
  -ArgumentList @("-m", "uvicorn", "app:app", "--host", $BindHost, "--port", "$Port") \
  -WorkingDirectory $ApiDir \
  -RedirectStandardOutput $OutLog \
  -RedirectStandardError $ErrLog \
  -PassThru

Start-Sleep -Seconds 2

try {
  $resp = Invoke-WebRequest -Uri "http://$BindHost`:$Port/api/health" -UseBasicParsing -TimeoutSec 8
  Write-Host "Backend started (PID=$($proc.Id)): $($resp.Content)"
}
catch {
  Write-Host "Backend failed to start. Check logs:"
  Write-Host "- $OutLog"
  Write-Host "- $ErrLog"
  exit 1
}
