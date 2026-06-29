param(
  [Parameter(Mandatory = $false)]
  [string]$ApiBase = "http://127.0.0.1:8000"
)

$ErrorActionPreference = "Stop"

try {
  $health = Invoke-WebRequest -Uri "$ApiBase/api/health" -UseBasicParsing -TimeoutSec 8
  Write-Host "health_status=$($health.StatusCode)"
  Write-Host $health.Content
}
catch {
  Write-Host "health_error=$($_.Exception.Message)"
  exit 1
}

$checks = @(
  "/api/catalog/brands",
  "/api/results"
)

foreach ($path in $checks) {
  try {
    $resp = Invoke-WebRequest -Uri "$ApiBase$path" -UseBasicParsing -TimeoutSec 20
    Write-Host "$path status=$($resp.StatusCode)"
  }
  catch {
    Write-Host "$path error=$($_.Exception.Message)"
  }
}
