param(
  [Parameter(Mandatory = $true)]
  [string]$Zone,

  [Parameter(Mandatory = $false)]
  [string]$TunnelName = "sauto-api",

  [Parameter(Mandatory = $false)]
  [string]$ApiHostname = "api.your-domain.com"
)

$ErrorActionPreference = "Stop"

Write-Host "[1/6] Checking cloudflared..."
if (-not (Get-Command cloudflared -ErrorAction SilentlyContinue)) {
  throw "cloudflared is not installed. Install it first: https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/"
}

Write-Host "[2/6] Login to Cloudflare (browser will open)..."
cloudflared tunnel login

Write-Host "[3/6] Creating tunnel: $TunnelName"
cloudflared tunnel create $TunnelName

Write-Host "[4/6] Creating DNS route: $ApiHostname"
cloudflared tunnel route dns $TunnelName $ApiHostname

$cfDir = Join-Path $env:USERPROFILE ".cloudflared"
$configPath = Join-Path $cfDir "config.yml"

Write-Host "[5/6] Writing config to $configPath"
if (-not (Test-Path $cfDir)) {
  New-Item -ItemType Directory -Path $cfDir | Out-Null
}

$cred = Get-ChildItem -Path $cfDir -Filter "*.json" | Select-Object -First 1
if (-not $cred) {
  throw "No credentials JSON file found in $cfDir."
}

$config = @"
tunnel: $TunnelName
credentials-file: $($cred.FullName)

ingress:
  - hostname: $ApiHostname
    service: http://127.0.0.1:8000
  - service: http_status:404
"@

Set-Content -Path $configPath -Value $config -Encoding UTF8

Write-Host "[6/6] Done. Start tunnel with:"
Write-Host "cloudflared tunnel run $TunnelName"
Write-Host ""
Write-Host "Remember to set web-api/.env:"
Write-Host "CORS_ALLOW_ORIGINS=https://app.$Zone"
Write-Host ""
Write-Host "And Cloudflare Pages env var:"
Write-Host "VITE_API_BASE_URL=https://$ApiHostname"
