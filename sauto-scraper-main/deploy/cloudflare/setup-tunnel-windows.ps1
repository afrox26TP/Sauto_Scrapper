param(
  [Parameter(Mandatory = $true)]
  [string]$Zone,

  [Parameter(Mandatory = $false)]
  [string]$TunnelName = "sauto-api",

  [Parameter(Mandatory = $false)]
  [string]$ApiHostname = "api.your-domain.com",

  [Parameter(Mandatory = $false)]
  [string]$CloudflaredPath = "c:\scraper\tools\cloudflared\cloudflared.exe"
)

$ErrorActionPreference = "Stop"

Write-Host "[1/6] Checking cloudflared..."
$cloudflaredCmd = ""
$cmd = Get-Command cloudflared -ErrorAction SilentlyContinue
if ($cmd) {
  $cloudflaredCmd = $cmd.Source
} elseif (Test-Path $CloudflaredPath) {
  $cloudflaredCmd = $CloudflaredPath
} else {
  throw "cloudflared was not found in PATH and local binary was not found at $CloudflaredPath"
}
Write-Host "Using cloudflared: $cloudflaredCmd"

Write-Host "[2/6] Login to Cloudflare (browser will open)..."
& $cloudflaredCmd tunnel login

Write-Host "[3/6] Creating tunnel: $TunnelName"
& $cloudflaredCmd tunnel create $TunnelName

Write-Host "[4/6] Creating DNS route: $ApiHostname"
& $cloudflaredCmd tunnel route dns $TunnelName $ApiHostname

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
Write-Host "`"$cloudflaredCmd`" tunnel run $TunnelName"
Write-Host ""
Write-Host "Remember to set web-api/.env:"
Write-Host "CORS_ALLOW_ORIGINS=https://app.$Zone"
Write-Host ""
Write-Host "And Cloudflare Pages env var:"
Write-Host "VITE_API_BASE_URL=https://$ApiHostname"
