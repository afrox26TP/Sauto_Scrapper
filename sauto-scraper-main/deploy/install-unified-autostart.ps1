param(
  [Parameter(Mandatory = $false)]
  [string]$TaskName = "UnifiedAutoidxMailpilot",

  [Parameter(Mandatory = $false)]
  [switch]$AutoidxWithWatchdog,

  [Parameter(Mandatory = $false)]
  [switch]$RunNow
)

$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
  $PSNativeCommandUseErrorActionPreference = $false
}

$scriptPath = Join-Path $PSScriptRoot "start-unified-services.ps1"
if (-not (Test-Path $scriptPath)) {
  throw "Missing startup script: $scriptPath"
}

# Disable old tasks to avoid duplicate starts.
$legacyTasks = @("MailPilot Backend", "MailPilot Cloudflare Tunnel", "SautoAutoidxLive")
foreach ($legacy in $legacyTasks) {
  Disable-ScheduledTask -TaskName $legacy -TaskPath "\\" -ErrorAction SilentlyContinue | Out-Null
}

$currentUser = "$env:USERDOMAIN\$env:USERNAME"
$argList = @("-NoProfile", "-ExecutionPolicy", "Bypass", "-WindowStyle", "Hidden", "-File", "`"$scriptPath`"")
if ($AutoidxWithWatchdog) {
  $argList += "-AutoidxWithWatchdog"
}

$taskCommand = "powershell.exe " + ($argList -join " ")

# Use schtasks for broader compatibility with local Windows task settings.
$createArgs = @(
  "/Create",
  "/TN", $TaskName,
  "/TR", $taskCommand,
  "/SC", "ONLOGON",
  "/RU", $currentUser,
  "/RL", "LIMITED",
  "/F"
)

$null = & schtasks.exe @createArgs
if ($LASTEXITCODE -ne 0) {
  throw "Failed to create scheduled task '$TaskName' (exit code $LASTEXITCODE)."
}

Write-Host "[INFO] Installed task '$TaskName' for user $currentUser"
Write-Host "[INFO] Legacy tasks disabled: $($legacyTasks -join ', ')"

if ($RunNow) {
  Write-Host "[INFO] Running unified startup now..."
  if ($AutoidxWithWatchdog) {
    & $scriptPath -AutoidxWithWatchdog
  } else {
    & $scriptPath
  }
}
