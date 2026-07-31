param(
  [Parameter(Mandatory = $false)]
  [switch]$AutoidxWithWatchdog
)

$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
  $PSNativeCommandUseErrorActionPreference = $false
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$autoidxScript = Join-Path $PSScriptRoot "start-autoidx-clean.ps1"
$mailpilotSupervisor = "C:\ORCAVE\mailchimp\scripts\mailpilot-supervisor.ps1"

function Write-Info([string]$Message) {
  Write-Host "[INFO] $Message"
}

function Ensure-Path([string]$Path, [string]$Label) {
  if (-not (Test-Path $Path)) {
    throw "$Label not found: $Path"
  }
}

function Start-MailPilotSupervisor {
  param([string]$SupervisorPath)

  $existing = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
    Where-Object {
      $_.Name -eq "powershell.exe" -and
      $_.CommandLine -and
      $_.CommandLine -match [regex]::Escape("mailpilot-supervisor.ps1")
    }

  if ($existing) {
    Write-Info "MailPilot supervisor already running."
    return
  }

  Start-Process -FilePath "powershell.exe" `
    -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $SupervisorPath) `
    -WorkingDirectory (Split-Path -Parent $SupervisorPath) `
    -WindowStyle Hidden | Out-Null

  Write-Info "MailPilot supervisor started."
}

Ensure-Path -Path $autoidxScript -Label "Autoidx startup script"
Ensure-Path -Path $mailpilotSupervisor -Label "MailPilot supervisor script"

Write-Info "Starting Autoidx services..."
if ($AutoidxWithWatchdog) {
  & $autoidxScript -WithWatchdog
} else {
  & $autoidxScript
}

Write-Info "Starting MailPilot services..."
Start-MailPilotSupervisor -SupervisorPath $mailpilotSupervisor

Write-Info "Unified startup complete."
