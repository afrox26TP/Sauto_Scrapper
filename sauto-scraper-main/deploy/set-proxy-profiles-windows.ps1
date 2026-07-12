param(
    [Parameter(Mandatory = $false)]
    [string]$FreeProxyList = "",

    [Parameter(Mandatory = $false)]
    [string]$FreeProxyUrl = "",

    [Parameter(Mandatory = $false)]
    [string]$PaidProxyList = "",

    [Parameter(Mandatory = $false)]
    [string]$PaidProxyUrl = "",

    [Parameter(Mandatory = $false)]
    [ValidateSet("round_robin", "random")]
    [string]$FreeProxyMode = "round_robin",

    [Parameter(Mandatory = $false)]
    [ValidateSet("round_robin", "random")]
    [string]$PaidProxyMode = "round_robin"
)

$target = "User"

[Environment]::SetEnvironmentVariable("SAUTO_FREE_PROXY_LIST", $FreeProxyList, $target)
[Environment]::SetEnvironmentVariable("SAUTO_FREE_PROXY_URL", $FreeProxyUrl, $target)
[Environment]::SetEnvironmentVariable("SAUTO_FREE_PROXY_MODE", $FreeProxyMode, $target)

[Environment]::SetEnvironmentVariable("SAUTO_PAID_PROXY_LIST", $PaidProxyList, $target)
[Environment]::SetEnvironmentVariable("SAUTO_PAID_PROXY_URL", $PaidProxyUrl, $target)
[Environment]::SetEnvironmentVariable("SAUTO_PAID_PROXY_MODE", $PaidProxyMode, $target)

# Legacy fallback used by middleware and billing checks.
if ($PaidProxyList -or $PaidProxyUrl) {
    [Environment]::SetEnvironmentVariable("SAUTO_PROXY_LIST", $PaidProxyList, $target)
    [Environment]::SetEnvironmentVariable("SAUTO_PROXY_URL", $PaidProxyUrl, $target)
    [Environment]::SetEnvironmentVariable("SAUTO_PROXY_MODE", $PaidProxyMode, $target)
}

Write-Host "Saved proxy profiles to USER environment variables."
Write-Host "Open a new terminal before starting backend so new env vars are loaded."

$names = @(
    "SAUTO_FREE_PROXY_LIST",
    "SAUTO_FREE_PROXY_URL",
    "SAUTO_FREE_PROXY_MODE",
    "SAUTO_PAID_PROXY_LIST",
    "SAUTO_PAID_PROXY_URL",
    "SAUTO_PAID_PROXY_MODE",
    "SAUTO_PROXY_LIST",
    "SAUTO_PROXY_URL",
    "SAUTO_PROXY_MODE"
)

foreach ($name in $names) {
    $value = [Environment]::GetEnvironmentVariable($name, "User")
    if ($value) {
        if ($name -like "*URL" -or $name -like "*LIST") {
            Write-Host "$name=<set>"
        } else {
            Write-Host "$name=$value"
        }
    } else {
        Write-Host "$name=<not set>"
    }
}
