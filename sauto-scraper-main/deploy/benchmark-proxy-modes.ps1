param(
    [Parameter(Mandatory = $false)]
    [string]$ApiBase = "http://127.0.0.1:8000",

    [Parameter(Mandatory = $false)]
    [string]$ApiKey = "",

    [Parameter(Mandatory = $false)]
    [string]$ProjectId = "proxy-benchmark",

    [Parameter(Mandatory = $false)]
    [int]$PollSeconds = 3,

    [Parameter(Mandatory = $false)]
    [int]$TimeoutSeconds = 1800
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function New-Headers {
    param([string]$Token)
    $headers = @{}
    if ($Token) {
        $headers["x-api-key"] = $Token
    }
    return $headers
}

function Start-ProxyRun {
    param(
        [string]$BaseUrl,
        [string]$Mode,
        [string]$RunProjectId,
        [hashtable]$Headers
    )

    $stamp = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $outputFile = "data/proxy_benchmark_${Mode}_${stamp}.json"
    $body = @{
        output_file = $outputFile
        project_id = $RunProjectId
        run_mode = $Mode
    } | ConvertTo-Json -Depth 3

    $response = Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/run" -Headers $Headers -ContentType "application/json" -Body $body
    $jobId = ""
    if ($response -and $response.job -and $response.job.job_id) {
        $jobId = [string]$response.job.job_id
    } elseif ($response -and $response.job_id) {
        $jobId = [string]$response.job_id
    }

    if (-not $jobId) {
        throw "Backend did not return job_id for mode '$Mode'."
    }

    return [PSCustomObject]@{
        Mode = $Mode
        JobId = $jobId
        OutputFile = $outputFile
        StartedAt = [DateTimeOffset]::UtcNow
    }
}

function Wait-ForJob {
    param(
        [string]$BaseUrl,
        [string]$JobId,
        [hashtable]$Headers,
        [int]$Poll,
        [int]$Timeout
    )

    $deadline = (Get-Date).AddSeconds($Timeout)
    while ((Get-Date) -lt $deadline) {
        $jobResponse = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/jobs/$JobId" -Headers $Headers
        $job = $jobResponse.job
        if (-not $job) {
            throw "Job '$JobId' not found in /api/jobs/{job_id}."
        }

        $status = [string]$job.status
        if ($status -eq "finished" -or $status -eq "failed") {
            return $job
        }

        Start-Sleep -Seconds $Poll
    }

    throw "Timed out waiting for job '$JobId' after $Timeout seconds."
}

function Get-ItemCountFromOutput {
    param([string]$OutputFile)

    if (-not $OutputFile) {
        return 0
    }

    $path = Join-Path (Get-Location) $OutputFile
    if (-not (Test-Path $path)) {
        return 0
    }

    try {
        $raw = Get-Content -Raw -Path $path
        if (-not $raw) {
            return 0
        }
        $parsed = $raw | ConvertFrom-Json
        if ($parsed -is [System.Array]) {
            return [int]$parsed.Count
        }
        return 0
    } catch {
        return 0
    }
}

$headers = New-Headers -Token $ApiKey

Write-Host "Starting proxy benchmark against $ApiBase"
Write-Host "ProjectId: $ProjectId"

$freeRun = Start-ProxyRun -BaseUrl $ApiBase -Mode "free_proxy" -RunProjectId $ProjectId -Headers $headers
Write-Host "free_proxy queued as $($freeRun.JobId)"

$freeJob = Wait-ForJob -BaseUrl $ApiBase -JobId $freeRun.JobId -Headers $headers -Poll $PollSeconds -Timeout $TimeoutSeconds
$freeDuration = [double]($freeJob.finished_at - $freeJob.started_at)
$freeItemCount = Get-ItemCountFromOutput -OutputFile ([string]$freeJob.output_file)

$paidRun = Start-ProxyRun -BaseUrl $ApiBase -Mode "paid_proxy" -RunProjectId $ProjectId -Headers $headers
Write-Host "paid_proxy queued as $($paidRun.JobId)"

$paidJob = Wait-ForJob -BaseUrl $ApiBase -JobId $paidRun.JobId -Headers $headers -Poll $PollSeconds -Timeout $TimeoutSeconds
$paidDuration = [double]($paidJob.finished_at - $paidJob.started_at)
$paidItemCount = Get-ItemCountFromOutput -OutputFile ([string]$paidJob.output_file)

$rows = @(
    [PSCustomObject]@{
        mode = "free_proxy"
        job_id = [string]$freeJob.job_id
        status = [string]$freeJob.status
        exit_code = [int]$freeJob.exit_code
        duration_sec = [Math]::Round($freeDuration, 2)
        item_count = $freeItemCount
        output_file = [string]$freeJob.output_file
    },
    [PSCustomObject]@{
        mode = "paid_proxy"
        job_id = [string]$paidJob.job_id
        status = [string]$paidJob.status
        exit_code = [int]$paidJob.exit_code
        duration_sec = [Math]::Round($paidDuration, 2)
        item_count = $paidItemCount
        output_file = [string]$paidJob.output_file
    }
)

Write-Host ""
Write-Host "Proxy mode benchmark summary:"
$rows | Format-Table -AutoSize | Out-String | Write-Host

if ($freeDuration -gt 0 -and $paidDuration -gt 0) {
    $speedup = [Math]::Round(($freeDuration / $paidDuration), 2)
    Write-Host "paid_proxy speed factor vs free_proxy: ${speedup}x (higher is faster paid mode)."
}
