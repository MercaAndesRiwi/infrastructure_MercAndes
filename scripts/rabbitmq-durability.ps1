param(
    [ValidateRange(1, 10)]
    [int]$Count = 5,
    [ValidateRange(1, 60)]
    [int]$WaitMinutes = 10,
    [ValidateSet("alertas.calidad_datos", "alertas.riesgo_quiebre")]
    [string]$Queue = "alertas.riesgo_quiebre"
)

$ErrorActionPreference = "Stop"

function Invoke-Compose {
    param([string[]]$Arguments)
    & docker compose @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

Invoke-Compose -Arguments @("stop", "operational-alerts-worker")

try {
    $publishOutput = & docker compose run --rm operational-alerts-worker python -m operational_messaging.publish_test_alerts --count $Count --queue $Queue
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to publish durability test alerts"
    }
    $publishOutput | ForEach-Object { Write-Host $_ }
    $runMatch = ($publishOutput | Select-String -Pattern "run_id=([a-f0-9-]+)" | Select-Object -Last 1)
    if (-not $runMatch) {
        throw "Could not read the durability test run ID"
    }
    $runId = $runMatch.Matches[0].Groups[1].Value

    $queueRows = & docker compose exec -T rabbitmq rabbitmqctl list_queues name messages_ready
    if ($LASTEXITCODE -ne 0) {
        throw "Could not inspect RabbitMQ queue depth"
    }
    $queueRow = $queueRows | Select-String -Pattern "^$([regex]::Escape($Queue))\s+(\d+)"
    if (-not $queueRow -or [int]$queueRow.Matches[0].Groups[1].Value -lt $Count) {
        throw "RabbitMQ did not retain all $Count messages in $Queue while its consumer was stopped"
    }

    Write-Host "All $Count messages are queued. Waiting $WaitMinutes minute(s)..."
    Start-Sleep -Seconds ($WaitMinutes * 60)
}
finally {
    Invoke-Compose -Arguments @("start", "operational-alerts-worker")
}

$deadline = (Get-Date).AddSeconds(90)
do {
    $matches = & docker compose exec -T operational-alerts-worker sh -c "grep -c 'durability-$runId-' /app/alerts/processed.jsonl 2>/dev/null || true"
    if ($LASTEXITCODE -eq 0 -and $matches -and [int]$matches[0] -ge $Count) {
        Write-Host "PASS: all $Count messages were processed after the $WaitMinutes-minute stop."
        return
    }
    Start-Sleep -Seconds 3
} while ((Get-Date) -lt $deadline)

throw "Only $($matches[0]) of $Count messages were recorded after the consumer restarted"
