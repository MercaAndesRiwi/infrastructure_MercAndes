param(
    [ValidateSet("alertas.calidad_datos", "alertas.riesgo_quiebre")]
    [string]$Queue = "alertas.riesgo_quiebre"
)

$ErrorActionPreference = "Stop"

& docker compose run --rm operational-alerts-worker python -m operational_messaging.publish_test_alerts --count 1 --queue $Queue --fail
if ($LASTEXITCODE -ne 0) {
    throw "Failed to publish the invalid test alert"
}

$deadLetterQueue = "$Queue.dlq"
$deadline = (Get-Date).AddSeconds(90)
do {
    $rows = & docker compose exec -T rabbitmq rabbitmqctl list_queues name messages_ready
    if ($LASTEXITCODE -ne 0) {
        throw "Could not inspect RabbitMQ queue depth"
    }
    $row = $rows | Select-String -Pattern "^$([regex]::Escape($deadLetterQueue))\s+(\d+)"
    if ($row -and [int]$row.Matches[0].Groups[1].Value -ge 1) {
        Write-Host "PASS: the invalid message reached $deadLetterQueue after bounded retries."
        return
    }
    Start-Sleep -Seconds 3
} while ((Get-Date) -lt $deadline)

throw "The invalid message did not reach $deadLetterQueue within 90 seconds"
