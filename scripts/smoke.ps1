$ErrorActionPreference = "Stop"
$checks = @(
  @{ Name = "pos-api"; Url = "http://localhost:8001/health" },
  @{ Name = "inventory-api"; Url = "http://localhost:8002/health" },
  @{ Name = "marketplace-api"; Url = "http://localhost:8003/health" },
  @{ Name = "rabbitmq-mgmt"; Url = "http://localhost:15672" },
  @{ Name = "spark-master"; Url = "http://localhost:8081" },
  @{ Name = "airflow-api"; Url = "http://localhost:8080/api/v2/version" }
)
foreach ($c in $checks) {
  $r = Invoke-WebRequest -Uri $c.Url -UseBasicParsing -TimeoutSec 15
  Write-Output "$($c.Name): $($r.StatusCode)"
}
docker compose ps
