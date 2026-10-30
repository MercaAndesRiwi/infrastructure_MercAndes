$ErrorActionPreference = "Stop"
$checks = @(
  @{ Name = "inventory-api"; Url = "http://localhost:8002/" },
  @{ Name = "marketplace-api"; Url = "http://localhost:8003/" },
  @{ Name = "f7-weekly-promotions"; Url = "http://localhost:8004/health" },
  @{ Name = "reports-api"; Url = "http://localhost:8005/health" },
  @{ Name = "rabbitmq-mgmt"; Url = "http://localhost:15672" },
  @{ Name = "spark-master"; Url = "http://localhost:8081" },
  @{ Name = "airflow-api"; Url = "http://localhost:8080/api/v2/version" },
  @{ Name = "airflow-flower"; Url = "http://localhost:5555" }
)
foreach ($c in $checks) {
  $r = Invoke-WebRequest -Uri $c.Url -UseBasicParsing -TimeoutSec 15
  Write-Output "$($c.Name): $($r.StatusCode)"
}
docker compose ps
