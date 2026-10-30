<#
.SYNOPSIS
    Prepara y sincroniza el entorno local de MercaAndes para correr en cualquier dispositivo.
.DESCRIPTION
    1. Asegura la existencia de .env a partir de .env.example.
    2. Verifica y clona los repositorios hermanos requeridos si no existen en la carpeta padre.
    3. Valida la configuracion de Docker Compose.
#>

[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$platformDir = Split-Path -Parent $scriptDir
$parentDir = Split-Path -Parent $platformDir

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "   MercaAndes Workspace Setup & Compatibility Bootstrap   " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Directorio Data Platform: $platformDir"
Write-Host "Directorio Padre (Workspace): $parentDir"
Write-Host ""

# 1. Validar .env
$envFile = Join-Path $platformDir ".env"
$envExample = Join-Path $platformDir ".env.example"
if (-not (Test-Path $envFile)) {
    Write-Host "[+] Creando archivo .env desde .env.example..." -ForegroundColor Yellow
    Copy-Item $envExample $envFile
    Write-Host "[OK] Archivo .env inicializado con valores por defecto." -ForegroundColor Green
} else {
    Write-Host "[OK] Archivo .env ya existe." -ForegroundColor Green
}

# 2. Verificar y clonar repositorios hermanos
$repos = @(
    @{ Name = "Transactional-POS-database"; Url = "https://github.com/MercaAndesRiwi/Transactional-POS-database.git" },
    @{ Name = "Inventory_Api"; Url = "https://github.com/MercaAndesRiwi/Inventory_Api.git" },
    @{ Name = "Marketplace_Api"; Url = "https://github.com/MercaAndesRiwi/Marketplace_Api.git" },
    @{ Name = "MercaAndes_Spark_Gold_Transformations"; Url = "https://github.com/MercaAndesRiwi/MercaAndes_Spark_Gold_Transformations.git" },
    @{ Name = "api-exchange-rates"; Url = "https://github.com/MercaAndesRiwi/api-exchange-rates.git" },
    @{ Name = "api-holidays"; Url = "https://github.com/MercaAndesRiwi/api-holidays.git" }
)

Write-Host ""
Write-Host "Verificando repositorios complementarios..." -ForegroundColor Cyan
foreach ($r in $repos) {
    $targetPath = Join-Path $parentDir $r.Name
    if (-not (Test-Path $targetPath)) {
        Write-Host "[+] Clonando repositorio faltante: $($r.Name)..." -ForegroundColor Yellow
        git clone $r.Url $targetPath
        if ($LASTEXITCODE -eq 0) {
            Write-Host "[OK] $($r.Name) clonado correctamente." -ForegroundColor Green
        } else {
            Write-Host "[WARN] No se pudo clonar $($r.Name). Verifica tus permisos de Git." -ForegroundColor Red
        }
    } else {
        Write-Host "[OK] $($r.Name) ya esta presente en el workspace." -ForegroundColor Green
    }
}

# 3. Validar Docker Compose
Write-Host ""
Write-Host "Validando sintaxis y resolucion de servicios en docker-compose.yml..." -ForegroundColor Cyan
Push-Location $platformDir
try {
    docker compose config > $null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] Docker Compose valido y listo para cualquier dispositivo." -ForegroundColor Green
    } else {
        Write-Host "[WARN] Docker compose config arrojo advertencias." -ForegroundColor Yellow
    }
} catch {
    Write-Host "[WARN] No se pudo invocar docker compose (verifica que Docker Desktop este instalado)." -ForegroundColor Yellow
} finally {
    Pop-Location
}

Write-Host ""
Write-Host "==========================================================" -ForegroundColor Green
Write-Host " Entorno listo. Para iniciar todos los servicios ejecuta:  " -ForegroundColor Green
Write-Host "   cd data-platform" -ForegroundColor White
Write-Host "   docker compose up -d --build" -ForegroundColor White
Write-Host "==========================================================" -ForegroundColor Green
