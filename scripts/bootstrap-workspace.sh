#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(dirname "$SCRIPT_DIR")"
PARENT_DIR="$(dirname "$PLATFORM_DIR")"

echo "=========================================================="
echo "   MercaAndes Workspace Setup & Compatibility Bootstrap   "
echo "=========================================================="
echo "Directorio Data Platform: $PLATFORM_DIR"
echo "Directorio Padre (Workspace): $PARENT_DIR"
echo ""

# 1. Validar .env
if [ ! -f "$PLATFORM_DIR/.env" ]; then
    echo "[+] Creando archivo .env desde .env.example..."
    cp "$PLATFORM_DIR/.env.example" "$PLATFORM_DIR/.env"
    echo "[OK] Archivo .env inicializado con valores por defecto."
else
    echo "[OK] Archivo .env ya existe."
fi

# 2. Verificar y clonar repositorios hermanos
REPOS=(
    "Transactional-POS-database:https://github.com/MercaAndesRiwi/Transactional-POS-database.git"
    "Inventory_Api:https://github.com/MercaAndesRiwi/Inventory_Api.git"
    "Marketplace_Api:https://github.com/MercaAndesRiwi/Marketplace_Api.git"
    "MercaAndes_Spark_Gold_Transformations:https://github.com/MercaAndesRiwi/MercaAndes_Spark_Gold_Transformations.git"
    "api-exchange-rates:https://github.com/MercaAndesRiwi/api-exchange-rates.git"
    "api-holidays:https://github.com/MercaAndesRiwi/api-holidays.git"
)

echo ""
echo "Verificando repositorios complementarios..."
for item in "${REPOS[@]}"; do
    NAME="${item%%:*}"
    URL="${item#*:}"
    TARGET="$PARENT_DIR/$NAME"
    if [ ! -d "$TARGET" ]; then
        echo "[+] Clonando repositorio faltante: $NAME..."
        git clone "$URL" "$TARGET"
        echo "[OK] $NAME clonado correctamente."
    else
        echo "[OK] $NAME ya esta presente en el workspace."
    fi
done

# 3. Validar Docker Compose
echo ""
echo "Validando sintaxis de docker-compose.yml..."
cd "$PLATFORM_DIR"
docker compose config > /dev/null
echo "[OK] Docker Compose valido y listo para cualquier dispositivo."

echo ""
echo "=========================================================="
echo " Entorno listo. Para iniciar todos los servicios ejecuta:  "
echo "   docker compose up -d --build                           "
echo "=========================================================="
