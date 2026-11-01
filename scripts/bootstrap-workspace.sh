#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORM_DIR="$(dirname "$SCRIPT_DIR")"
PARENT_DIR="$(dirname "$PLATFORM_DIR")"

echo "=========================================================="
echo "   MercaAndes Workspace Setup & Auto-Healing Bootstrap    "
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

# 2. Verificar y clonar repositorios hermanos con Auto-Healing
REPOS=(
    "Transactional-POS-database:https://github.com/MercaAndesRiwi/Transactional-POS-database.git:Dockerfile"
    "Inventory_Api:https://github.com/MercaAndesRiwi/Inventory_Api.git:Dockerfile"
    "Marketplace_Api:https://github.com/MercaAndesRiwi/Marketplace_Api.git:Dockerfile"
    "MercaAndes_Spark_Gold_Transformations:https://github.com/MercaAndesRiwi/MercaAndes_Spark_Gold_Transformations.git:src"
    "api-exchange-rates:https://github.com/MercaAndesRiwi/api-exchange-rates.git:Dockerfile"
    "api-holidays:https://github.com/MercaAndesRiwi/api-holidays.git:Dockerfile"
)

echo ""
echo "Verificando repositorios complementarios..."
for item in "${REPOS[@]}"; do
    IFS=':' read -r NAME URL KEYFILE <<< "$item"
    TARGET="$PARENT_DIR/$NAME"
    if [ ! -d "$TARGET" ]; then
        echo "[+] Clonando repositorio faltante: $NAME..."
        git clone "$URL" "$TARGET"
        echo "[OK] $NAME clonado correctamente."
    elif [ ! -e "$TARGET/$KEYFILE" ]; then
        echo "[!] Carpeta $NAME existe pero le falta $KEYFILE. Re-clonando..."
        rm -rf "$TARGET"
        git clone "$URL" "$TARGET"
        echo "[OK] $NAME re-clonado con exito."
    else
        echo "[OK] $NAME esta presente y completo en el workspace."
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
