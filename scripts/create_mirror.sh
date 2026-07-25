#!/usr/bin/env bash
# Applies scripts/create_mirror.sql against peerdb-server's SQL interface.
# Idempotent: every statement in create_mirror.sql uses IF NOT EXISTS.
#
# Credentials from .env are injected at apply time via sed, so the
# checked-in SQL file doesn't need to match .env exactly — change .env,
# re-run this script, and the peers pick up the new values.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then
    set -a; . ./.env; set +a
fi

echo "Waiting for PeerDB SQL interface (peerdb-server) ..."
deadline=$((SECONDS + 120))
until PGPASSWORD=peerdb docker compose exec -T catalog psql \
    "host=peerdb-server port=9900 user=peerdb password=peerdb dbname=postgres" \
    -c "SELECT 1" >/dev/null 2>&1; do
    if [ $SECONDS -ge $deadline ]; then
        echo "PeerDB server did not become reachable within 120s." >&2
        exit 1
    fi
    sleep 3
done

sql=$(cat scripts/create_mirror.sql)
sql=$(echo "$sql" | sed \
    -e "s|user = 'ecommerce'|user = '${SOURCE_PG_USER:-ecommerce}'|" \
    -e "s|password = 'ecommerce'|password = '${SOURCE_PG_PASSWORD:-ecommerce}'|" \
    -e "s|database = 'ecommerce'|database = '${SOURCE_PG_DB:-ecommerce}'|" \
    -e "s|user = 'peerdb_etl'|user = '${CLICKHOUSE_ETL_USER:-peerdb_etl}'|" \
    -e "s|password = 'peerdb_etl_password'|password = '${CLICKHOUSE_ETL_PASSWORD:-peerdb_etl_password}'|")

echo "$sql" | PGPASSWORD=peerdb docker compose exec -T catalog psql \
    "host=peerdb-server port=9900 user=peerdb password=peerdb dbname=postgres" \
    -f -
