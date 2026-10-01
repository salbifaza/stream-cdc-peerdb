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

# peerdb-server answering SQL doesn't mean it can act on it: CREATE PEER and
# CREATE MIRROR are forwarded to flow-api over gRPC, which comes up on its
# own schedule (after Temporal). On a cold start peerdb-server is routinely
# ready first, and CREATE PEER then fails with "tcp connect error ...:8112".
# Probe flow-api's HTTP gateway (same process as the gRPC server) from
# inside the docker network so host port mappings don't matter.
echo "Waiting for PeerDB flow-api ..."
deadline=$((SECONDS + 120))
until docker compose exec -T catalog wget -qO /dev/null http://flow_api:8113/v1/version 2>/dev/null; do
    if [ $SECONDS -ge $deadline ]; then
        echo "PeerDB flow-api did not become reachable within 120s." >&2
        exit 1
    fi
    sleep 3
done

# CREATE MIRROR starts a Temporal workflow tagged with the MirrorName search
# attribute, which temporal-admin-tools registers a few seconds *after* its
# healthcheck already passes (peerdb-internal/scripts/mirror-name-search.sh
# sleeps first). Until it exists, CREATE MIRROR fails with "Namespace
# default has no mapping defined for search attribute MirrorName".
echo "Waiting for Temporal search attribute MirrorName ..."
deadline=$((SECONDS + 120))
until docker compose exec -T temporal-admin-tools \
    temporal operator search-attribute list --namespace default 2>/dev/null | grep -qw MirrorName; do
    if [ $SECONDS -ge $deadline ]; then
        echo "MirrorName search attribute not registered within 120s -- check 'docker compose logs temporal-admin-tools'." >&2
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

# ON_ERROR_STOP makes psql exit non-zero on the first failed statement.
# Without it, a failed CREATE PEER is printed and skipped, the dependent
# CREATE MIRROR fails too, and the script still exits 0.
echo "$sql" | PGPASSWORD=peerdb docker compose exec -T catalog psql \
    "host=peerdb-server port=9900 user=peerdb password=peerdb dbname=postgres" \
    -v ON_ERROR_STOP=1 -f -
