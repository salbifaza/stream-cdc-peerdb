#!/usr/bin/env bash
# Applies clickhouse/gold/*.sql (in filename order) as the ch_admin
# operator user, creating the gold database and its refreshable
# materialized views.
#
# Re-runnable: each view file drops and recreates its view, so editing a
# view's SQL and re-running this script is how you deploy the change. Gold
# is fully derived from silver, so dropping it loses nothing -- the first
# refresh runs immediately on CREATE.
#
# Must run after the mirror exists: a refreshable MV's SELECT is validated
# at CREATE time, so the peerdb.* tables it reads have to exist already.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then
    set -a; . ./.env; set +a
fi

CH_EXEC="docker compose exec -T clickhouse clickhouse-client --user ${CLICKHOUSE_USER:-ch_admin} --password ${CLICKHOUSE_PASSWORD:-ch_admin_password}"

TABLES=(categories customers products orders order_items payments)

echo "Waiting for the mirror to create all ${#TABLES[@]} silver tables in peerdb.* ..."
table_list=$(printf "'%s'," "${TABLES[@]}"); table_list=${table_list%,}
deadline=$((SECONDS + 120))
until [ "$($CH_EXEC -q "SELECT count() FROM system.tables WHERE database = 'peerdb' AND name IN (${table_list})" 2>/dev/null)" = "${#TABLES[@]}" ]; do
    if [ $SECONDS -ge $deadline ]; then
        echo "Silver tables did not appear within 120s -- has 'make mirror' been run?" >&2
        exit 1
    fi
    sleep 3
done

for f in clickhouse/gold/*.sql; do
    echo "  applying ${f}"
    $CH_EXEC --multiquery < "$f"
done

echo
echo "Gold views (refresh status):"
$CH_EXEC -q "
    SELECT view, status, last_success_time, next_refresh_time
    FROM system.view_refreshes
    WHERE database = 'gold'
    ORDER BY view
    FORMAT PrettyCompactMonoBlock"
