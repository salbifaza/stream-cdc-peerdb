#!/usr/bin/env bash
# A scriptable equivalent of what peerdb-ui shows for a mirror, queried
# straight from PeerDB's own metadata store (the `catalog` Postgres
# database, schema `peerdb_stats`). Useful for a status check in CI, a cron
# job, or just not having to open a browser.
set -euo pipefail
cd "$(dirname "$0")/.."

CATALOG="docker compose exec -T catalog psql -U postgres -d postgres"

echo "== Replication lag (source LSN vs. last LSN applied to ClickHouse) =="
$CATALOG -c "
SELECT flow_name,
       latest_lsn_at_source,
       latest_lsn_at_target,
       (latest_lsn_at_source - latest_lsn_at_target) AS lsn_lag_bytes
FROM peerdb_stats.cdc_flows;"

echo "== Last 5 sync batches =="
$CATALOG -c "
SELECT flow_name, batch_id, rows_in_batch, start_time,
       (end_time - start_time) AS batch_duration
FROM peerdb_stats.cdc_batches
ORDER BY batch_id DESC
LIMIT 5;"

echo "== Per-table change counts synced so far =="
$CATALOG -c "
SELECT destination_table_name, inserts_count, updates_count, deletes_count,
       total_count, last_updated_at
FROM peerdb_stats.cdc_table_aggregate_counts
ORDER BY destination_table_name;"

echo "== Replication slot size on source (grows if CDC falls behind or stalls) =="
$CATALOG -c "
SELECT slot_name, peer_name, slot_size, wal_status, updated_at
FROM peerdb_stats.peer_slot_size
ORDER BY updated_at DESC
LIMIT 1;"

echo "== Real errors (excludes routine 'info' lifecycle log lines) =="
$CATALOG -c "
SELECT error_timestamp, flow_name, error_type, left(error_message, 120) AS message
FROM peerdb_stats.flow_errors
WHERE error_type <> 'info'
ORDER BY error_timestamp DESC
LIMIT 10;"

# Gold refreshes live in ClickHouse, not PeerDB's catalog. A failed refresh
# keeps serving the last good result, so "exception is non-empty" and
# "last_success_time is old" are the signals to alert on -- the data itself
# won't look broken, just stale.
if [ -f .env ]; then
    set -a; . ./.env; set +a
fi
echo "== Gold refreshable views (a non-empty exception means gold is serving stale data) =="
docker compose exec -T clickhouse clickhouse-client \
    --user "${CLICKHOUSE_USER:-ch_admin}" --password "${CLICKHOUSE_PASSWORD:-ch_admin_password}" -q "
SELECT view, status, last_success_time, last_success_duration_ms AS last_ms,
       written_rows, retry, left(exception, 80) AS exception
FROM system.view_refreshes
WHERE database = 'gold'
ORDER BY view
FORMAT PrettyCompactMonoBlock" </dev/null || echo "  (gold layer not created -- run 'make gold')"
