#!/bin/bash
# Creates the least-privilege user PeerDB's ClickHouse peer actually
# connects as. CLICKHOUSE_USER/CLICKHOUSE_PASSWORD (with
# CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 in docker-compose.yml) is a
# bootstrap/admin identity used only to run this script -- nothing else
# should authenticate as it. The grants below are exactly what PeerDB's own
# docs list as required for a ClickHouse destination, no broader:
# https://docs.peerdb.io/connect/clickhouse
set -e

CH=(clickhouse-client -u "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}")

"${CH[@]}" -q "CREATE USER IF NOT EXISTS ${CLICKHOUSE_ETL_USER} IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_ETL_PASSWORD}'"

# Data manipulation + schema evolution, scoped to the peerdb database only.
"${CH[@]}" -q "GRANT INSERT, SELECT, DROP, CREATE TABLE, ALTER ADD COLUMN ON ${CLICKHOUSE_DB}.* TO ${CLICKHOUSE_ETL_USER}"

# The S3 staging path PeerDB uses for bulk loads needs CREATE TEMPORARY
# TABLE and S3 -- these can't be scoped to one database since the
# intermediary table isn't created inside `peerdb`.
"${CH[@]}" -q "GRANT CREATE TEMPORARY TABLE, S3 ON *.* TO ${CLICKHOUSE_ETL_USER}"

echo "$0: peerdb_etl user provisioned with least-privilege grants"
