#!/usr/bin/env bash
# Applies scripts/create_mirror.sql against peerdb-server's SQL interface.
# Idempotent: every statement in create_mirror.sql uses IF NOT EXISTS.
set -euo pipefail
cd "$(dirname "$0")/.."

PGPASSWORD=peerdb docker compose exec -T catalog psql \
    "host=peerdb-server port=9900 user=peerdb password=peerdb dbname=postgres" \
    -f - < scripts/create_mirror.sql
