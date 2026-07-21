#!/bin/sh
# PeerDB's flow-worker/flow-snapshot-worker containers connect from other
# containers on the compose network, not localhost, so the default
# pg_hba.conf (which only trusts local replication connections) won't let
# them open a replication stream. Add an explicit, password-authenticated
# rule scoped to the replication pseudo-database rather than reaching for
# `trust`, which is what a real ingest-user setup should look like too.
set -e
echo "host replication ${POSTGRES_USER} 0.0.0.0/0 scram-sha-256" >> "$PGDATA/pg_hba.conf"
