-- Run against peerdb-server's SQL interface (port 9900), e.g.:
--   docker compose exec -T catalog psql \
--     "host=peerdb-server port=9900 user=peerdb password=peerdb dbname=postgres" \
--     -f scripts/create_mirror.sql
--
-- peerdb-server's default connection password is "peerdb" -- it's not the
-- source or destination credentials, it's PeerDB's own control-plane auth.

-- Source peer: the e-commerce OLTP database.
CREATE PEER IF NOT EXISTS source_pg FROM POSTGRES WITH (
    host = 'source-postgres',
    port = '5432',
    user = 'ecommerce',
    password = 'ecommerce',
    database = 'ecommerce'
);

-- Destination peer: ClickHouse, native protocol port, connecting as the
-- least-privilege peerdb_etl user (INSERT/SELECT/DROP/CREATE TABLE/ALTER
-- ADD COLUMN on peerdb.* + CREATE TEMPORARY TABLE/S3 on *.* -- see
-- clickhouse/init/01_peerdb_etl_user.sh) rather than the ch_admin
-- bootstrap user. If you changed CLICKHOUSE_ETL_USER/PASSWORD in .env,
-- update this to match -- it's not templated from .env.
--
-- S3 staging config (bucket/credentials/endpoint) is intentionally omitted
-- here -- it's already supplied stack-wide via the
-- PEERDB_CLICKHOUSE_AWS_CREDENTIALS_* env vars in docker-compose.yml,
-- which point at the bundled MinIO. That's what "For PeerDB OSS, a minio
-- bucket is provided as part of the stack" means in PeerDB's own docs: you
-- don't repeat S3 config per ClickHouse peer in a local OSS setup.
CREATE PEER IF NOT EXISTS ch_dest FROM CLICKHOUSE WITH (
    host = 'clickhouse',
    port = 9000,
    user = 'peerdb_etl',
    password = 'peerdb_etl_password',
    database = 'peerdb',
    disable_tls = true
);

-- The mirror itself: initial snapshot (do_initial_copy = true) followed by
-- continuous CDC streaming, using the publication already created in
-- postgres/init/01_schema.sql rather than letting PeerDB manage its own.
-- Target table names are deliberately NOT schema-qualified -- ClickHouse
-- mirrors reject `public.foo` as a destination table name.
CREATE MIRROR IF NOT EXISTS pg_to_ch
FROM source_pg TO ch_dest
WITH TABLE MAPPING (
  public.categories:categories,
  public.customers:customers,
  public.products:products,
  public.orders:orders,
  public.order_items:order_items,
  public.payments:payments
)
WITH (
  do_initial_copy = true,
  publication_name = 'peerdb_pub'
);
