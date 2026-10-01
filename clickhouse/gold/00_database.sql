-- Gold layer: business-shaped tables derived from the PeerDB-mirrored
-- `peerdb` database (the silver layer). Kept in its own database so that
-- (1) PeerDB's least-privilege peerdb_etl user, whose grants are scoped to
-- peerdb.*, can never touch it, and (2) a RESYNC MIRROR that drops and
-- recreates peerdb.* tables can't take gold down with it.
--
-- Applied by scripts/create_gold.sh as the ch_admin operator user, not at
-- container boot: the source tables these views read from only exist once
-- the mirror has created them.
CREATE DATABASE IF NOT EXISTS gold;
