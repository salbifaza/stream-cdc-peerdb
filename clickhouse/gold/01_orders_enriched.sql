-- One row per live order, denormalized with its customer, line items, and
-- payments: the "wide fact" shape most dashboards want to start from.
--
-- Every silver read below follows the same rule: FINAL to collapse
-- ReplacingMergeTree versions to the latest one, then
-- _peerdb_is_deleted = 0 to drop soft-delete tombstones -- both *inside*
-- the subquery, so the filter applies to the already-collapsed row rather
-- than to an older version that happens to be undeleted.
--
-- Why a refreshable MV and not an incremental one: an incremental MV only
-- fires on inserts into the left-most table, so a customer changing
-- country would never update the gold rows for that customer's existing
-- orders. A full recompute every refresh has no such blind spot.
DROP VIEW IF EXISTS gold.orders_enriched SYNC;

CREATE MATERIALIZED VIEW gold.orders_enriched
REFRESH EVERY 10 SECOND
ENGINE = MergeTree
ORDER BY order_id
AS
SELECT
    o.order_id                       AS order_id,
    o.customer_id                    AS customer_id,
    c.email                          AS customer_email,
    c.country                        AS customer_country,
    o.status                         AS status,
    o.order_total_cents              AS order_total_cents,
    i.item_count                     AS item_count,
    i.units                          AS units,
    i.items_total_cents              AS items_total_cents,
    p.paid_cents                     AS paid_cents,
    o.created_at                     AS created_at,
    o.updated_at                     AS updated_at
FROM
(
    SELECT order_id, customer_id, status, order_total_cents, created_at, updated_at
    FROM peerdb.orders FINAL
    WHERE _peerdb_is_deleted = 0
) AS o
LEFT JOIN
(
    SELECT customer_id, email, country
    FROM peerdb.customers FINAL
    WHERE _peerdb_is_deleted = 0
) AS c ON c.customer_id = o.customer_id
LEFT JOIN
(
    SELECT
        order_id,
        count()                          AS item_count,
        sum(quantity)                    AS units,
        sum(quantity * unit_price_cents) AS items_total_cents
    FROM peerdb.order_items FINAL
    WHERE _peerdb_is_deleted = 0
    GROUP BY order_id
) AS i ON i.order_id = o.order_id
LEFT JOIN
(
    SELECT order_id, sumIf(amount_cents, status = 'succeeded') AS paid_cents
    FROM peerdb.payments FINAL
    WHERE _peerdb_is_deleted = 0
    GROUP BY order_id
) AS p ON p.order_id = o.order_id;
