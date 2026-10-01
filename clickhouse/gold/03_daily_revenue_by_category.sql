-- Daily revenue per product category, from line items of non-cancelled
-- orders. A four-way join across silver tables, any of which can change
-- under it: a deleted line item, a cancelled order, or a product moved to
-- another category all have to show up here, and a full recompute handles
-- every one of them the same way.
--
-- Days are bucketed in UTC explicitly rather than relying on the server
-- timezone, so the result is stable regardless of where ClickHouse runs
-- (and matches the Postgres-side reconciliation in scripts/verify_gold.sh).
DROP VIEW IF EXISTS gold.daily_revenue_by_category SYNC;

CREATE MATERIALIZED VIEW gold.daily_revenue_by_category
REFRESH EVERY 10 SECOND
ENGINE = MergeTree
ORDER BY (day, category)
AS
SELECT
    toDate(o.created_at, 'UTC')            AS day,
    cat.name                               AS category,
    uniqExact(o.order_id)                  AS orders,
    sum(oi.quantity)                       AS units,
    sum(oi.quantity * oi.unit_price_cents) AS revenue_cents
FROM
(
    SELECT order_id, product_id, quantity, unit_price_cents
    FROM peerdb.order_items FINAL
    WHERE _peerdb_is_deleted = 0
) AS oi
INNER JOIN
(
    SELECT order_id, created_at
    FROM peerdb.orders FINAL
    WHERE _peerdb_is_deleted = 0 AND status != 'cancelled'
) AS o ON o.order_id = oi.order_id
INNER JOIN
(
    SELECT product_id, category_id
    FROM peerdb.products FINAL
    WHERE _peerdb_is_deleted = 0
) AS p ON p.product_id = oi.product_id
INNER JOIN
(
    SELECT category_id, name
    FROM peerdb.categories FINAL
    WHERE _peerdb_is_deleted = 0
) AS cat ON cat.category_id = p.category_id
GROUP BY day, category;
