-- One row per customer with lifetime order count and value, excluding
-- cancelled orders. Customers with no qualifying orders are kept (LEFT
-- JOIN) with zeros, so the row count always matches the customer base.
--
-- This is the shape an incremental MV gets silently wrong: when an order
-- goes paid -> cancelled, PeerDB inserts a *new* version of the row, and an
-- insert-triggered sum() would keep the old version's contribution forever.
-- Recomputing from FINAL on each refresh means cancelling an order simply
-- removes it from the total.
DROP VIEW IF EXISTS gold.customer_ltv SYNC;

CREATE MATERIALIZED VIEW gold.customer_ltv
REFRESH EVERY 10 SECOND
ENGINE = MergeTree
ORDER BY customer_id
AS
SELECT
    c.customer_id                                        AS customer_id,
    c.email                                              AS email,
    c.country                                            AS country,
    o.orders                                             AS orders,
    o.lifetime_value_cents                               AS lifetime_value_cents,
    intDivOrZero(o.lifetime_value_cents, o.orders)       AS avg_order_value_cents,
    if(o.orders = 0, NULL, o.first_order_at)             AS first_order_at,
    if(o.orders = 0, NULL, o.last_order_at)              AS last_order_at
FROM
(
    SELECT customer_id, email, country
    FROM peerdb.customers FINAL
    WHERE _peerdb_is_deleted = 0
) AS c
LEFT JOIN
(
    SELECT
        customer_id,
        count()                AS orders,
        sum(order_total_cents) AS lifetime_value_cents,
        min(created_at)        AS first_order_at,
        max(created_at)        AS last_order_at
    FROM peerdb.orders FINAL
    WHERE _peerdb_is_deleted = 0 AND status != 'cancelled'
    GROUP BY customer_id
) AS o ON o.customer_id = c.customer_id;
