#!/usr/bin/env bash
# Verifies the gold layer by reconciliation: each gold view is recomputed
# directly in source-postgres (the ground truth) and diffed against what
# ClickHouse's refreshable MV holds. Then it makes the source changes an
# insert-triggered (incremental) MV would get wrong -- an order cancelled
# after the fact, a customer attribute changing under existing orders, a
# line item deleted, plus a brand-new order -- and polls until gold
# reconciles again, reporting how long that took end to end.
#
# Run after scripts/create_gold.sh. Like verify_cdc.sh, this performs real
# writes against the source each time it runs.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then
    set -a; . ./.env; set +a
fi

COMPOSE="docker compose"
PG_EXEC="$COMPOSE exec -T source-postgres psql -U ${SOURCE_PG_USER:-ecommerce} -d ${SOURCE_PG_DB:-ecommerce} -tA -v ON_ERROR_STOP=1"
CH_EXEC="$COMPOSE exec -T clickhouse clickhouse-client --user ${CLICKHOUSE_USER:-ch_admin} --password ${CLICKHOUSE_PASSWORD:-ch_admin_password}"

VIEWS=(orders_enriched customer_ltv daily_revenue_by_category)

# Per view: the Postgres query that defines "correct", and the ClickHouse
# query that reads the same columns back out of gold. Timestamps are left
# out on purpose (the two systems format sub-second precision differently);
# everything compared is an integer, a string, or a UTC date.
pg_query() {
    case "$1" in
        orders_enriched) echo "
            SELECT o.order_id, c.email, c.country, o.status, o.order_total_cents,
                   coalesce(i.item_count, 0), coalesce(i.units, 0), coalesce(i.items_total_cents, 0),
                   coalesce(p.paid_cents, 0)
            FROM orders o
            LEFT JOIN customers c USING (customer_id)
            LEFT JOIN (SELECT order_id, count(*) AS item_count, sum(quantity) AS units,
                              sum(quantity * unit_price_cents) AS items_total_cents
                       FROM order_items GROUP BY order_id) i USING (order_id)
            LEFT JOIN (SELECT order_id, sum(amount_cents) FILTER (WHERE status = 'succeeded') AS paid_cents
                       FROM payments GROUP BY order_id) p USING (order_id);" ;;
        customer_ltv) echo "
            SELECT c.customer_id, c.email, c.country, coalesce(o.orders, 0), coalesce(o.ltv, 0),
                   coalesce(o.ltv / nullif(o.orders, 0), 0)
            FROM customers c
            LEFT JOIN (SELECT customer_id, count(*) AS orders, sum(order_total_cents) AS ltv
                       FROM orders WHERE status <> 'cancelled' GROUP BY customer_id) o USING (customer_id);" ;;
        daily_revenue_by_category) echo "
            SELECT (o.created_at AT TIME ZONE 'UTC')::date, cat.name, count(DISTINCT o.order_id),
                   sum(oi.quantity), sum(oi.quantity * oi.unit_price_cents)
            FROM order_items oi
            JOIN orders o USING (order_id)
            JOIN products p USING (product_id)
            JOIN categories cat ON cat.category_id = p.category_id
            WHERE o.status <> 'cancelled'
            GROUP BY 1, 2;" ;;
    esac
}

ch_query() {
    case "$1" in
        orders_enriched) echo "
            SELECT order_id, customer_email, customer_country, status, order_total_cents,
                   item_count, units, items_total_cents, paid_cents
            FROM gold.orders_enriched" ;;
        customer_ltv) echo "
            SELECT customer_id, email, country, orders, lifetime_value_cents, avg_order_value_cents
            FROM gold.customer_ltv" ;;
        daily_revenue_by_category) echo "
            SELECT day, category, orders, units, revenue_cents
            FROM gold.daily_revenue_by_category" ;;
    esac
}

# Both sides emit tab-separated rows; sorting with a fixed byte collation
# makes the comparison independent of either database's ORDER BY rules.
pg_rows() { $PG_EXEC -F $'\t' -c "$(pg_query "$1")" | LC_ALL=C sort; }
ch_rows() { $CH_EXEC -q "$(ch_query "$1") FORMAT TSV" | LC_ALL=C sort; }

view_matches() { [ "$(pg_rows "$1")" = "$(ch_rows "$1")" ]; }

# Polls until every view reconciles or the timeout passes. Prints one line
# per view; on timeout, prints the diff (< postgres, > gold) for each view
# that is still off.
reconcile_all() {
    local timeout=$1 start=$SECONDS
    local deadline=$((SECONDS + timeout))
    local -A ok=()
    while :; do
        local pending=0
        for v in "${VIEWS[@]}"; do
            if [ -z "${ok[$v]:-}" ]; then
                if view_matches "$v"; then ok[$v]=$((SECONDS - start)); else pending=1; fi
            fi
        done
        [ "$pending" -eq 0 ] && break
        [ $SECONDS -ge $deadline ] && break
        sleep 2
    done

    local fail=0
    for v in "${VIEWS[@]}"; do
        if [ -n "${ok[$v]:-}" ]; then
            printf "  %-26s rows=%-4s matches postgres (after %ss)\n" "$v" "$(ch_rows "$v" | wc -l)" "${ok[$v]}"
        else
            printf "  %-26s MISMATCH after %ss -- diff (< postgres, > gold):\n" "$v" "$timeout"
            diff <(pg_rows "$v") <(ch_rows "$v") | sed 's/^/      /' || true
            fail=1
        fi
    done
    return "$fail"
}

echo "== Step 1: reconcile gold against source-postgres (current state) =="
if ! reconcile_all 60; then
    echo "Gold does not match the source -- check 'make status' for CDC lag and system.view_refreshes for refresh errors." >&2
    exit 1
fi

echo
echo "== Step 2: changes an incremental MV would get wrong =="

cancel_id=$($PG_EXEC -c "
    SELECT o.order_id FROM orders o
    WHERE o.status <> 'cancelled' AND EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.order_id)
    ORDER BY o.order_id DESC LIMIT 1;")
echo "  cancelling order_id=${cancel_id} (its revenue must leave daily_revenue_by_category and customer_ltv)..."
$PG_EXEC -c "UPDATE orders SET status = 'cancelled', updated_at = now() WHERE order_id = ${cancel_id};" >/dev/null

echo "  changing customer_id=2's country (every existing order row in orders_enriched must follow)..."
$PG_EXEC -c "UPDATE customers SET country = CASE WHEN country = 'XA' THEN 'XB' ELSE 'XA' END,
                                   updated_at = now() WHERE customer_id = 2;" >/dev/null

del_id=$($PG_EXEC -c "
    SELECT oi.order_item_id FROM order_items oi
    WHERE (SELECT count(*) FROM order_items x WHERE x.order_id = oi.order_id) > 1
    ORDER BY oi.order_item_id DESC LIMIT 1;")
if [ -n "$del_id" ]; then
    echo "  deleting order_items.order_item_id=${del_id} from a multi-item order (item counts and revenue must drop)..."
    $PG_EXEC -c "DELETE FROM order_items WHERE order_item_id = ${del_id};" >/dev/null
fi

new_id=$($PG_EXEC -c "
    WITH o AS (
        INSERT INTO orders (customer_id, status, order_total_cents) VALUES (3, 'paid', 4499)
        RETURNING order_id
    ), i AS (
        INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents)
        SELECT order_id, 10, 1, 4499 FROM o
    ), p AS (
        INSERT INTO payments (order_id, amount_cents, method, status, processed_at)
        SELECT order_id, 4499, 'card', 'succeeded', now() FROM o
    )
    SELECT order_id FROM o;" | head -n1)
echo "  inserting new order_id=${new_id} with one item and a succeeded payment..."

echo "  polling until gold reconciles (CDC sync cycle + 10s refresh; up to 90s)..."
if reconcile_all 90; then
    echo
    echo "Gold verification PASSED."
else
    echo
    echo "Gold verification FAILED -- one or more views did not reconcile within 90s." >&2
    exit 1
fi
