-- Minimal but realistic e-commerce schema for the CDC source database.
--
-- Every table has an explicit primary key. That's not incidental: PeerDB's
-- Postgres connector uses logical replication, and without a primary key
-- (or REPLICA IDENTITY FULL) Postgres cannot include enough of the old row
-- in UPDATE/DELETE WAL records for a downstream consumer to identify which
-- row changed. REPLICA IDENTITY defaults to "use the primary key", so as
-- long as every CDC'd table has one, no extra configuration is needed here.

CREATE TABLE categories (
    category_id     SERIAL PRIMARY KEY,
    name             TEXT NOT NULL UNIQUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE customers (
    customer_id      SERIAL PRIMARY KEY,
    email            TEXT NOT NULL UNIQUE,
    first_name       TEXT NOT NULL,
    last_name        TEXT NOT NULL,
    country          TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id       SERIAL PRIMARY KEY,
    sku              TEXT NOT NULL UNIQUE,
    name             TEXT NOT NULL,
    category_id      INTEGER NOT NULL REFERENCES categories(category_id),
    price_cents      INTEGER NOT NULL CHECK (price_cents >= 0),
    description      TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id         SERIAL PRIMARY KEY,
    customer_id      INTEGER NOT NULL REFERENCES customers(customer_id),
    status           TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'paid', 'shipped', 'delivered', 'cancelled')),
    order_total_cents INTEGER NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_item_id    SERIAL PRIMARY KEY,
    order_id         INTEGER NOT NULL REFERENCES orders(order_id),
    product_id       INTEGER NOT NULL REFERENCES products(product_id),
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE payments (
    payment_id       SERIAL PRIMARY KEY,
    order_id         INTEGER NOT NULL REFERENCES orders(order_id),
    amount_cents     INTEGER NOT NULL CHECK (amount_cents >= 0),
    method           TEXT NOT NULL CHECK (method IN ('card', 'paypal', 'bank_transfer')),
    status           TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'succeeded', 'failed', 'refunded')),
    processed_at     TIMESTAMPTZ
);

CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_payments_order ON payments(order_id);

-- A dedicated publication naming the exact tables we want mirrored. This is
-- deliberate rather than `CREATE PUBLICATION ... FOR ALL TABLES`: an
-- explicit table list means adding a new table to this schema later does
-- NOT silently start streaming it, which is the behavior you want on a
-- production source DB.
CREATE PUBLICATION peerdb_pub FOR TABLE
    categories, customers, products, orders, order_items, payments;
