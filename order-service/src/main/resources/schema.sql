-- Order Service Database Schema
-- Run this to initialize the order_db database

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL,
    total_amount NUMERIC(19, 4) NOT NULL,
    currency    VARCHAR(3) NOT NULL,
    status      VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at  TIMESTAMP(0) WITH TIME ZONE,
    updated_at  TIMESTAMP(0) WITH TIME ZONE,

    CONSTRAINT chk_status CHECK (status IN ('PENDING', 'CONFIRMED', 'PAID', 'CANCELLED')),
    CONSTRAINT chk_amount  CHECK (total_amount > 0)
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

-- Outbox events table (Transactional Outbox Pattern)
CREATE TABLE IF NOT EXISTS outbox_events (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic          VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id   UUID NOT NULL,
    payload        TEXT NOT NULL,
    processed      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMP(0) WITH TIME ZONE,
    processed_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_processed ON outbox_events(processed) WHERE processed = FALSE;
CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON outbox_events(created_at);