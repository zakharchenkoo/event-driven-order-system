-- Payment Service Database Schema

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS payments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id        UUID NOT NULL UNIQUE,
    customer_id     UUID NOT NULL,
    amount          NUMERIC(19, 4) NOT NULL,
    currency        VARCHAR(3) NOT NULL,
    status          VARCHAR(20) NOT NULL,
    source_event_id UUID NOT NULL UNIQUE,  -- Idempotency key
    created_at      TIMESTAMP(0) WITH TIME ZONE,

    CONSTRAINT chk_payment_status CHECK (status IN ('SUCCESS', 'FAILED')),
    CONSTRAINT chk_payment_amount CHECK (amount > 0)
);

-- Idempotency index: prevents duplicate payment processing for the same event
CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_source_event_id ON payments(source_event_id);
CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id);
CREATE INDEX IF NOT EXISTS idx_payments_customer_id ON payments(customer_id);