-- Create dedicated schema for the transactional e-commerce source
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- 1. Customers table (tracked via SCD Type 2 in the Silver layer)
CREATE TABLE ecommerce.customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    full_name VARCHAR(255) NOT NULL,
    tier VARCHAR(50) DEFAULT 'STANDARD', -- STANDARD, GOLD, VIP
    city VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Orders table (high-frequency INSERT / UPDATE / DELETE workload)
CREATE TABLE ecommerce.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES ecommerce.customers(customer_id),
    order_status VARCHAR(50) NOT NULL, -- CREATED, PAID, SHIPPED, CANCELLED
    total_amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'USD',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Payments table
CREATE TABLE ecommerce.payments (
    payment_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES ecommerce.orders(order_id),
    payment_method VARCHAR(50) NOT NULL, -- CARD, APPLE_PAY, PAYPAL
    payment_status VARCHAR(50) NOT NULL, -- PENDING, COMPLETED, FAILED, REFUNDED
    amount NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- CRITICAL FOR LOG-BASED CDC:
-- Enforce REPLICA IDENTITY FULL so PostgreSQL writes both 'before' and 'after'
-- row states into the Write-Ahead Log (WAL) on UPDATE and DELETE operations.
ALTER TABLE ecommerce.customers REPLICA IDENTITY FULL;
ALTER TABLE ecommerce.orders REPLICA IDENTITY FULL;
ALTER TABLE ecommerce.payments REPLICA IDENTITY FULL;

-- Initial seed data
INSERT INTO ecommerce.customers (email, full_name, tier, city) VALUES
('alice@example.com', 'Alice Johnson', 'STANDARD', 'New York'),
('bob@example.com', 'Bob Smith', 'GOLD', 'Chicago'),
('charlie@example.com', 'Charlie Davis', 'VIP', 'San Francisco');

INSERT INTO ecommerce.orders (customer_id, order_status, total_amount) VALUES
(1, 'CREATED', 120.50),
(2, 'PAID', 450.00),
(3, 'SHIPPED', 999.99);

INSERT INTO ecommerce.payments (order_id, payment_method, payment_status, amount) VALUES
(1, 'CARD', 'PENDING', 120.50),
(2, 'APPLE_PAY', 'COMPLETED', 450.00),
(3, 'PAYPAL', 'COMPLETED', 999.99);
