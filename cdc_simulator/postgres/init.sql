-- Create dedicated schema for the Airbnb transactional source
CREATE SCHEMA IF NOT EXISTS airbnb;

-- 1. Listings table (tracked via SCD Type 2 in Silver to capture historical price changes)
CREATE TABLE airbnb.listings (
    listing_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    host_id INT NOT NULL,
    host_name VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    neighbourhood VARCHAR(100) NOT NULL,
    room_type VARCHAR(50) NOT NULL, -- Entire home/apt, Private room, Shared room
    price_per_night NUMERIC(10, 2) NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'USD',
    minimum_nights INT NOT NULL DEFAULT 1,
    listing_status VARCHAR(30) NOT NULL DEFAULT 'ACTIVE', -- ACTIVE, SNOOZED, UNLISTED
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Bookings table (high-frequency INSERT / UPDATE / DELETE CDC workload)
CREATE TABLE airbnb.bookings (
    booking_id SERIAL PRIMARY KEY,
    listing_id INT NOT NULL REFERENCES airbnb.listings(listing_id),
    guest_id INT NOT NULL,
    guest_country VARCHAR(50) NOT NULL,
    booking_status VARCHAR(30) NOT NULL, -- PENDING, CONFIRMED, COMPLETED, CANCELLED
    check_in_date DATE NOT NULL,
    check_out_date DATE NOT NULL,
    nights INT NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'USD',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Payments table (captures payouts, fees, refunds, and hard deletes on voided txns)
CREATE TABLE airbnb.payments (
    payment_id SERIAL PRIMARY KEY,
    booking_id INT NOT NULL REFERENCES airbnb.bookings(booking_id),
    payment_method VARCHAR(50) NOT NULL, -- CREDIT_CARD, APPLE_PAY, PAYPAL
    payment_status VARCHAR(30) NOT NULL, -- AUTHORIZED, CAPTURED, REFUNDED, FAILED
    amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'USD',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- CRITICAL FOR LOG-BASED CDC:
-- Enforce REPLICA IDENTITY FULL so PostgreSQL writes full 'before' and 'after' states into WAL
ALTER TABLE airbnb.listings REPLICA IDENTITY FULL;
ALTER TABLE airbnb.bookings REPLICA IDENTITY FULL;
ALTER TABLE airbnb.payments REPLICA IDENTITY FULL;

-- Initial seed data from real Inside Airbnb cities (New York, London, Amsterdam)
INSERT INTO airbnb.listings (listing_id, name, host_id, host_name, city, neighbourhood, room_type, price_per_night, currency, minimum_nights) VALUES
(2595, 'Skylit Midtown Castle Loft', 2845, 'Jennifer', 'New York', 'Midtown', 'Entire home/apt', 225.00, 'USD', 3),
(13913, 'Holiday London Studio Apartment', 54730, 'Alina', 'London', 'Islington', 'Entire home/apt', 110.00, 'GBP', 2),
(2818, 'Quiet Garden View Canal House', 3159, 'Daniel', 'Amsterdam', 'Oostelijk Havengebied', 'Private room', 145.00, 'EUR', 2);

INSERT INTO airbnb.bookings (listing_id, guest_id, guest_country, booking_status, check_in_date, check_out_date, nights, total_amount, currency) VALUES
(2595, 9001, 'United States', 'CONFIRMED', '2026-12-01', '2026-12-05', 4, 900.00, 'USD'),
(13913, 9002, 'Kazakhstan', 'PENDING', '2026-12-10', '2026-12-13', 3, 330.00, 'GBP'),
(2818, 9003, 'Germany', 'CONFIRMED', '2026-12-15', '2026-12-17', 2, 290.00, 'EUR');

INSERT INTO airbnb.payments (booking_id, payment_method, payment_status, amount, currency) VALUES
(1, 'CREDIT_CARD', 'CAPTURED', 900.00, 'USD'),
(2, 'APPLE_PAY', 'AUTHORIZED', 330.00, 'GBP'),
(3, 'PAYPAL', 'CAPTURED', 290.00, 'EUR');
