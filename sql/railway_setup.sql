-- Run this in Railway PostgreSQL Admin/SQL editor

-- Create table
CREATE TABLE listings (
    id SERIAL PRIMARY KEY,
    title VARCHAR(500),
    description TEXT,
    price NUMERIC,
    currency VARCHAR(10),
    surface_m2 NUMERIC,
    listing_type VARCHAR(50),
    seller_name VARCHAR(200),
    seller_type VARCHAR(50),
    city VARCHAR(100),
    category VARCHAR(100),
    url TEXT,
    price_range VARCHAR(50),
    price_per_m2 NUMERIC
);

-- Insert your data (run this after creating the table)
-- You can use the Railway Admin SQL editor to INSERT your rows
