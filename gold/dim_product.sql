-- Dimension: Product
-- This table contains details for each product.

CREATE TABLE IF NOT EXISTS dim_product (
    product_key         SERIAL PRIMARY KEY,
    product_id          INTEGER UNIQUE, -- The original product_id from the source
    name                VARCHAR(255) NOT NULL,
    category            VARCHAR(100),
    unit_price          NUMERIC(10, 2),
    unit_type           VARCHAR(50),
    supplier            VARCHAR(255)
);
