-- Fact: Sales
-- This table records individual sales events.

CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key       BIGSERIAL PRIMARY KEY,
    date_key        INTEGER REFERENCES dim_date(date_key),
    store_key       INTEGER REFERENCES dim_store(store_key),
    product_key     INTEGER REFERENCES dim_product(product_key),
    sales_amount    NUMERIC(10, 2) NOT NULL,
    units_sold      INTEGER NOT NULL
);
