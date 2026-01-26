-- Dimension: Store
-- This table holds information about each store location.

CREATE TABLE IF NOT EXISTS dim_store (
    store_key           SERIAL PRIMARY KEY,
    store_id            INTEGER UNIQUE, -- The original store_id from the source
    name                VARCHAR(255) NOT NULL,
    address             VARCHAR(255),
    municipality_key    INTEGER REFERENCES dim_municipality(municipality_key)
);
