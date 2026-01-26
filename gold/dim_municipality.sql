-- Dimension: Municipality
-- This table holds information about each municipality.

CREATE TABLE IF NOT EXISTS dim_municipality (
    municipality_key    SERIAL PRIMARY KEY,
    municipality_code   VARCHAR(50) UNIQUE, -- The original business code
    name                VARCHAR(255) NOT NULL
);
