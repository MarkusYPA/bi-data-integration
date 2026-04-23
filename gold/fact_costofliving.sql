-- Fact: Cost of Living
-- Records cost of living index metrics.

CREATE TABLE IF NOT EXISTS fact_costofliving (
    costofliving_key    BIGSERIAL PRIMARY KEY,
    date_key            INTEGER REFERENCES dim_date(date_key),
    index_value         NUMERIC(12, 2)
);
