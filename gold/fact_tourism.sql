-- Fact: Tourism
-- Records tourism-related metrics.

CREATE TABLE IF NOT EXISTS fact_tourism (
    tourism_key         BIGSERIAL PRIMARY KEY,
    date_key            INTEGER REFERENCES dim_date(date_key),
    municipality_key    INTEGER REFERENCES dim_municipality(municipality_key),
    accommodation_type  VARCHAR(50),
    origin_country      VARCHAR(100),
    visitor_count       INTEGER NOT NULL,
    revenue             NUMERIC(12, 2)
);
