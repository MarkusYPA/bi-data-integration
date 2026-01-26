-- Fact: Demographics
-- A snapshot table of population counts.

CREATE TABLE IF NOT EXISTS fact_demographics (
    demographics_key    BIGSERIAL PRIMARY KEY,
    date_key            INTEGER REFERENCES dim_date(date_key),
    municipality_key    INTEGER REFERENCES dim_municipality(municipality_key),
    age_group           VARCHAR(20),
    gender              VARCHAR(20),
    population_count    INTEGER NOT NULL
);
