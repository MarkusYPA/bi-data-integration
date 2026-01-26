-- Dimension: Date
-- This table is a comprehensive date dimension.
-- The key is an integer in YYYYMMDD format for easy joins from business logic.

CREATE TABLE IF NOT EXISTS dim_date (
    date_key            INTEGER PRIMARY KEY, -- E.g., 20230115
    date                DATE NOT NULL,
    day_of_week         SMALLINT NOT NULL,
    day_of_month        SMALLINT NOT NULL,
    day_of_year         SMALLINT NOT NULL,
    month_of_year       SMALLINT NOT NULL,
    quarter_of_year     SMALLINT NOT NULL,
    year                SMALLINT NOT NULL,
    month_name          VARCHAR(20) NOT NULL,
    day_name            VARCHAR(20) NOT NULL,
    is_weekend          BOOLEAN NOT NULL
);
