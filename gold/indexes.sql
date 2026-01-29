-- Indexes for Foreign Keys (crucial for JOIN performance)
-- Postgres does not automatically index FKs, so we must do it manually.
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_store ON fact_sales(store_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_key);

CREATE INDEX IF NOT EXISTS idx_fact_tourism_date ON fact_tourism(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_tourism_municipality ON fact_tourism(municipality_key);

CREATE INDEX IF NOT EXISTS idx_fact_demographics_date ON fact_demographics(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_demographics_municipality ON fact_demographics(municipality_key);

CREATE INDEX IF NOT EXISTS idx_dim_store_municipality ON dim_store(municipality_key);

-- Indexes for Dimension Columns used in Filtering & Grouping
-- These speed up WHERE clauses and GROUP BY operations.

-- Product Category is used in Q5, Q10
CREATE INDEX IF NOT EXISTS idx_dim_product_category ON dim_product(category);

-- Municipality Name is used for grouping/display in almost all queries (Q3, Q5, Q6, Q7, Q8)
CREATE INDEX IF NOT EXISTS idx_dim_municipality_name ON dim_municipality(name);

-- Date fields are used in Q1, Q4, Q7, Q9, Q10
CREATE INDEX IF NOT EXISTS idx_dim_date_year ON dim_date(year);
CREATE INDEX IF NOT EXISTS idx_dim_date_month ON dim_date(month_of_year);
CREATE INDEX IF NOT EXISTS idx_dim_date_is_weekend ON dim_date(is_weekend);
