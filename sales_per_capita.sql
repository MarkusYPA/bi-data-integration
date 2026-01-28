-- SQL query to calculate sales per capita over time.
-- This query joins sales data with population data and groups by municipality and month.

WITH monthly_sales AS (
    SELECT
        d.year,
        d.month_of_year,
        m.name AS municipality_name,
        SUM(fs.sales_amount) AS total_sales
    FROM fact_sales fs
    JOIN dim_date d ON fs.date_key = d.date_key
    JOIN dim_store s ON fs.store_key = s.store_key
    JOIN dim_municipality m ON s.municipality_key = m.municipality_key
    GROUP BY d.year, d.month_of_year, m.name
),
monthly_population AS (
    SELECT
        d.year,
        d.month_of_year,
        m.name AS municipality_name,
        SUM(fd.population_count) AS total_population
    FROM fact_demographics fd
    JOIN dim_date d ON fd.date_key = d.date_key
    JOIN dim_municipality m ON fd.municipality_key = m.municipality_key
    GROUP BY d.year, d.month_of_year, m.name
)
SELECT
    ms.year,
    ms.month_of_year,
    ms.municipality_name,
    ms.total_sales,
    mp.total_population,
    CASE
        WHEN mp.total_population > 0 THEN ms.total_sales / mp.total_population
        ELSE 0
    END AS sales_per_capita
FROM monthly_sales ms
JOIN monthly_population mp
    ON ms.year = mp.year
    AND ms.month_of_year = mp.month_of_year
    AND ms.municipality_name = mp.municipality_name
ORDER BY
    ms.year,
    ms.month_of_year,
    ms.municipality_name;
