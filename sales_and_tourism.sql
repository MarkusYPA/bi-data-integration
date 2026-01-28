-- SQL query to retrieve monthly sales and tourism data for correlation analysis.

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
monthly_tourism AS (
    SELECT
        d.year,
        d.month_of_year,
        m.name AS municipality_name,
        SUM(ft.visitor_count) AS total_visitors
    FROM fact_tourism ft
    JOIN dim_date d ON ft.date_key = d.date_key
    JOIN dim_municipality m ON ft.municipality_key = m.municipality_key
    GROUP BY d.year, d.month_of_year, m.name
)
SELECT
    ms.year,
    ms.month_of_year,
    ms.municipality_name,
    ms.total_sales,
    mt.total_visitors
FROM monthly_sales ms
JOIN monthly_tourism mt
    ON ms.year = mt.year
    AND ms.month_of_year = mt.month_of_year
    AND ms.municipality_name = mt.municipality_name
ORDER BY
    ms.year,
    ms.month_of_year,
    ms.municipality_name;
