-- Q3: Municipalities with highest sales per capita and tourism relation
WITH sales_summary AS (
    SELECT
        m.municipality_key,
        m.name AS municipality_name,
        SUM(fs.sales_amount) AS total_sales
    FROM fact_sales fs
    JOIN dim_store s ON fs.store_key = s.store_key
    JOIN dim_municipality m ON s.municipality_key = m.municipality_key
    GROUP BY m.municipality_key, m.name
),
population_summary AS (
    -- Use the latest available year for population to calculate "current" per capita
    SELECT
        m.municipality_key,
        SUM(fd.population_count) AS total_population
    FROM fact_demographics fd
    JOIN dim_municipality m ON fd.municipality_key = m.municipality_key
    JOIN dim_date d ON fd.date_key = d.date_key
    WHERE d.year = (SELECT MAX(year) FROM dim_date WHERE date_key IN (SELECT date_key FROM fact_demographics))
    GROUP BY m.municipality_key
),
tourism_summary AS (
    SELECT
        m.municipality_key,
        SUM(ft.visitor_count) AS total_visitors,
        SUM(ft.revenue) AS total_tourism_revenue
    FROM fact_tourism ft
    JOIN dim_municipality m ON ft.municipality_key = m.municipality_key
    GROUP BY m.municipality_key
)
SELECT
    s.municipality_name,
    s.total_sales,
    p.total_population,
    (s.total_sales / NULLIF(p.total_population, 0)) AS sales_per_capita,
    COALESCE(t.total_visitors, 0) AS total_visitors,
    COALESCE(t.total_tourism_revenue, 0) AS total_tourism_revenue
FROM sales_summary s
LEFT JOIN population_summary p ON s.municipality_key = p.municipality_key
LEFT JOIN tourism_summary t ON s.municipality_key = t.municipality_key
ORDER BY sales_per_capita DESC;
