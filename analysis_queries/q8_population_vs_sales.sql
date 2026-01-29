-- Q8: Relationship between population size and total grocery sales
WITH population_stats AS (
    -- Average population over the available years per municipality
    SELECT
        m.municipality_key,
        m.name AS municipality,
        AVG(yearly_pop) AS avg_population
    FROM (
        SELECT
            municipality_key,
            date_key,
            SUM(population_count) as yearly_pop
        FROM fact_demographics
        GROUP BY municipality_key, date_key
    ) sub
    JOIN dim_municipality m ON sub.municipality_key = m.municipality_key
    GROUP BY m.municipality_key, m.name
),
sales_stats AS (
    SELECT
        m.municipality_key,
        SUM(fs.sales_amount) AS total_sales
    FROM fact_sales fs
    JOIN dim_store s ON fs.store_key = s.store_key
    JOIN dim_municipality m ON s.municipality_key = m.municipality_key
    GROUP BY m.municipality_key
)
SELECT
    p.municipality,
    p.avg_population,
    COALESCE(s.total_sales, 0) AS total_sales
FROM population_stats p
LEFT JOIN sales_stats s ON p.municipality_key = s.municipality_key
ORDER BY p.avg_population DESC;
