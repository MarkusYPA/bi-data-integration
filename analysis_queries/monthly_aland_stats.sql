-- SQL query to retrieve monthly aggregated sales per capita and tourism data for entire Åland,
-- averaged across all years, to analyze correlation between sales per capita and visitor counts.

WITH daily_aland_sales AS (
    SELECT
        fs.date_key,
        SUM(fs.sales_amount) AS total_daily_aland_sales
    FROM fact_sales fs
    GROUP BY fs.date_key
),
daily_aland_population AS (
    SELECT
        fd.date_key,
        SUM(fd.population_count) AS total_daily_aland_population
    FROM fact_demographics fd
    GROUP BY fd.date_key
),
daily_aland_visitors AS (
    SELECT
        ft.date_key,
        SUM(ft.visitor_count) AS total_daily_aland_visitors
    FROM fact_tourism ft
    GROUP BY ft.date_key
),
monthly_aland_stats_per_year AS (
    SELECT
        dd.year,
        dd.month_of_year,
        SUM(COALESCE(das.total_daily_aland_sales, 0)) AS total_monthly_aland_sales,
        SUM(COALESCE(dap.total_daily_aland_population, 0)) AS total_monthly_aland_population,
        SUM(COALESCE(dav.total_daily_aland_visitors, 0)) AS total_monthly_aland_visitors
    FROM dim_date dd
    LEFT JOIN daily_aland_sales das ON dd.date_key = das.date_key
    LEFT JOIN daily_aland_population dap ON dd.date_key = dap.date_key
    LEFT JOIN daily_aland_visitors dav ON dd.date_key = dav.date_key
    GROUP BY dd.year, dd.month_of_year
)
SELECT
    year,
    month_of_year,
    total_monthly_aland_sales,
    total_monthly_aland_visitors,
    total_monthly_aland_population,
    CASE
        WHEN total_monthly_aland_population > 0 THEN total_monthly_aland_sales / total_monthly_aland_population
        ELSE 0
    END AS monthly_sales_per_capita
FROM monthly_aland_stats_per_year
WHERE total_monthly_aland_population > 0 -- Exclude months with no population data
ORDER BY year, month_of_year;