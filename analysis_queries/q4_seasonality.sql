-- Q4: Seasonal patterns in tourism and grocery sales
WITH monthly_sales AS (
    SELECT
        d.month_of_year,
        d.month_name,
        SUM(fs.sales_amount) AS total_sales
    FROM fact_sales fs
    JOIN dim_date d ON fs.date_key = d.date_key
    GROUP BY d.month_of_year, d.month_name
),
monthly_tourism AS (
    SELECT
        d.month_of_year,
        SUM(ft.visitor_count) AS total_visitors,
        SUM(ft.revenue) AS total_tourism_revenue
    FROM fact_tourism ft
    JOIN dim_date d ON ft.date_key = d.date_key
    GROUP BY d.month_of_year
)
SELECT
    ms.month_name,
    ms.total_sales,
    COALESCE(mt.total_visitors, 0) AS total_visitors,
    COALESCE(mt.total_tourism_revenue, 0) AS total_tourism_revenue
FROM monthly_sales ms
LEFT JOIN monthly_tourism mt ON ms.month_of_year = mt.month_of_year
ORDER BY ms.month_of_year;
