-- Q10: Product category sales correlation with tourism seasons
WITH monthly_category_sales AS (
    SELECT
        d.month_of_year,
        p.category,
        SUM(fs.sales_amount) AS category_sales
    FROM fact_sales fs
    JOIN dim_date d ON fs.date_key = d.date_key
    JOIN dim_product p ON fs.product_key = p.product_key
    GROUP BY d.month_of_year, p.category
),
monthly_tourism_total AS (
    SELECT
        d.month_of_year,
        SUM(ft.visitor_count) AS total_visitors
    FROM fact_tourism ft
    JOIN dim_date d ON ft.date_key = d.date_key
    GROUP BY d.month_of_year
)
SELECT
    dcs.category,
    dcs.month_of_year,
    dcs.category_sales,
    COALESCE(t.total_visitors, 0) AS total_visitors
FROM monthly_category_sales dcs
LEFT JOIN monthly_tourism_total t ON dcs.month_of_year = t.month_of_year
ORDER BY dcs.category, dcs.month_of_year;
