-- Q7: Tourism revenue change over time by municipality
SELECT
    d.year,
    d.month_name,
    m.name AS municipality_name,
    SUM(ft.revenue) AS total_tourism_revenue,
    SUM(ft.visitor_count) AS total_visitors
FROM fact_tourism ft
JOIN dim_date d ON ft.date_key = d.date_key
JOIN dim_municipality m ON ft.municipality_key = m.municipality_key
GROUP BY d.year, d.month_of_year, d.month_name, m.name
ORDER BY m.name, d.year, d.month_of_year;
