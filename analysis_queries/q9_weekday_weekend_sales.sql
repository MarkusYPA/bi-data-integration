-- Q9: Sales patterns between weekdays and weekends
SELECT
    d.is_weekend,
    d.day_name,
    AVG(fs.sales_amount) AS avg_daily_sales,
    SUM(fs.sales_amount) AS total_sales
FROM fact_sales fs
JOIN dim_date d ON fs.date_key = d.date_key
GROUP BY d.is_weekend, d.day_of_week, d.day_name
ORDER BY d.is_weekend, d.day_of_week;
