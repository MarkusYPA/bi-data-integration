-- Q6: Top performing stores and factors (location, municipality)
SELECT
    s.name AS store_name,
    m.name AS municipality_name,
    s.address,
    SUM(fs.sales_amount) AS total_sales,
    SUM(fs.units_sold) AS total_units_sold
FROM fact_sales fs
JOIN dim_store s ON fs.store_key = s.store_key
JOIN dim_municipality m ON s.municipality_key = m.municipality_key
GROUP BY s.name, m.name, s.address
ORDER BY total_sales DESC;
