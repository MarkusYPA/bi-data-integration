-- Q5: Product category performance across municipalities
SELECT
    m.name AS municipality_name,
    p.category,
    SUM(fs.sales_amount) AS total_sales,
    SUM(fs.units_sold) AS total_units
FROM fact_sales fs
JOIN dim_product p ON fs.product_key = p.product_key
JOIN dim_store s ON fs.store_key = s.store_key
JOIN dim_municipality m ON s.municipality_key = m.municipality_key
GROUP BY m.name, p.category
ORDER BY m.name, total_sales DESC;
