SELECT store_type,
	   ROUND(SUM(product_quantity * product_price)::NUMERIC,2) AS total_sales,
	   ROUND(SUM(product_quantity * product_price)::NUMERIC / SUM(SUM(product_quantity * product_price)::NUMERIC) OVER() *100 ,2) AS "percentage_total(%)" 
FROM dim_store_details AS dsd
JOIN orders_table AS ot
	ON dsd.store_code = ot.store_code
JOIN dim_products AS dp
	ON dp.product_code = ot.product_code
GROUP BY store_type
ORDER BY total_sales DESC;