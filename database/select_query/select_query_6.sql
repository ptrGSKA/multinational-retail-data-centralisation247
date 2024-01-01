SELECT  ROUND(SUM(product_quantity * product_price)::NUMERIC,2) AS total_sales,
		year,
		month
FROM dim_date_times AS ddt
JOIN orders_table AS ot
	ON ddt.date_uuid = ot.date_uuid
JOIN dim_products AS dp
 	ON dp.product_code = ot.product_code
GROUP BY year, month
ORDER BY total_sales DESC
LIMIT 10;