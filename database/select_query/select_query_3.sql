SELECT month, SUM(product_quantity * product_price) AS total_sales
FROM dim_date_times AS ddt
JOIN orders_table AS ot
	ON ddt.date_uuid = ot.date_uuid
JOIN dim_products AS dp
 	ON dp.product_code = ot.product_code
GROUP BY month
ORDER BY total_sales DESC;