SELECT  ROUND(SUM(product_quantity * product_price)::NUMERIC,2) AS total_sales,
		store_type, country_code
FROM dim_products AS dp
JOIN orders_table AS ot
	ON dp.product_code = ot.product_code
JOIN dim_store_details AS dsd
	ON ot.store_code = dsd.store_code
WHERE country_code = 'DE'
GROUP BY store_type, country_code
ORDER BY ROUND(SUM(product_quantity * product_price)::NUMERIC,2);