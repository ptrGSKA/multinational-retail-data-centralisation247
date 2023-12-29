SELECT store_type,
	   SUM(product_quantity * product_price) AS total_sales,
	   COUNT(store_type) AS number_of_sales 
FROM dim_store_details AS dsd
JOIN orders_table AS ot
	ON dsd.store_code = ot.store_code
JOIN dim_products AS dp
	ON dp.product_code = ot.product_code
GROUP BY store_type
ORDER BY total_sales DESC;

-- NOt READY