SELECT COUNT(user_uuid) AS numbers_of_sales,
	   SUM(product_quantity) AS product_quantity_count,
		(
		CASE
			WHEN store_type = 'Web Portal' THEN 'Web'
			ELSE 'Offline'
		END) AS location
FROM dim_store_details AS dsd
JOIN orders_table AS ot
	ON dsd.store_code = ot.store_code
GROUP BY location
ORDER BY numbers_of_sales ASC;