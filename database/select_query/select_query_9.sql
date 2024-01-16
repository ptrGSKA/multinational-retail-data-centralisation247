WITH cte AS (
		SELECT
			year,
			to_timestamp(year || '-' ||month || '-' || day || '-' || timestamp, 'YYYY-MM-DD-HH24-MI-SS') AS sales_date_time
		FROM
			dim_date_times
			), cte2 AS
					(SELECT 
					 		year,
					 		sales_date_time,
							LEAD(sales_date_time, 1) OVER(PARTITION BY year
													ORDER BY sales_date_time) AS diff
					FROM cte)
SELECT  year,
		AVG(diff - sales_date_time) AS actual_time_taken
FROM cte2
GROUP BY year
ORDER BY actual_time_taken DESC;