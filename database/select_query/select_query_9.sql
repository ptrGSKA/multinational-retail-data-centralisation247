 SELECT year,
		month,
		day,
		timestamp,
		LEAD((year, month, day, timestamp),1) OVER( ORDER BY year) AS actual_time_taken
FROM dim_date_times AS ddt
JOIN orders_table AS ot
	ON ddt.date_uuid = ot.date_uuid
ORDER BY year, month, day,timestamp;

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