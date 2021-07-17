SELECT
	customer_id,device_model,device_platform_name
FROM (
	SELECT
		customer_id,
		device_model,
		device_platform_name,
		ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_timestamp DESC) AS record
	FROM
		awsma.customer_app_events
	WHERE
		customer_id IN(`customers_list`)
		ORDER BY
			event_timestamp DESC) A
			WHERE record = 1;


