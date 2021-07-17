SELECT
	a_device_id,
	count(distinct customer_id) as distinct_customer_id
	FROM
	awsma.customer_app_events
	where a_device_id in (`IMEI_list`)
group by  a_device_id
	;