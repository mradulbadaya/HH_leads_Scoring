select a_device_id,customer_id
from (
SELECT
	a_device_id,
	customer_id,
	ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_timestamp DESC) AS record
FROM
	awsma.customer_app_events
WHERE
	customer_id IN(`customers_list`)
	)x 
	where record = 1
;
