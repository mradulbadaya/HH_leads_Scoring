SELECT customer_id,
       to_timestamp(trip_cancelled_time) AT TIME zone 'asia/kolkata' AS trip_cancelled_time
FROM orders 
WHERE 
customer_id IN (`customers_list`)
AND 
status=5 AND 
deleted_at IS NULL
AND order_type=0
;
