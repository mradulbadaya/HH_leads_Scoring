SELECT customer_id,
order_id,
       to_timestamp(trip_cancelled_time) AT TIME zone 'asia/kolkata' AS trip_cancelled_time,
       cancel_reason_id
FROM orders 
WHERE 
 customer_id IN (`customers_list`)
and
 status=5 AND 
deleted_at IS NULL
AND order_type=0
and 
cancel_reason_id in ('72','73')
;