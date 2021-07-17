SELECT customer_id,
         is_delayed
FROM orders 
WHERE 
customer_id IN (`customers_list`)
and
 order_id in (`orders_list`)
;