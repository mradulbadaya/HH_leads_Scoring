


select customer_id,driver_rating
from (
SELECT customer_id, driver_rating,ROW_NUMBER() over (partition by customer_id order by pickup_time asc) as rank
from orders
where status = 4
and customer_id IN(`customers_list`)
)x 
where rank = 1;