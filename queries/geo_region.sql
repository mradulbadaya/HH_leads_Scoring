 SELECT concat(a.first_name,' ',a.last_name) as customer_name
,a.id as customer_id,
b.name as geo_region
from customers a
left join geo_regions b 
on a.geo_region_id = b.id
where a.id IN (`customers_list`)
;