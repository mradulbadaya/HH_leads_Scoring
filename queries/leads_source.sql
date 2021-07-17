
select customer_id, lead_source,first_form_source
from (
SELECT
	a.lead_source,
	a.first_form_source,
	b.customer_id,
	ROW_NUMBER() over (partition by b.customer_id order by b.created_at desc) as rank
FROM
	sf_lead_spot_details a
	INNER JOIN sf_lead_spot_detail_phones b ON a.id = b.detail_id
WHERE
	customer_id IN(`customers_list`)
	)x where rank = 1 ;