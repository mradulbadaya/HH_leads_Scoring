WITH 
hh_leads AS (
	SELECT
		a.id,
		((a.sf_created_at AT TIME zone 'utc') AT TIME zone 'asia/kolkata') AS sf_created_at,
		b.lead_spot_id,
		timezone('Asia/Kolkata'::text,
			to_timestamp(b.first_order_time::double precision)) AS first_order_date,
		timezone('Asia/Kolkata'::text,
			to_timestamp(b.second_order_time::double precision)) AS second_order_date,
		timezone('Asia/Kolkata'::text,
			to_timestamp(b.third_order_time::double precision)) AS third_order_date,
		c.customer_id,
		c. "number" AS phone_number
	FROM
		sf_leads a
		INNER JOIN sf_lead_spot_details b ON a.id = b.lead_spot_id
		INNER JOIN sf_lead_spot_detail_phones c ON b.id = c.detail_id
	WHERE (a.record_type = 'Spot'
		OR(a.salesforce_id LIKE 'a0R%'
			AND a.record_type IS NULL))
	AND number in (`phone_number_list`)
),
flag AS (
	SELECT
		*,
		CASE WHEN ((second_order_date IS NULL
				OR third_order_date IS NULL)
			and(CURRENT_DATE - date(first_order_date)) < 30) THEN
			0
		ELSE
			1
		END AS "30_days_flag"
	FROM
		hh_leads
)
SELECT
	*
FROM
	flag
	--WHERE "30_days_flag" = 1 
	;


