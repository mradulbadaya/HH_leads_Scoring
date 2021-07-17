SELECT
	customer_id,CASE WHEN vehicle_type ILIKE '%Tata Ace%'
			OR vehicle_type ILIKE '%3 wheeler%'
			OR vehicle_type ILIKE '%8ft%'
			OR vehicle_type ILIKE '%407%'
			OR vehicle_type ILIKE '%eeco%' 
			OR vehicle_type ILIKE '%champion%' THEN
			'LCV'
		WHEN vehicle_type ILIKE '%2 wheeler%' THEN
			'2 wheeler'
		ELSE
			'Others'
		END AS vehicle_type,
		order_id
FROM (
	SELECT
		a.customer_id,
		a.order_id,
		a.vehicle_id,
		a.order_date,
		b.vehicle_type,
		ROW_NUMBER() OVER (PARTITION BY a.customer_id ORDER BY a.order_date ASC) AS rank
	FROM
		completed_spot_orders_mv a
	LEFT JOIN vehicles b ON a.vehicle_id = b.id
WHERE
	a.customer_id  IN (`customers_list`) )x
WHERE
	rank = 1;
 