
SELECT
	id AS customer_id,
	frequency,
	CASE WHEN frequency IN(0, 5) THEN
		'Personal'
	WHEN frequency IN(1, 2, 3, 4) THEN
		'Business'
	WHEN frequency = 6 THEN 'House Shifting'
	END AS customer_frequency
FROM
	customers
WHERE
	id IN (`customers_list`)
	;
