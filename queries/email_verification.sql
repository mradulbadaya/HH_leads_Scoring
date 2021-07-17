

SELECT
	id AS customer_id,
	confirmed_at,
	verified_email
FROM
	customers
WHERE
	verified_email = TRUE
	AND id IN(`customers_list`);