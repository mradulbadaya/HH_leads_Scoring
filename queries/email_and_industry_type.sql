SELECT
        DISTINCT ON (c.id) c.id AS customer_id,
        email,
        business_type_description AS industry_type,
        concat(c.first_name, ' ', c.last_name) AS customers_name

FROM
        customers AS c
        LEFT JOIN orders AS o ON o.customer_id = c.id
WHERE
        c.id IN (`customers_list`)
        AND o.order_type = 0
        AND o.status = 4
        AND o.deleted_at IS NULL
ORDER BY
        c.id,
        pickup_time ASC;