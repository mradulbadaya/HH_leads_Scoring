WITH leads_customer_type AS (
    SELECT
        lsdp.customer_id,
        customer_type
    FROM
        sf_lead_spot_details AS lsd
        INNER JOIN sf_lead_spot_detail_phones AS lsdp ON lsdp.detail_id = lsd.id
    WHERE
        customer_id IN (`customers_list`)
)
SELECT
    *
FROM
    leads_customer_type;