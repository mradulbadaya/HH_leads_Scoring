SELECT
    customer_id,
    created_at AT TIME zone 'utc' AT TIME zone 'asia/kolkata' AS recharged_at
FROM
    customer_account_infos
WHERE
    entry_for IN (3, 18, 26) 
    AND customer_id IN (`customers_list`) ;