SELECT
    id AS subs_id,
     customer_id,
    to_timestamp(subscribed_at) AT TIME zone 'utc' AT TIME zone 'asia/kolkata' AS gold_subscribed_at
FROM
    porter_club_members
WHERE
    customer_id IN (`customers_list`)
;