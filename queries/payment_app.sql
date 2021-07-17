WITH tracking_data AS (
    SELECT
        user_type_id,
        RIGHT(user_type_id, len(user_type_id) -9) :: INT AS customer_id,
        application_name,
        is_installed ,
        sync_ts AT TIME zone 'utc' AT TIME zone 'asia/kolkata' AS sync_ts,
        RANK() OVER (
            PARTITION BY user_type_id
            ORDER BY
                SYNC_TS DESC
        ) AS test_num
    FROM
        alfred.user_application_logs
    WHERE
        category_name = 'payment'
        AND  user_type_id LIKE ('customer-%')
        AND RIGHT(user_type_id, len(user_type_id) -9) :: INT IN (`customers_list`)
)
SELECT
    customer_id,
    application_name as payment_app,
    is_installed
FROM
    tracking_data
WHERE
    test_num = 1
    ;