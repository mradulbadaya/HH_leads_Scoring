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
        ) AS latest_sync
    FROM
        alfred.user_application_logs
    WHERE
        application_name IN ('Packers & Movers by NoBroker', 'LYNK')
        AND 
        category_name = 'competitor'
        And user_type_id LIKE ('customer-%')
        AND RIGHT(user_type_id, len(user_type_id) -9) :: INT IN (`customers_list`)
)
SELECT
    customer_id,
    application_name as competitor_app,
   is_installed
FROM
    tracking_data
WHERE
    latest_sync = 1 ;