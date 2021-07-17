


WITH installation_details AS (
    SELECT
        DISTINCT ON (mobile) mobile AS phone_number,
        (request_ts AT TIME zone 'utc') AT TIME zone 'asia/kolkata' as app_install_time,
        extract(
            HOUR
            FROM
                (request_ts AT TIME zone 'utc') AT TIME zone 'asia/kolkata'
        ) AS app_download_time
    FROM
        sf_customer_signup_requests
    WHERE
        mobile IN (`phone_number_list`)
),
download_hour_detail AS (
    SELECT
        phone_number,
        app_install_time, 
        CASE
            WHEN app_download_time >= 9
            AND app_download_time < 21 THEN 'downloaded_working_hours'
            ELSE 'downloaded_non_working_hours'
        END AS app_download_time_of_day
    FROM
        installation_details
)
SELECT
    *
FROM
    download_hour_detail
    ;