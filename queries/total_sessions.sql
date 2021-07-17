WITH bookings AS (
    SELECT
        DISTINCT customer_id,
        a_booking_id,
        MIN(
            event_timestamp AT TIME zone 'utc' AT TIME zone 'asia/kolkata'
        ) AS booking_start_time,
        MAX(
            event_timestamp AT TIME zone 'utc' AT TIME zone 'asia/kolkata'
        ) AS booking_end_time
    FROM
        awsma.customer_app_events
    WHERE
        customer_id IN (`customers_list`)
        AND a_booking_id IS NOT NULL
        AND a_app_session_id IS NOT NULL
        AND a_booking_id NOT IN ('null')
    GROUP BY
        1,
        2
)
SELECT
    *,
    extract(
        MINUTE
        FROM
            (booking_end_time - booking_start_time)
    ) AS TIME_spent
    FROM
    bookings;
