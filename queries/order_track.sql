SELECT
    customer_id,
    a_app_session_id,
    event_timestamp AT TIME zone 'utc' AT TIME zone 'asia/kolkata' AS event_timestamp
FROM
    awsma.customer_app_events
WHERE
    a_screen_name IN ('s_track_live_screen')
    AND customer_id IN (`customers_list`)
    AND event_type = 'b_tls_call_partner'
;