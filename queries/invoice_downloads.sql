
SELECT
        customer_id,
        event_type,
       event_timestamp AT TIME zone 'utc' AT TIME zone 'asia/kolkata' AS invoice_timestamp
       FROM awsma.customer_app_events
WHERE event_type ='b_ods_mail_invoice'
AND customer_id IN (`customers_list`)
;