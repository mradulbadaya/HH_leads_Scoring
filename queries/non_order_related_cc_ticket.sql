
explain
SELECT case_cc_id,customer_partner_phone,crn,created_at,order_stage_v2,issue_v2
from sf_case_cc_details
where customer_partner_phone in ('6005454534')
and
crn = 'CRN00000000'
and 
id is not null and 
case_cc_id is not null and
order_stage_v2  = 'Non Order Related'
;