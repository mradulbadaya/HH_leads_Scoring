SELECT customer_partner_phone,crn,order_stage_v2,issue_v2
from sf_case_cc_details
where customer_partner_phone in (`phone_number_list`)
and crn in (`order_list`)
and order_stage_v2 not in ('Non Order Related')
;