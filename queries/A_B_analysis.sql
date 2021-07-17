select a.lead_spot_id, timezone('Asia/Kolkata'::text,
			to_timestamp(a.first_order_time::double precision)) AS first_order_date,
		timezone('Asia/Kolkata'::text,
			to_timestamp(a.second_order_time::double precision)) AS second_order_date,
		timezone('Asia/Kolkata'::text,
			to_timestamp(a.third_order_time::double precision)) AS third_order_date,
			 b.customer_id,
			 b."number"
			 from sf_lead_spot_details a
join	sf_lead_spot_detail_phones  b 
on a.id = b.detail_id
where number IN (`phone_number_list`);