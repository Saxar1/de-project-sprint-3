with
fsales as 
	(select 
	 	dc.date_id, item_id, customer_id, 
	 	city_id, quantity, payment_amount, status
	 from staging.user_order_log uol
	 left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
	 where uol.date_time::Date = '{{ds}}'),
tr as 
	(select 
	 	customer_id,
	 	sum(payment_amount) as total_revenue
	 from staging.user_order_log uol2
	 where status = 'shipped'
	 group by customer_id)
insert into mart.f_sales_new (date_id, item_id, customer_id, city_id, quantity, payment_amount, status, total_revenue)
select date_id, item_id, customer_id, 
		city_id, quantity, payment_amount, status, total_revenue
from fsales
left join tr using(customer_id)

-- with 
-- fsales as 
--     (select * 
--     from mart.f_sales fs2
-- 	left join staging.user_order_log uol
-- 	using(customer_id, item_id, city_id, quantity, payment_amount)
-- 	left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual),
-- revenue as
-- 	(select 
-- 	   customer_id,
-- 	   sum(payment_amount) as total_revenue
-- 	 from staging.user_order_log uol2
-- 	 where status = 'shipped' and uol2.date_time::Date = '{{ds}}'
-- 	 group by customer_id)
-- insert into mart.f_sales_new (date_id, item_id, customer_id, city_id, quantity, payment_amount, status, total_revenue)
-- select date_id, item_id, customer_id, 
-- 	   city_id, quantity, payment_amount, 
--        status, total_revenue
-- from fsales
-- left join revenue using(customer_id);

