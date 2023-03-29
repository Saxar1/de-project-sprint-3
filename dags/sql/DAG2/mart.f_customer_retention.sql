with 
customers as 
    (select * 
    from mart.f_sales_new 
    join mart.d_calendar on f_sales_new.date_id = d_calendar.date_id
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)), 
new_customers as 
    (select customer_id
    from customers where status = 'shipped' 
    group by customer_id having count(*) = 1), 
returning_customers as 
    (select customer_id
    from customers 
    where status = 'shipped' 
    group by customer_id having count(*) > 1), 
refunded_customers as 
    (select customer_id
    from customers 
    where status = 'refunded' 
	group by customer_id),
new_customers_agregate as 
	(select
	 	c.week_of_month as period_id,
	 	c.item_id,
	 	count(nc.customer_id) as new_customers_count,
	 	sum(c.payment_amount) as new_customers_revenue
	from new_customers as nc
	join customers as c using(customer_id)
	group by c.week_of_month, c.item_id),
returning_customers_agregate as 
	(select
	 	c.week_of_month as period_id,
	 	c.item_id,
	 	count(retc.customer_id) as returning_customers_count,
	 	sum(c.payment_amount) as returning_customers_revenue
	from returning_customers as retc
	join customers as c using(customer_id)
	group by c.week_of_month, c.item_id),
refunded_customers_agregate as 
	(select
	 	c.week_of_month as period_id,
	 	c.item_id,
	 	count(refc.customer_id) as refunded_customers_count,
	 	sum(c.payment_amount) as customers_refunded
	from refunded_customers as refc
	join customers as c using(customer_id)
	group by c.week_of_month, c.item_id),
period_item as
	(select 
	 	week_of_month as period_id,
	 	item_id
	 from customers)
insert into mart.f_customer_retention 
select *
from new_customers_agregate
left join returning_customers_agregate using(period_id, item_id)
left join refunded_customers_agregate using(period_id, item_id);



 