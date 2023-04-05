delete from mart.f_customer_retention 
where period_id = DATE_PART('week', '{{ds}}'::DATE);
with 
customers as 
    (select * 
    from mart.f_sales 
    join mart.d_calendar on f_sales.date_id = d_calendar.date_id
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
nc_revenue as 
	(select customer_id, payment_amount
	from customers
	right join new_customers using(customer_id)),
retc_revenue as
	(select customer_id, payment_amount
	from customers
	right join returning_customers using(customer_id))
insert into mart.f_customer_retention
select 
	c.week_of_year as period_id,
	c.item_id,
	count(nc.customer_id) as new_customers_count,
	count(retc.customer_id) as returning_customers_count,
	count(refc.customer_id) as refunded_customers_count,
	sum(ncr.payment_amount) as new_customers_revenue,
	sum(retcr.payment_amount) as returning_customers_revenue,
	count(refc.customer_id) as customers_refunded
from customers as c
left join new_customers as nc using(customer_id)
left join returning_customers as retc using(customer_id)
left join refunded_customers as refc using(customer_id)
left join nc_revenue as ncr using(customer_id)
left join retc_revenue as retcr using(customer_id)
group by item_id, week_of_year