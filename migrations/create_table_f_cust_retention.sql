--drop table mart.f_customer_retention cascade;

create table IF NOT EXISTS mart.f_customer_retention (
    period_id bigint not null,
    item_id bigint not null,
    new_customers_count bigint null,
    returning_customers_count bigint null,
    refunded_customer_count bigint null, 
    new_customers_revenue numeric(10,2) null,
    returning_customers_revenue numeric(10,2) null,
    customers_refunded bigint null,
    period_name text not null default 'weekly'
);