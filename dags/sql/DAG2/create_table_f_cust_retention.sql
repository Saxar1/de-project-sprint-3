--drop table mart.f_customer_retention cascade;

create table IF NOT EXISTS mart.f_customer_retention (
    period_id bigint not null,
    item_id bigint not null,
    new_customers_count bigint not null,
    new_customers_revenue numeric(10,2) not null,
    returning_customers_count bigint not null,
    returning_customers_revenue numeric(10,2) not null,
    refunded_customer_count bigint not null, 
    customers_refunded numeric(10,2) not null,
    period_name text not null default 'weekly'
);