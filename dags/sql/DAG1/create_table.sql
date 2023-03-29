-- Drop table

-- DROP TABLE mart.f_sales_new;

CREATE TABLE IF NOT EXISTS mart.f_sales_new (
	id serial4 NOT NULL,
	date_id int4 NOT NULL,
	item_id int4 NOT NULL,
	customer_id int4 NOT NULL,
	city_id int4 NOT NULL,
	quantity int8 NULL,
	payment_amount numeric(10, 2) NULL,
    status varchar(15) NOT NULL Default 'shipped',
	total_revenue numeric(10,2) NULL,
	CONSTRAINT f_sales_new_pkey PRIMARY KEY (id),
	CONSTRAINT f_sales_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id),
	CONSTRAINT f_sales_date_id_fkey FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id),
	CONSTRAINT f_sales_item_id_fkey FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id),
	CONSTRAINT f_sales_item_id_fkey1 FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id)
);
CREATE INDEX IF NOT EXISTS f_ds_n1 ON mart.f_sales_new USING btree (date_id);
CREATE INDEX IF NOT EXISTS f_ds_n2 ON mart.f_sales_new USING btree (item_id);
CREATE INDEX IF NOT EXISTS f_ds_n3 ON mart.f_sales_new USING btree (customer_id);
CREATE INDEX IF NOT EXISTS f_ds_n4 ON mart.f_sales_new USING btree (city_id);
