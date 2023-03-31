ALTER TABLE staging.user_order_log ADD COLUMN IF NOT EXISTS status varchar(15) NOT NULL Default 'shipped';
ALTER TABLE mart.f_sales ADD COLUMN IF NOT EXISTS total_revenue numeric(10,2) null; 
ALTER TABLE mart.f_sales ADD COLUMN IF NOT EXISTS status varchar(15) NOT NULL Default 'shipped';