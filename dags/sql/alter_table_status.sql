ALTER TABLE staging.user_order_log ADD COLUMN IF NOT EXISTS status varchar(15) NOT NULL Default 'shipped';