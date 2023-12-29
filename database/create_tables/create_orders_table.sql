CREATE TABLE IF NOT EXISTS orders_table (
	index SERIAL UNIQUE NOT NULL,
    date_uuid UUID UNIQUE NOT NULL,
    user_uuid UUID NOT NULL,
	card_number BIGINT NOT NULL,
	store_code VARCHAR(100) NOT NULL,
    product_code VARCHAR(100) NOT NULL,
	product_quantity SMALLINT NOT NULL
);