CREATE TABLE IF NOT EXISTS orders_table (
	index SERIAL UNIQUE NOT NULL PRIMARY KEY,
    date_uuid UUID UNIQUE NOT NULL,
    user_uuid UUID UNIQUE NOT NULL,
	card_number BIGINT UNIQUE NOT NULL,
	store_code VARCHAR(100) UNIQUE NOT NULL,
    product_code VARCHAR(100) UNIQUE NOT NULL,
	product_quantity SMALLINT NOT NULL
);