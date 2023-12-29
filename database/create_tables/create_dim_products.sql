CREATE TABLE IF NOT EXISTS dim_products (
	index SERIAL UNIQUE NOT NULL,
    product_name VARCHAR(250) NOT NULL,
    product_price VARCHAR(50) NOT NULL,
	weight FLOAT NOT NULL,
	category VARCHAR(250) NOT NULL,
    "EAN" VARCHAR(50) NOT NULL,
	date_added DATE NOT NULL CHECK (date_added < NOW()),
	uuid  UUID UNIQUE NOT NULL,
    removed VARCHAR(100) NOT NULL,
	product_code VARCHAR(50) UNIQUE NOT NULL
);