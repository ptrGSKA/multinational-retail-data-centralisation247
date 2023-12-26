CREATE TABLE IF NOT EXISTS dim_products (
	index SERIAL UNIQUE NOT NULL PRIMARY KEY,
    product_name VARCHAR(250) NOT NULL,
    product_price(£) NUMERIC NOT NULL,
	weight(kg) NUMERIC NOT NULL,
	category VARCHAR(250) NOT NULL,
    EAN BIGINT NOT NULL,
	date_added DATE NOT NULL CHECK (date_added < NOW()),
	uuid  UUID UNIQUE NOT NULL,
    removed VARCHAR(100) NOT NULL,
	product_code VARCHAR(50) UNIQUE NOT NULL
);