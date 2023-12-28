CREATE TABLE IF NOT EXISTS dim_store_details (
	index SERIAL UNIQUE NOT NULL,
    address VARCHAR(250) NOT NULL,
    longitude VARCHAR(50),
	locality VARCHAR(150) NOT NULL,
	store_code VARCHAR(250) UNIQUE NOT NULL,
    staff_numbers INTEGER NOT NULL,
	opening_date DATE NOT NULL CHECK (opening_date < NOW()),
	store_type  VARCHAR(100) NOT NULL,
    latitude VARCHAR(50),
	country_code VARCHAR(6) NOT NULL,
	continent VARCHAR(100) NOT NULL
);