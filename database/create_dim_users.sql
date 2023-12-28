CREATE TABLE IF NOT EXISTS dim_users (
	index SERIAL UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
	date_of_birth DATE NOT NULL,
	company VARCHAR(250) NOT NULL,
    email_address VARCHAR(100) NOT NULL,
	address VARCHAR(250) NOT NULL,
	country VARCHAR(100) NOT NULL,
	country_code VARCHAR(6) NOT NULL,
	phone_number VARCHAR(100) NOT NULL,
    join_date DATE NOT NULL CHECK (join_date < NOW()),
    user_uuid UUID NOT NULL
);