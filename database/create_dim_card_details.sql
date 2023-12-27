CREATE TABLE IF NOT EXISTS dim_card_details (
	index SERIAL UNIQUE NOT NULL PRIMARY KEY,
    card_number BIGINT UNIQUE NOT NULL,
    expiry_date DATE NOT NULL,
    card_provider VARCHAR(100) NOT NULL,
	date_payment_confirmed DATE NOT NULL
);