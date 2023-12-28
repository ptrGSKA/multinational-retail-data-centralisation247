CREATE TABLE IF NOT EXISTS dim_card_details (
	index SERIAL UNIQUE NOT NULL,
    card_number BIGINT UNIQUE NOT NULL,
    expiry_date VARCHAR(50) NOT NULL,
    card_provider VARCHAR(100) NOT NULL,
	date_payment_confirmed DATE NOT NULL
);