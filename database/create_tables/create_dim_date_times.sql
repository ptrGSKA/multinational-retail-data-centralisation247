CREATE TABLE IF NOT EXISTS dim_date_times (
	index SERIAL UNIQUE NOT NULL,
    timestamp TIME NOT NULL,
    month SMALLINT NOT NULL,
	year SMALLINT NOT NULL,
	day SMALLINT NOT NULL,
    time_period VARCHAR(50) NOT NULL,
	date_uuid UUID UNIQUE NOT NULL
);