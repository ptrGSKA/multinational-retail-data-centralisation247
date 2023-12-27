ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(50),
    ALTER COLUMN staff_numbers TYPE SMALLINT,
    ALTER COLUMN opening_date TYPE DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
    ALTER COLUMN country_code TYPE VARCHAR(10),
    ALTER COLUMN continent TYPE VARCHAR(255);