UPDATE dim_products
SET product_price = TRIM(LEADING '£' FROM product_price);

ALTER TABLE dim_products
ADD weight_class VARCHAR(50);

UPDATE dim_products as dm
SET weight_class = (CASE WHEN dm.weight < 2 THEN 'Light'
                         WHEN dm.weight >= 2 AND dm.weight < 40 THEN 'Mid_Sized'
                         WHEN dm.weight >= 40 AND dm.weight < 140 THEN 'Heavy'
                         WHEN dm.weight >= 140 THEN 'Truck_Required'
				   END);

ALTER TABLE dim_products
RENAME removed TO still_available;

ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
    ALTER COLUMN weight TYPE FLOAT USING weight::double precision,
    ALTER COLUMN "EAN" TYPE VARCHAR(50),
    ALTER COLUMN product_code TYPE VARCHAR(50),
    ALTER COLUMN date_added TYPE DATE,
    ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
    ALTER COLUMN still_available TYPE BOOLEAN USING CASE WHEN still_available = 'Still_avaliable' THEN TRUE ELSE FALSE END,
    ALTER COLUMN weight_class TYPE VARCHAR(50);