--- Checking maximum length of entries in various columns
SELECT MAX(LENGTH(store_code::TEXT)) AS max_length
FROM dim_store_details;

--- Check the EAN error
SELECT EAN FROM dim_products;

--- Check the data types in columns
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'dim_products';
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_products';

--- Task 1. Casting columns of the orders_table ---
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN card_number TYPE VARCHAR(22),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN product_quantity TYPE SMALLINT;

--- Task 2. Casting columns of the dim_users ---

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_users';

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN country_code TYPE VARCHAR(3),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN join_date TYPE DATE,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

--- Task 3. Casting columns of the dim_store_details ---
    --- Create a new column called `merged_latitude`
ALTER TABLE dim_store_details
ADD COLUMN merged_latitude TEXT NULL;

    --- Update the new column with the merged content of the `lat` and `latitude` columns
UPDATE dim_store_details
SET merged_latitude = CONCAT(lat, '', latitude);

    --- Drop the `lat` column
ALTER TABLE dim_store_details
DROP lat,
DROP COLUMN latitude;

    --- Rename the `merged_latitude` column to `latitude`
ALTER TABLE dim_store_details
RENAME COLUMN merged_latitude TO latitude;

    --- Cast the data types in dim_store_details

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(11),
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
ALTER COLUMN opening_date TYPE DATE,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255); 

--- Task 4. Work on dim_product table ---

    --- Remove the "£" from prices
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '')
WHERE product_price LIKE '£%';

    --- Add weight_class column
ALTER TABLE dim_products
ADD weight_class VARCHAR(14);

    --- Bin the weight_class values

UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    ELSE 'Truck_Required'
END; 

--- Task 5. sorting dim_products ---

    --- Renaming the removed column to still_available
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

    --- Sort out the error with "EAN column not present"

ALTER TABLE dim_products
RENAME COLUMN ean TO "EAN";

    --- Cast the data types

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
ALTER COLUMN weight TYPE FLOAT,
ALTER COLUMN ean TYPE VARCHAR(17),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
ALTER COLUMN weight_class TYPE VARCHAR(14); 

    --- Changing the still_available data type into boolean
ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOL
USING CASE
	WHEN still_available = 'Still_available' THEN true
	WHEN still_available = 'Removed' THEN false
	ELSE NULL
END; 

---Task 6. working on dim_date_times---
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(10),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID; 

---Task 7. dim_card_details ---
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(22),
ALTER COLUMN expiry_date TYPE VARCHAR(10),
ALTER COLUMN date_payment_confirmed TYPE DATE;

---Task 8. primary keys ---

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number); 

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid); 

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code); 

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code); 

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid); 

---Task 9. foreign keys ---

    --- Fixing error with differences between the tables
DELETE FROM orders_table
WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);

DELETE FROM orders_table
WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);

DELETE FROM orders_table
WHERE product_code NOT IN (SELECT product_code FROM dim_products);

DELETE FROM orders_table
WHERE user_uuid NOT IN (SELECT user_uuid FROM dim_users);

    --- Creating foreign keys

ALTER TABLE orders_table
ADD CONSTRAINT FK_orders_table_card_number
FOREIGN KEY (card_number)
REFERENCES dim_card_details(card_number),
ADD CONSTRAINT FK_orders_table_date_uuid
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times(date_uuid),
ADD CONSTRAINT FK_orders_table_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details(store_code),
ADD CONSTRAINT FK_orders_table_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products(product_code),
ADD CONSTRAINT FK_orders_table_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users(user_uuid);