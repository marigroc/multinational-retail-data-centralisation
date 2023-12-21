--- Checking maximum length of entries in various columns
SELECT MAX(LENGTH(card_number::TEXT)) AS max_length
FROM dim_card_details;

--- Check the EAN error
SELECT EAN FROM dim_products;

--- Check the data types in columns
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_store_details';
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_products';

--- Task 1. Casting columns of the orders_table ---
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(12),
ALTER COLUMN product_quantity TYPE SMALLINT;

--- Task 2. Casting columns of the dim_users ---

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_users';

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
ALTER COLUMN join_date TYPE DATE USING join_date::DATE,
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
    ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(12),
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
    RENAME COLUMN "EAN" TO ean;
    ALTER TABLE dim_products
    RENAME COLUMN ean TO "EAN";

    --- Cast the data types

    ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
    ALTER COLUMN weight TYPE FLOAT,
    ALTER COLUMN ean TYPE VARCHAR(17),
    ALTER COLUMN product_code TYPE VARCHAR(12),
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
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_card_details';
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
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
        --- Some cards werre in orders_table and not in dim_card_details ---
        SELECT DISTINCT card_number
        FROM orders_table
        WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);
        
        INSERT INTO dim_card_details (card_number, date_payment_confirmed, expiry_date, card_provider)
        SELECT card_number, NULL, NULL, NULL
        FROM (
            SELECT DISTINCT card_number
            FROM orders_table
            WHERE card_number NOT IN (SELECT card_number FROM dim_card_details)
        ) AS missing_cards;

        
        --- WEB-XXXXXXXX in orders_details not in dim_store_details ---
        SELECT DISTINCT store_code
        FROM orders_table
        WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);
        
        INSERT INTO dim_store_details (store_code, latitude, longitude, staff_numbers, opening_date, country_code, continent, address, store_type, locality, index, level_0)
        SELECT store_code, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        FROM (
            SELECT DISTINCT store_code
            FROM orders_table
            WHERE store_code NOT IN (SELECT store_code FROM dim_store_details)
        ) AS WEB;
	
        --- product_code in orders_details and not in dim_products ---
        SELECT DISTINCT product_code
        FROM orders_table
        WHERE product_code NOT IN (SELECT product_code FROM dim_products);
        INSERT INTO dim_products (product_code)
        SELECT UPPER(product_code)
        FROM dim_products
        WHERE product_code ~ '[a-z]';
        
        INSERT INTO dim_store_details (store_code, latitude, longitude, staff_numbers, opening_date, country_code, continent, address, store_type, locality, index, level_0)
        SELECT store_code, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        FROM (
            SELECT DISTINCT store_code
            FROM orders_table
            WHERE store_code NOT IN (SELECT store_code FROM dim_store_details)
        ) AS WEB;

    --- Creating foreign keys
    ALTER TABLE orders_table
    ADD FOREIGN KEY (date_uuid) 
    REFERENCES dim_date_times(date_uuid);

    ALTER TABLE orders_table 
    ADD FOREIGN KEY (user_uuid) 
    REFERENCES dim_users(user_uuid);

    ALTER TABLE orders_table 
    ADD FOREIGN KEY (card_number) 
    REFERENCES dim_card_details (card_number);

    ALTER TABLE orders_table 
    ADD FOREIGN KEY (store_code) 
    REFERENCES dim_store_details(store_code);

    ALTER TABLE orders_table 
    ADD FOREIGN KEY (product_code) 
    REFERENCES dim_products(product_code);