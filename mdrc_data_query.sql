--- Milestone 4. Task 1. ---
SELECT 
    country_code, 
    COUNT(store_code) AS store_count
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY 
    store_count DESC;

--- Milestone 4. Task 2. ---
SELECT 
    locality, 
        COUNT(store_code) AS store_count
FROM 
    dim_store_details
GROUP BY 
    locality
ORDER BY 
    store_count DESC
LIMIT 7;

--- Milestone 4. Task 3. ---
SELECT SUM(orders_table.product_quantity * dim_products.product_price) as total_sales, dim_date_times.month 
FROM dim_date_times 
JOIN orders_table ON dim_date_times.date_uuid = orders_table.date_uuid 
JOIN dim_products ON orders_table.product_code = dim_products.product_code 
GROUP BY dim_date_times.month 
ORDER BY total_sales DESC
LIMIT 6;

---Milestone 4. Task 3. ---
SELECT
    COUNT(orders_table.product_quantity) AS number_of_sales,
    SUM(orders_table.product_quantity) AS product_quantity_count,
    CASE
        WHEN dim_store_details.store_code LIKE 'WEB-1388012W' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM orders_table
INNER JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY location
ORDER BY number_of_sales;

---Milestone 4. Task4. ---
SELECT
    dim_store_details.store_type AS store_type,
	ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS decimal), 2) Total_sales,
	ROUND((CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS decimal) / 
		 CAST((SELECT SUM(orders_table.product_quantity * dim_products.product_price)
			  FROM orders_table
			  INNER JOIN dim_products
			  ON orders_table.product_code = dim_products.product_code) AS decimal) * 100), 2) AS percentag_total
FROM 
	dim_products
INNER JOIN
	orders_table ON dim_products.product_code = orders_table.product_code
INNER JOIN
	dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
	dim_store_details.store_type
ORDER BY
	total_sales DESC;
	
---Milestone 4. Task 5. ---
SELECT
    ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS decimal), 2) total_sales,
	dim_date_times.year,
	dim_date_times.month
FROM
    dim_products
INNER JOIN
    orders_table ON dim_products.product_code = orders_table.product_code
INNER JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    dim_date_times.year,
	dim_date_times.month
ORDER BY
    total_sales DESC
LIMIT 10;

---Milestine 4. Task 7. ---
	---Web treated as GB sales.
SELECT 
    COALESCE(country_code, 'GB') AS country_code,
    SUM(staff_numbers) AS total_staff_headcount
FROM 
    dim_store_details
GROUP BY 
    COALESCE(country_code, 'GB')
ORDER BY 
    total_staff_headcount DESC;

---Milestone 4. Task 8. ---
SELECT 
    dim_store_details.store_type AS store_type,
	ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS decimal), 2) AS total_sales
FROM
	orders_table
JOIN
	dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN 
	dim_products ON orders_table.product_code = dim_products.product_code
JOIN
	dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE
	dim_store_details.country_code = 'DE'
GROUP BY
	dim_store_details.store_type
ORDER BY
	total_sales ASC;

---Milestone 4. Task 8. ---
WITH cte AS(
    SELECT TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp),
    'YYYY-MM-DD HH24:MI:SS') as datetimes, year FROM dim_date_times
    ORDER BY datetimes DESC
), cte2 AS(
    SELECT 
        year, 
        datetimes, 
        LEAD(datetimes, 1) OVER (ORDER BY datetimes DESC) as time_difference 
        FROM cte
) SELECT year, AVG((datetimes - time_difference)) as actual_time_taken FROM cte2
GROUP BY year
ORDER BY actual_time_taken DESC
LIMIT 5;