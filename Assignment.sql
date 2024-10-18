/*
Write a t-SQL script that returns the following data for each CustomerID. It
can be one or more queries but the final result should return the following:

REPORT_RESULTS
CustomerID
OrderID of the First Order
OrderDate of the First Order
First Order Amount
OrderID of the Last Order
OrderDate of the Last Order
Order Amount of the Last Order
Total Order Amount
Average OrderAmount
Total Item Count
Average Item Count
Avg Item Amount
Most Commonly Purchased Item
Average Days to Ship

From this website: https://mavenanalytics.io/data-playground?order=date_added%2Cdesc&search=Global%20Electronics%20Retailer

downloaded dummy data and completed the following steps to accomplish the above tasks:

https://maven-datasets.s3.amazonaws.com/Global+Electronics+Retailer/Global+Electronics+Retailer.zip

*/
--CREATE SCHEMA sample_retail;

CREATE TABLE sample_retail.customers (
    customer_key INT NOT NULL,
    gender CHAR(6),
    customer_name VARCHAR(100),
    city VARCHAR(50),
    state_code CHAR(30),
    customer_state CHAR(30),
    zip_code CHAR(10),
    country VARCHAR(50),
    continent VARCHAR(50),
    birthday DATE
);
ALTER TABLE sample_retail.customers
ADD PRIMARY KEY (customer_key);

EXEC sys.sp_addextendedproperty 
    @name=N'TableDescription', 
    @value=N'Main customer information table.',
    @level0type=N'SCHEMA',
    @level0name=N'sample_retail', 
    @level1type=N'TABLE',
    @level1name=N'customers';

EXEC sys.sp_addextendedproperty 
    @name=N'SourceDescription', 
    @value=N'Data sourced from store location or website input.',
    @level0type=N'SCHEMA',
    @level0name=N'sample_retail', 
    @level1type=N'TABLE',
    @level1name=N'customers';

EXECUTE sp_addextendedproperty 
    @name = 'ColumnDescription', 
    @value = 'Primary key to identify customers', 
    @level0type = 'SCHEMA', 
    @level0name= N'sample_retail', 
    @level1type = N'TABLE', 
    @level1name = N'customers', 
    @level2type = N'COLUMN', 
    @level2name = N'customer_key';

-- the column descriptions can be added for the rest of the columns.

CREATE TABLE sample_retail.products(
	product_key nvarchar(50) NOT NULL,
	product_name nvarchar(100) NOT NULL,
	brand nvarchar(50) NOT NULL,
	color nvarchar(50) NOT NULL,
	unit_cost_usd FLOAT NOT NULL,
	unit_price_usd FLOAT NOT NULL,
	subcategory_key int NOT NULL,
	subcategory nvarchar(50) NOT NULL,
	category_key nvarchar(50) NOT NULL,
	category nvarchar(50) NOT NULL
)

/* The table below was originally named sales, renamed to orders. */
CREATE TABLE sample_retail.orders (
    order_number INT NOT NULL,
    line_item INT NOT NULL,
    order_date DATE NOT NULL,
    delivery_date DATE,
    customer_key INT NOT NULL,
    store_key INT NOT NULL,
    product_key INT NOT NULL,
    quantity INT NOT NULL,
    currency_code CHAR(3) NOT NULL
);

CREATE TABLE sample_retail.stores (
    store_key INT NOT NULL,
    country CHAR(15) NOT NULL,
    customer_state CHAR(30) NOT NULL,
    square_meters INT,
    open_date DATE NOT NULL
);

CREATE TABLE sample_retail.exchange_rates (
    rate_date DATE NOT NULL,
    currency CHAR(3) NOT NULL,
    exchange_rate DECIMAL(10, 4) NOT NULL
);

WITH product_amount AS (
	SELECT
		ors.order_date,
		ors.customer_key,
		ors.order_number,
		ors.product_key,
		ors.quantity,
		ors.delivery_date,
		prs.unit_price_usd,
		prs.product_name,
		(ors.quantity * prs.unit_price_usd) AS cost_of_items
	FROM
		sample_retail.orders ors
	INNER JOIN
		sample_retail.customers cu
	ON  cu.customer_key = ors.customer_key
	INNER JOIN
		sample_retail.products prs
	ON  ors.product_key = prs.product_key
),
asc_order_metrics AS (
    SELECT
    	customer_key,
    	order_number,
    	order_date,
    	SUM(cost_of_items) AS cost_of_order,
		RANK() OVER (
            PARTITION BY customer_key
            ORDER BY order_date ASC
        ) AS last_rank
    FROM 
    	product_amount
    GROUP BY
    	customer_key,
    	order_number,
    	order_date
),
desc_order_metrics AS (
    SELECT
    	customer_key,
    	order_number,
    	order_date,
    	SUM(cost_of_items) AS cost_of_order,
		RANK() OVER (
            PARTITION BY customer_key
            ORDER BY order_date DESC
        ) AS first_rank
    FROM 
    	product_amount
    GROUP BY
    	customer_key,
    	order_number,
    	order_date
),
customer_spend_metrics AS (
	SELECT
		customer_key,
		COUNT(order_number) AS total_orders_placed,
		SUM(cost_of_items) AS total_order_amount,
		ROUND((SUM(cost_of_items)/COUNT(order_number)), 2) AS avg_order_amount,
		SUM(quantity) AS total_items_purchased,
		(SUM(quantity)/COUNT(order_number)) AS avg_items_purchased,
		ROUND((SUM(cost_of_items)/SUM(quantity)), 2) AS avg_item_amount
	FROM 
		product_amount
	GROUP BY
		customer_key
),
ranked_products AS (
    SELECT
        customer_key,
        product_key,
		product_name,
        SUM(quantity) AS product_purchased_count,
        RANK() OVER (
            PARTITION BY customer_key
            ORDER BY SUM(quantity) DESC
        ) AS purchase_rank
    FROM 
        product_amount
    GROUP BY
        customer_key,
        product_key,
		product_name
),
ship_days AS (
	SELECT
		customer_key,
		order_number,
		order_date,
		delivery_date,
		CASE
			WHEN delivery_date IS NULL THEN 1
			ELSE DATEDIFF(DAY, order_date, delivery_date)
		END AS days_to_ship
	FROM 
		product_amount
	GROUP BY
		customer_key,
		order_number,
		order_date,
		delivery_date
)
SELECT
    pa.customer_key AS Customer_ID,
    aom.order_number AS First_Order_ID,
    aom.order_date AS First_Order_Date,
    aom.cost_of_order AS First_Order_Amount,
    dom.order_number AS Last_Order_ID,
    dom.order_date AS Last_Order_Date,
    dom.cost_of_order AS Last_Order_Amount,
    csm.total_order_amount AS Total_Customer_Order_Amount,
    csm.avg_order_amount AS Average_Customer_Order_Amount,
    csm.total_items_purchased AS Total_Items_Purchased,
    csm.avg_items_purchased AS Average_Items_Purchased,
    csm.avg_item_amount AS Average_Item_Amount,
    rp.product_name AS Commonly_Purchased_Item,
    ROUND((SUM(sd.days_to_ship)/COUNT(sd.order_number)), 2) AS Average_Days_to_Ship
FROM
    product_amount pa
INNER JOIN
    asc_order_metrics aom
ON
    pa.customer_key = aom.customer_key
AND last_rank = 1
INNER JOIN
    desc_order_metrics dom
ON
    pa.customer_key = dom.customer_key
AND first_rank = 1
INNER JOIN
    customer_spend_metrics csm
ON
    pa.customer_key = csm.customer_key
INNER JOIN
    ranked_products rp
ON
    pa.customer_key = rp.customer_key
INNER JOIN
    ship_days sd
ON
    pa.customer_key = sd.customer_key
AND rp.purchase_rank = 1
GROUP BY
	pa.customer_key,
	aom.order_number,
	aom.order_date,
	aom.cost_of_order,
	dom.order_number,
	dom.order_date,
	dom.cost_of_order,
	csm.total_order_amount,
	csm.avg_order_amount,
	csm.total_items_purchased,
	csm.avg_items_purchased,
	csm.avg_item_amount,
	rp.product_name;

