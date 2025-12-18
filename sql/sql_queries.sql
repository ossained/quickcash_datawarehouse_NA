--Quickcash dimensional data warehouse

--create schema first
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS fact;
CREATE SCHEMA IF NOT EXISTS reports;


--Create Banking transaction table
CREATE TABLE staging.banking_transactions(
	transaction_id VARCHAR(20),
	customer_id VARCHAR(20),
	customer_name VARCHAR(100),
	service_type VARCHAR(30),
	transaction_amount DECIMAL(12,2),
	fee_amount DECIMAL (10,2),
	channel VARCHAR(20),
	transaction_timestamp TIMESTAMP,
	status VARCHAR(15),
	customer_rating INTEGER
);


--DIMENSION TABLES
--CUSTOMER DIMENSION TABLE
CREATE TABLE dim.customer AS 
SELECT DISTINCT --no duplicates
	ROW_NUMBER() OVER (ORDER BY customer_id)AS customer_sk,
	customer_id,
	customer_name,
	--customer segmentation
	CASE 
		WHEN AVG(transaction_amount) > 500000 THEN 'Premium'
		WHEN AVG(transaction_amount) > 100000 THEN 'Gold' 
		WHEN AVG(transaction_amount) > 20000  THEN 'Silver'
		ELSE 'Bronze'
		END AS customer_tier,
		--SCD TYPE 2
		CURRENT_DATE as effective_date,
		TRUE as is_current
		FROM staging.banking_transactions
		WHERE customer_id IS NOT NULL
		GROUP BY customer_id,customer_name;
	

---SERVICE DIMENSION TABLE
CREATE  TABLE dim.service AS 
SELECT DISTINCT 
	ROW_NUMBER () OVER (ORDER BY service_type) AS service_sk,
	service_type,
	CASE 
		WHEN service_type = 'Bank Transfer' THEN 'Money Transfer'
		WHEN service_type  IN ('Airtime_Purchase','Data Purchase') THEN 'telecom'
		WHEN service_type = 'Bill Payment' THEN 'Bills'
		ELSE 'Others'
	END AS service_category
From staging.banking_transactions
WHERE service_type IS NOT NULL
GROUP BY service_type;

--SERVICE DIMENSION
CREATE TABLE dim.channel AS
SELECT DISTINCT
	ROW_NUMBER () OVER (ORDER BY channel) AS channel_sk,
	channel,
	CASE
		WHEN channel = 'USSD' THEN 'Text_Based'
		WHEN channel = 'Mobile App' THEN 'Application'
		WHEN channel = 'Web Portal' THEN 'Web'
		ELSE 'Unclassified'
	END AS Channel_category
FROM staging.banking_transactions
WHERE channel IS NOT NULL
GROUP BY channel,channel_category;

--date dimension
CREATE TABLE dim.date AS
SELECT 
	TO_CHAR(date_val, 'YYYYMMDDD'):: INTEGER AS date_sk,
	date_val AS full_date,
	TRIM(TO_CHAR(date_val,'DAY')) AS day_name,
	TRIM(TO_CHAR(date_val,'MONTH')) AS month_name,
	EXTRACT(MONTH FROM date_val) AS minth_number,
	  CASE 
        WHEN EXTRACT(DOW FROM date_val) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
	EXTRACT(YEAR FROM date_val) AS yaer_number
FROM generate_series('2024-01-01'::DATE,'2024-12-31':: DATE, '1 day') AS date_val;


ALTER TABLE dim.customer ADD PRIMARY KEY (customer_sk);
ALTER TABLE dim.service ADD PRIMARY KEY (service_sk);
ALTER TABLE dim.channel ADD PRIMARY KEY (channel_sk);
ALTER TABLE dim.date ADD PRIMARY KEY (date_sk);

ALTER TABLE dim.customer ADD UNIQUE (customer_id);
ALTER TABLE fact.transactions ADD PRIMARY KEY (transaction_sk);

ALTER TABLE fact.transactions 
ADD CONSTRAINT FK_customer FOREIGN KEY (customer_sk) REFERENCES dim.customer(customer_sk);
ALTER TABLE fact.transactions 
ADD CONSTRAINT FK_service FOREIGN KEY (service_sk) REFERENCES dim.service(service_sk);
ALTER TABLE fact.transactions 
ADD CONSTRAINT FK_channel FOREIGN KEY (channel_sk) REFERENCES dim.channel(channel_sk);
ALTER TABLE fact.transactions 
 ADD CONSTRAINT FK_date FOREIGN KEY (date_sk) REFERENCES dim.date(date_sk);

---fact table
CREATE TABLE fact.transactions AS 
SELECT 
	ROW_NUMBER () OVER (ORDER BY bt.transaction_timestamp) transaction_sk,
	bt.transaction_id,
	--foreign keys
	c.customer_sk,
	s.service_sk,
	ch.channel_sk,
	d.date_sk,
	--the numbers
	bt.transaction_amount,
	bt.fee_amount,
	bt.customer_rating,
	bt.status,
	bt.transaction_timestamp
FROM staging.banking_transactions bt
LEFT JOIN dim.customer c ON bt.customer_id = c.customer_id
LEFT JOIN dim.service s ON bt.service_type = s.service_type
LEFT JOIN dim.channel ch ON bt.channel = ch.channel
LEFT JOIN dim.date d ON DATE(bt.transaction_timestamp) = d.full_date
WHERE bt.customer_id IS NOT NULL
LIMIT 1000;


---performance optimization 
CREATE INDEX idx_fact_customer ON fact.transactions (customer_sk);
CREATE INDEX idx_fact_service  ON fact.transactions(service_sk);
CREATE INDEX idx_fact_channel ON fact.transactions(channel_sk);
CREATE INDEX idx_fact_date ON fact.transactions(date_sk);


CREATE INDEX idx_customer ON dim.customer(customer_sk,customer_id);

---security and access control
CREATE ROLE quickcash_finace_team;
CREATE ROLE quickcash_markerting_team;

CREATE USER finance WITH PASSWORD 'secure_finance';
GRANT quickcash_finace_team TO finance;

GRANT SELECT ON fact.transactions TO quickcash_finace_team;









































































































