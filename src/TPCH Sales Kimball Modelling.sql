-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Kimball Dimensional Modeling
-- MAGIC
-- MAGIC Dimensional modeling is a design concept used in data warehousing to structure data for easy retrieval and analysis. It involves organizing data into fact and dimension tables. Fact tables store quantitative data for analysis, while dimension tables store descriptive attributes related to the facts.
-- MAGIC
-- MAGIC #### Dimensions
-- MAGIC Dimensions provide context to the facts, making the data meaningful and easier to understand.
-- MAGIC
-- MAGIC ##### Characteristics
-- MAGIC - **Descriptive Attributes**: Dimensions contain descriptive information, such as names, dates, or categories.
-- MAGIC - **Unique Row Key**: Each row has a [surrogate key](https://en.wikipedia.org/wiki/Surrogate_key) that uniquely identifies each record. This is analogous to a primary key in transactional DBs.
-- MAGIC - **Handle Unknown, Unresolved, or Invalid Data**: Kimball dimensions typically include special-purpose rows to manage incomplete or missing data. These rows often contain descriptions like “Unknown,” “Not Applicable,” or “Pending” for cases where data is missing or unresolved. This ensures referential integrity and allows for analysis even with incomplete data.
-- MAGIC - **Denormalizated**: Dimension tables in Kimball design are usually denormalized to avoid complex joins and support faster query performance, simplifying the data structure for business intelligence and reporting.
-- MAGIC - **Can track changes in different ways**: Dimensions come in different "types", refered to as [slowly changing dimension (SCD) types](https://en.wikipedia.org/wiki/Slowly_changing_dimension). For example, a table that contains the current snapshot of the world is called an SCD Type 1 dimension; but if the table tracks historical changes over time, it's an SCD Type 2 dimension. Databricks has [built-in functions](https://docs.databricks.com/en/delta-live-tables/cdc.html) to make creating these types easy.
-- MAGIC
-- MAGIC #### Fact Tables
-- MAGIC Fact tables are the tables in a data warehouse that store quantitative data for analysis. Fact tables contain foreign keys that reference dimension tables, linking the quantitative data to descriptive attributes.
-- MAGIC
-- MAGIC ##### Characteristics
-- MAGIC - **Quantitative Data**: Fact tables store quantitative data for analysis, such as sales amounts, quantities, or transaction counts.
-- MAGIC - **Foreign Keys**: Fact tables contain foreign keys that reference dimension tables, linking the quantitative data to descriptive attributes.
-- MAGIC - **Granularity**: The level of detail in a fact table is defined by its **grain**, which determines the lowest level of information captured (e.g., 1 row per individual transactions, 1 row per customer per day).
-- MAGIC - **Additive Measures**: Measures in fact tables are often additive, meaning they can be summed across dimensions (e.g., total sales by region).
-- MAGIC - **Aggregations**: Fact tables can include pre-aggregated data to improve query performance, such as monthly or yearly totals.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Dimension: customers_dim
-- MAGIC ##### Type: SCD 1
-- MAGIC
-- MAGIC This table contains customer-related information, providing the current descriptive attributes for each customer. It includes details such as the customer's name, address, nation, region, and phone number.
-- MAGIC
-- MAGIC | Column  | Type   | Description                        |
-- MAGIC |---------|--------|------------------------------------|
-- MAGIC | key     | BIGINT | Unique key for each record  |
-- MAGIC | name    | STRING | Customer's name                    |
-- MAGIC | address | STRING | Customer's address                 |
-- MAGIC | nation  | STRING | Customer's nation                  |
-- MAGIC | region  | STRING | Customer's region                  |
-- MAGIC | phone   | STRING | Customer's phone number            |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW customers_dim (
  key BIGINT PRIMARY KEY,
  name STRING,
  address STRING,
  nation STRING,
  region STRING,
  phone STRING
) AS 
WITH nation_regions AS (
  SELECT
    n.n_nationkey AS nationkey,
    n.n_name AS nation,
    r.r_name AS region
  FROM
    samples.tpch.nation n
    JOIN samples.tpch.region r ON n.n_regionkey = r.r_regionkey
)
SELECT
  c.c_custkey AS key,
  c.c_name AS name,
  c.c_address AS address,
  n.nation AS nation,
  n.region AS region,
  c.c_phone AS phone
FROM
  samples.tpch.customer c
  JOIN nation_regions n ON c.c_nationkey = n.nationkey
UNION ALL 
SELECT -1, 'unknown name', 'unknown address', 'unknown nation', 'unknown region', 'unknown phone number'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dimension: parts_dim
-- MAGIC ##### Type: SCD 1
-- MAGIC
-- MAGIC This table contains information on the parts sold by the company, providing the current descriptive attributes for each part. It includes details such as the part's name, manufacturer, brand, type, size, container, and price.
-- MAGIC
-- MAGIC | Column      | Type          | Description                        |
-- MAGIC |-------------|---------------|------------------------------------|
-- MAGIC | key         | BIGINT        | Unique key for each record         |
-- MAGIC | name        | STRING        | Part's name                        |
-- MAGIC | manufacturer| STRING        | Part's manufacturer                |
-- MAGIC | brand       | STRING        | Part's brand                       |
-- MAGIC | type        | STRING        | Part's type                        |
-- MAGIC | size        | INT           | Part's size                        |
-- MAGIC | container   | STRING        | Part's container                   |
-- MAGIC | price       | DECIMAL(18,2) | Part's price                       |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW parts_dim (
  key BIGINT PRIMARY KEY,
  name STRING,
  manufacturer STRING,
  brand STRING,
  type STRING,
  size INT,
  container STRING,
  price DECIMAL(18,2)
) AS 
SELECT
  p.p_partkey AS key,
  p.p_name AS name,
  p.p_mfgr AS manufacturer,
  p.p_brand as brand,
  p.p_type AS type,
  p.p_size AS size,
  p.p_container AS container,
  p.p_retailprice AS price
FROM samples.tpch.part p
UNION ALL
SELECT -1, 'unknown part', 'unknown manufacturer', 'unknown brand', 'unknown type', NULL, 'unknown container', NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dimension: date_dim
-- MAGIC ##### Type: SCD 1
-- MAGIC
-- MAGIC This table contains date-related information, providing various attributes for each day in time. It includes details such as the date itself, day of the week, day of the month, day of the year, week of the year, month name, month number, quarter, year, and whether it is a weekend.
-- MAGIC
-- MAGIC | Column           | Type    | Description                              |
-- MAGIC |------------------|---------|------------------------------------------|
-- MAGIC | key              | BIGINT  | Unique key for each record               |
-- MAGIC | date             | DATE    | The date                                 |
-- MAGIC | day_of_week_name | STRING  | Name of the day of the week              |
-- MAGIC | day_of_week      | INT     | Day of the week (1 = Sunday, 2 = Monday) |
-- MAGIC | day_of_month     | INT     | Day of the month (1 to 31)               |
-- MAGIC | day_of_year      | INT     | Day of the year (1 to 366)               |
-- MAGIC | week_of_year     | INT     | Week number of the year (1 to 53)        |
-- MAGIC | month_name       | STRING  | Name of the month                        |
-- MAGIC | month_number     | INT     | Month number (1 to 12)                   |
-- MAGIC | quarter          | INT     | Quarter of the year (1 to 4)             |
-- MAGIC | year             | INT     | Year                                      |
-- MAGIC | is_weekend       | BOOLEAN | Flag indicating if the date is a weekend |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW date_dim (
  key BIGINT PRIMARY KEY,
  date DATE,
  day_of_week_name STRING,
  day_of_week INT,
  day_of_month INT,
  day_of_year INT,
  week_of_year INT,
  month_name STRING,
  month_number INT,
  quarter INT,
  year INT,
  is_weekend BOOLEAN
) AS
WITH dates AS (
  SELECT 
    explode(sequence(DATE'1992-01-01', DATE'1998-12-31', INTERVAL 1 DAY)) as date
)
SELECT
    CAST(DATE_FORMAT(date, 'yyyyMMdd') AS BIGINT) AS key,    -- Format YYYYMMDD
    date,
    DATE_FORMAT(date, 'EEEE') AS day_of_week_name,            -- Day of the week name (e.g., 'Monday')
    DAYOFWEEK(date) AS day_of_week,                           -- Day of the week (1 = Sunday, 2 = Monday, ...)
    DAY(date) AS day_of_month,                                -- Day of the month (1 to 31)
    DAYOFYEAR(date) AS day_of_year,                           -- Day of the year (1 to 366)
    WEEKOFYEAR(date) AS week_of_year,                         -- Week number of the year (1 to 53)
    DATE_FORMAT(date, 'MMMM') AS month_name,                  -- Month name (e.g., 'January')
    MONTH(date) AS month_number,                              -- Month number (1 to 12)
    QUARTER(date) AS quarter,                                 -- Quarter of the year (1 to 4)
    YEAR(date) AS year,                                       -- Year (e.g., 2024)
    CASE WHEN DAYOFWEEK(date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend -- Weekend flag
FROM dates
UNION ALL
SELECT -1, NULL, 'unknown day of week', NULL, NULL, NULL, NULL, 'unknown month', NULL, NULL, NULL, NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dimension: month_dim
-- MAGIC ##### Type: SCD 1
-- MAGIC
-- MAGIC This table contains month-related information, providing various attributes for each month in time. It includes details such as the month name, month number, quarter, year, and the number of days in the month.
-- MAGIC
-- MAGIC | Column      | Type    | Description                              |
-- MAGIC |-------------|---------|------------------------------------------|
-- MAGIC | key         | BIGINT  | Unique key for each record               |
-- MAGIC | date        | DATE    | The first date of the month              |
-- MAGIC | month_name  | STRING  | Name of the month                        |
-- MAGIC | month_number| INT     | Month number (1 to 12)                   |
-- MAGIC | quarter     | INT     | Quarter of the year (1 to 4)             |
-- MAGIC | year        | INT     | Year                                     |
-- MAGIC | num_days    | INT     | Number of days in the month              |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW month_dim (
  key BIGINT PRIMARY KEY NOT NULL,
  date DATE,
  month_name STRING,
  month_number INT,
  quarter INT,
  year INT,
  num_days INT
) AS
WITH dates1 AS (
  SELECT
    explode(sequence(DATE'1992-01-01', DATE'1998-12-31', INTERVAL 1 MONTH)) as date
), dates AS (
  SELECT
      CAST(DATE_FORMAT(date, 'yyyyMM') AS BIGINT) AS key,
      date,
      DATE_FORMAT(date, 'MMMM') AS month_name,
      MONTH(date) AS month_number,
      QUARTER(date) AS quarter,
      YEAR(date) AS year,
      DATEDIFF(add_months(date, 1), date) AS num_days
  FROM dates1
)
SELECT * FROM dates WHERE key IS NOT NULL
UNION ALL
SELECT -1, NULL, 'unknown month', NULL, NULL, NULL, NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact Table: sales_facts
-- MAGIC ##### Grain: 1 row per order line item
-- MAGIC
-- MAGIC This table contains sales transaction data, capturing detailed information about each sale. It includes keys to related dimension tables, sales metrics, and status flags.
-- MAGIC
-- MAGIC | Column           | Type          | Description                                      |
-- MAGIC |------------------|---------------|--------------------------------------------------|
-- MAGIC | order_num        | BIGINT        | Unique ID of the order                           |
-- MAGIC | line_item_num    | BIGINT        | Line item number within the order                |
-- MAGIC | customer_key     | BIGINT        | Foreign key to the customer dimension            |
-- MAGIC | part_key         | BIGINT        | Foreign key to the part dimension                |
-- MAGIC | num_parts        | DECIMAL(18,2) | Number of parts sold                             |
-- MAGIC | gross_sales      | DECIMAL(18,2) | Gross sales amount                               |
-- MAGIC | discount         | DECIMAL(18,2) | Discount applied                                 |
-- MAGIC | tax              | DECIMAL(18,2) | Tax applied                                      |
-- MAGIC | net_sales        | DECIMAL(18,2) | Net sales amount after discount and tax          |
-- MAGIC | is_fulfilled     | INT           | Flag indicating if the order is fulfilled (1/0)  |
-- MAGIC | is_returned      | INT           | Flag indicating if the order is returned (1/0)   |
-- MAGIC | order_date_key   | BIGINT        | Foreign key to the order date dimension          |
-- MAGIC | commit_date_key  | BIGINT        | Foreign key to the commit date dimension         |
-- MAGIC | receipt_date_key | BIGINT        | Foreign key to the receipt date dimension        |
-- MAGIC | ship_date_key    | BIGINT        | Foreign key to the ship date dimension           |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW sales_facts (
  order_num BIGINT NOT NULL,
  line_item_num INT NOT NULL,
  customer_key BIGINT FOREIGN KEY REFERENCES dbdemos.tpch_kimball.customers_dim,
  part_key BIGINT FOREIGN KEY REFERENCES dbdemos.tpch_kimball.parts_dim,
  num_parts DECIMAL(18,2),
  gross_sales DECIMAL(18,2),
  discount DECIMAL(18,2),
  tax DECIMAL(18,2),
  net_sales DECIMAL(18,2),
  is_fulfilled INT,
  is_returned INT,
  order_date_key BIGINT CONSTRAINT order_date_fk FOREIGN KEY REFERENCES dbdemos.tpch_kimball.date_dim,
  commit_date_key BIGINT CONSTRAINT commit_date_fk FOREIGN KEY REFERENCES dbdemos.tpch_kimball.date_dim,
  receipt_date_key BIGINT CONSTRAINT receipt_date_fk FOREIGN KEY REFERENCES dbdemos.tpch_kimball.date_dim,
  ship_date_key BIGINT CONSTRAINT ship_date_fk FOREIGN KEY REFERENCES dbdemos.tpch_kimball.date_dim,
  CONSTRAINT sales_pk PRIMARY KEY(order_num, line_item_num)
) AS WITH sales AS (
  SELECT
    o.o_orderkey order_num,
    l.l_linenumber line_item_num,
    IFNULL(o.o_custkey, -1) customer_key,
    IFNULL(l.l_partkey, -1)  part_key,
    l.l_quantity num_parts,
    l.l_extendedprice gross_sales,
    l.l_discount discount,
    l.l_tax tax,
    CASE WHEN l.l_linestatus='F' THEN 1 ELSE 0 END is_fulfilled,
    CASE WHEN l.l_returnflag='R' THEN 1 ELSE 0 END is_returned,
    IFNULL(dbdemos.tpch_kimball.date_key(o.o_orderdate), -1) order_date_key,
    IFNULL(dbdemos.tpch_kimball.date_key(l.l_commitdate), -1) commit_date_key,
    IFNULL(dbdemos.tpch_kimball.date_key(l.l_receiptdate), -1) receipt_date_key,
    IFNULL(dbdemos.tpch_kimball.date_key(l.l_shipdate), -1) ship_date_key
  FROM samples.tpch.lineitem l
  JOIN samples.tpch.orders o ON l.l_orderkey = o.o_orderkey
  WHERE 
    o.o_orderkey IS NOT NULL AND l.l_linenumber IS NOT NULL
)
SELECT
  *,
  dbdemos.tpch_kimball.net_sales(gross_sales, discount, tax, is_returned) net_sales
FROM sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact Table: daily_sales_snapshot
-- MAGIC ##### Grain: 1 row per customer, part, and receipt date
-- MAGIC
-- MAGIC This table captures daily aggregated sales data, summarizing the sales activities for each customer and part on a given receipt date. It includes keys to related dimension tables and aggregated sales metrics.
-- MAGIC
-- MAGIC | Column             | Type          | Description                                      |
-- MAGIC |--------------------|---------------|--------------------------------------------------|
-- MAGIC | customer_key       | BIGINT        | Foreign key to the customer dimension            |
-- MAGIC | part_key           | BIGINT        | Foreign key to the part dimension                |
-- MAGIC | num_parts          | DECIMAL(28,2) | Total number of parts sold                       |
-- MAGIC | num_parts_returned | DECIMAL(38,2) | Total number of parts returned                   |
-- MAGIC | gross_sales        | DECIMAL(28,2) | Total gross sales amount                         |
-- MAGIC | sales_returned     | DECIMAL(38,2) | Total sales amount returned                      |
-- MAGIC | net_sales          | DECIMAL(28,2) | Total net sales amount after returns             |
-- MAGIC | receipt_date_key   | BIGINT        | Foreign key to the receipt date dimension        |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW daily_sales_snapshot (
  customer_key BIGINT NOT NULL FOREIGN KEY REFERENCES dbdemos.tpch_kimball.customers_dim,
  part_key BIGINT NOT NULL FOREIGN KEY REFERENCES dbdemos.tpch_kimball.parts_dim,
  num_parts DECIMAL(28,2) NOT NULL,
  num_parts_returned DECIMAL(38,2) NOT NULL,
  gross_sales DECIMAL(28,2) NOT NULL,
  sales_returned DECIMAL(38,2) NOT NULL,
  net_sales DECIMAL(28,2) NOT NULL,
  receipt_date_key BIGINT NOT NULL FOREIGN KEY REFERENCES dbdemos.tpch_kimball.date_dim,
  CONSTRAINT daily_sales_pk PRIMARY KEY(receipt_date_key, customer_key, part_key)
)
AS SELECT
  d.key AS receipt_date_key,
  IFNULL(s.customer_key, -1) customer_key,
  IFNULL(s.part_key, -1) part_key,
  IFNULL(SUM(s.num_parts), 0) num_parts,
  IFNULL(SUM(s.num_parts * s.is_returned), 0) num_parts_returned,
  IFNULL(sum(s.gross_sales), 0) gross_sales,
  IFNULL(SUM(s.gross_sales * s.is_returned), 0) sales_returned,
  IFNULL(SUM(s.net_sales), 0) net_sales
FROM LIVE.sales_facts s
RIGHT OUTER JOIN LIVE.date_dim d ON s.receipt_date_key = d.key
GROUP BY d.key, customer_key, part_key
ORDER BY receipt_date_key, customer_key, part_key ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fact Table: monthly_sales_snapshot
-- MAGIC ##### Grain: 1 row per customer, part, and receipt month
-- MAGIC
-- MAGIC This table captures monthly aggregated sales data, summarizing the sales activities for each customer and part within a given receipt month. It includes keys to related dimension tables and aggregated sales metrics.
-- MAGIC
-- MAGIC | Column             | Type          | Description                                      |
-- MAGIC |--------------------|---------------|--------------------------------------------------|
-- MAGIC | customer_key       | BIGINT        | Foreign key to the customer dimension            |
-- MAGIC | part_key           | BIGINT        | Foreign key to the part dimension                |
-- MAGIC | num_parts          | DECIMAL(38,2) | Total number of parts sold                       |
-- MAGIC | num_parts_returned | DECIMAL(38,2) | Total number of parts returned                   |
-- MAGIC | gross_sales        | DECIMAL(38,2) | Total gross sales amount                         |
-- MAGIC | sales_returned     | DECIMAL(38,2) | Total sales amount returned                      |
-- MAGIC | net_sales          | DECIMAL(38,2) | Total net sales amount after returns             |
-- MAGIC | reciept_month_key  | BIGINT        | Foreign key to the receipt month dimension       |

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW monthly_sales_snapshot (
  customer_key BIGINT NOT NULL FOREIGN KEY REFERENCES dbdemos.tpch_kimball.customers_dim,
  part_key BIGINT NOT NULL FOREIGN KEY REFERENCES dbdemos.tpch_kimball.parts_dim,
  num_parts DECIMAL(38,2) NOT NULL,
  num_parts_returned DECIMAL(38,2) NOT NULL,
  gross_sales DECIMAL(38,2) NOT NULL,
  sales_returned DECIMAL(38,2) NOT NULL,
  net_sales DECIMAL(38,2) NOT NULL,
  reciept_month_key BIGINT NOT NULL FOREIGN KEY REFERENCES dbdemos.tpch_kimball.month_dim,
  CONSTRAINT monthly_sales_pk PRIMARY KEY(reciept_month_key, customer_key, part_key)
)
AS SELECT
  if(receipt_date_key != -1, receipt_date_key div 100, receipt_date_key) AS reciept_month_key,
  IFNULL(customer_key, -1) customer_key,
  IFNULL(part_key, -1) part_key,
  IFNULL(SUM(num_parts), 0) num_parts,
  IFNULL(SUM(num_parts_returned), 0) num_parts_returned,
  IFNULL(SUM(gross_sales), 0) gross_sales,
  IFNULL(SUM(sales_returned), 0) sales_returned,
  IFNULL(SUM(net_sales), 0) net_sales
FROM LIVE.daily_sales_snapshot
GROUP BY reciept_month_key, customer_key, part_key
ORDER BY reciept_month_key, customer_key, part_key ASC