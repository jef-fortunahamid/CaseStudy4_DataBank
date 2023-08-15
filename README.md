# Case Study #4 Data Bank

*Note: All information and data related to the case study were obtained from [here](https://8weeksqlchallenge.com/case-study-4/).*
![Screenshot 2023-08-13 at 8 38 04 pm](https://github.com/jef-fortunahamid/CaseStudy4_DataBank/assets/125134025/b4000e01-a425-47e3-9cbe-010b3ad6ee3f)

## Business Task
Neo-Banks, the new-age digital-only banks, have inspired Danny to innovate at the crossroads of banking, cryptocurrency, and data storage. Thus, "Data Bank" was born. Unlike traditional digital banks, Data Bank offers a unique proposition: a highly secure distributed data storage platform. The amount of cloud data storage a customer gets is directly proportional to their account balance.
The challenge for Data Bank is twofold. Firstly, they aim to expand their customer base. Secondly, they need to estimate the data storage requirements of their customers. Our mission is to assist Data Bank in analyzing their data intelligently, calculating essential metrics, and forecasting to aid their future planning. The Data Bank team has provided a data model and sample dataset rows to familiarize us with their system.

## Entity Relationship Diagram
![image](https://github.com/jef-fortunahamid/CaseStudy4_DataBank/assets/125134025/92671c19-bfb2-4adc-9398-6008f4dda2c6)

## General Insights
1. Customer Node Exploration:
- *System Nodes:* The queries analyzed the number of unique nodes present in the Data Bank system and provided a breakdown of these nodes per region.
- *Customer Allocation:*  Insights were derived on the allocation of customers to various nodes and regions. This can help understand the distribution and possible network load in different regions.
- *Node Reallocation:* The average duration customers are associated with a node before being reallocated was calculated. Additionally, metrics like the median, 80th, and 95th percentiles for node reallocation were explored per region. This provides insights into the stability and churn of node allocations.
2. Customer Transactions
- *Transaction Analysis:* The transactions were analyzed based on their types, providing insights into the frequency and value of each transaction type.
- *Deposit Behaviour:* The average count and amount of deposits were calculated, giving an understanding of customer saving habits.
- *Monthly Activities:* The monthly activity of customers was analyzed, showing how many customers have active transaction behaviors like multiple deposits and at least one purchase or withdrawal.
- *Balance Insights:* Monthly balance behaviors of customers were explored, highlighting the closing balance and the monthly change in balance.
- *Balance Trends:* A deeper analysis into the customer's balances was conducted to determine trends like increasing or decreasing balances and shifts from positive to negative balances.

## Key SQL Syntax and Functions:
- Temporary Tables (`CREATE TEMP TABLE`)
- Joins (`INNER JOIN`, `LEFT JOIN`)
- Aggregation Functions (`SUM`, `COUNT`, `AVG`)
- Common Table Expressions (CTE)
- Conditional Logic (`CASE WHEN`)
- Date Functions (`DATE_TRUNC`, `DATE_PART`, `AGE`, `TO_DATE`)
- Window Functions (`ROW_NUMBER`, `LAG`)
- Recursive Queries (`WITH RECURSIVE`)
- Analytical Function (`PERCENTILE_CONT`)
- Set Operation (`UNION ALL`)

## Questions and Solutions

### Part A: Customer Node Exploration
> 1. How many unique nodes are there on the Data Bank system?
```sql
WITH combinations AS (
  SELECT DISTINCT
      region_id
    , node_id
  FROM data_bank.customer_nodes
)
SELECT
  COUNT(*) as unique_nodes
FROM combinations;
```
![image](https://github.com/jef-fortunahamid/CaseStudy4_DataBank/assets/125134025/8f57f063-5c34-4f69-9f57-ac068099831b)

> 2. What is the number of nodes per region?
```sql
SELECT
    re.region_name
  , COUNT(DISTINCT cn.node_id) AS nodes_per_region
FROM data_bank.customer_nodes AS cn 
INNER JOIN data_bank.regions AS re 
  ON cn.region_id = re.region_id
GROUP BY re.region_name
ORDER BY re.region_name;
```
![image](https://github.com/jef-fortunahamid/CaseStudy4_DataBank/assets/125134025/5108066c-68a4-4d58-8d96-7057d7842c58)

> 3. How many customers are allocated to each region?
```sql
SELECT
    re.region_name
  , COUNT(DISTINCT cn.customer_id) AS customers_count
FROM data_bank.customer_nodes AS cn 
INNER JOIN data_bank.regions AS re 
  ON cn.region_id = re.region_id
GROUP BY re.region_name
ORDER BY re.region_name;
```
![image](https://github.com/jef-fortunahamid/CaseStudy4_DataBank/assets/125134025/d432ff00-16d3-4fc9-ba5a-6e7291d2044e)

> 4. How many days on average are customers reallocated to a different node?
```sql
DROP TABLE IF EXISTS ranked_customer_nodes;
CREATE TEMP TABLE ranked_customer_nodes AS
  SELECT
      customer_id
    , node_id
    , region_id
    , DATE_PART('day', AGE(end_date, start_date))::INT AS duration
    , ROW_NUMBER() OVER(
          PARTITION BY customer_id
          ORDER BY start_date
          ) AS ranking
  FROM data_bank.customer_nodes;

WITH RECURSIVE output_table AS (
  SELECT
      customer_id
    , node_id
    , duration
    , ranking
    , 1 AS run_id
  FROM ranked_customer_nodes
  WHERE ranking = 1

UNION ALL

  SELECT
      t2.customer_id
    , t2.node_id
    , t2.duration
    , t2.ranking
    , CASE
        WHEN t1.node_id != t2.node_id THEN t1.run_id +1
        ELSE t1.run_id
        END AS run_id
  FROM output_table AS t1 
  INNER JOIN ranked_customer_nodes AS t2  
    ON t1.customer_id = t2.customer_id
    AND t1.ranking + 1 = t2.ranking
    AND t2.ranking >1
),
customer_nodes AS (
  SELECT
      customer_id
    , run_id
    , SUM(duration) AS node_duration
  FROM output_table
  GROUP BY
      customer_id
    , run_id
)
SELECT
  ROUND(AVG(node_duration)) AS average_node_duration
FROM customer_nodes;
```
![image](https://github.com/jef-fortunahamid/CaseStudy4_DataBank/assets/125134025/37c67ba7-0bed-49f0-8826-3c79e62d9016)

> 5. What is the median, 80th and 95th percentile for this same reallocation days metric for each region?
```sql
DROP TABLE IF EXISTS ranked_customer_nodes;
CREATE TEMP TABLE ranked_customer_nodes AS
  SELECT
      customer_id
    , node_id
    , region_id
    , DATE_PART('day', AGE(end_date, start_date))::INT AS duration
    , ROW_NUMBER() OVER(
          PARTITION BY customer_id
          ORDER BY start_date
          ) AS ranking
  FROM data_bank.customer_nodes;

WITH RECURSIVE output_table AS (
  SELECT
      customer_id
    , node_id
    , region_id
    , duration
    , ranking
    , 1 AS run_id
  FROM ranked_customer_nodes
  WHERE ranking = 1

UNION ALL

  SELECT
      t2.customer_id
    , t2.node_id
    , t2. region_id
    , t2.duration
    , t2.ranking
    , CASE
        WHEN t1.node_id != t2.node_id THEN t1.run_id +1
        ELSE t1.run_id
        END AS run_id
  FROM output_table AS t1 
  INNER JOIN ranked_customer_nodes AS t2  
    ON t1.customer_id = t2.customer_id
    AND t1.ranking + 1 = t2.ranking
    AND t2.ranking >1
),
customer_nodes AS (
  SELECt
      customer_id
    , region_id
    , run_id
    , SUM(duration) AS node_duration
  FROM output_table
  GROUP BY
      customer_id
    , region_id
    , run_id
)
SELECT
    regions.region_name
  , ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY node_duration)) AS median_node_duration
  , ROUND(PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY node_duration)) AS pc80_node_duration
  , ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY node_duration)) AS pc95_node_duration
FROM customer_nodes
INNER JOIN data_bank.regions
  ON customer_nodes.region_id = regions.region_id
GROUP BY regions.region_name
ORDER BY regions.region_name;
```
![image](https://github.com/jef-fortunahamid/CaseStudy4_DataBank/assets/125134025/0b95179e-377a-422e-8882-443659e0c2ac)

### Part B: Customer Transactions
> 1. What is the unique count and total amount for each transaction type?
```sql
SELECT
    txn_type
  , COUNT(*) AS txn_count
  , SUM(txn_amount) AS total_amount
FROM data_bank.customer_transactions
GROUP BY txn_type;
```
![image](https://github.com/jef-fortunahamid/CaseStudy4_DataBank/assets/125134025/df9fdfa9-1728-4c88-a4a6-97f1e3d10155)

> 2. What is the average total historical deposit counts and amounts for all customers?
```sql
WITH txn_count_avg_amount AS (
  SELECT
      customer_id
    , COUNT(*) AS deposit_count
    , SUM(txn_amount) AS total_deposit_amount
  FROM data_bank.customer_transactions
  WHERE txn_type = 'deposit'
  GROUP BY customer_id
)
SELECT
    ROUND(AVG(deposit_count)) AS avg_deposit_count
  , ROUND(AVG(total_deposit_amount / deposit_count )) AS avg_txn_amount
FROM txn_count_avg_amount;
```
![image](https://github.com/jef-fortunahamid/CaseStudy4_DataBank/assets/125134025/e60bc204-e380-41d3-941a-430ffc61a84a)

> 3. For each month - how many Data Bank customers make more than 1 deposit and at least either 1 purchase or 1 withdrawal in a single month?
```sql
WITH monthly_transactions AS (
  SELECT
      customer_id
    , DATE_PART('month', txn_date) AS month
    , SUM(CASE WHEN txn_type = 'deposit' THEN 1 ELSE 0 END) AS deposit_count
    , SUM(CASE WHEN txn_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count
    , SUM(CASE WHEN txn_type = 'withdrawal' THEN 1 ELSE 0 END) AS withdrawal_count
  FROM data_bank.customer_transactions
  GROUP BY
      customer_id
    , DATE_PART('month', txn_date)
)
SELECT
    TO_DATE('2020-' || month::TEXT || '-01', 'YYYY-MM-DD') AS month
  , COUNT(DISTINCT customer_id) AS customer_count
FROM monthly_transactions
WHERE deposit_count > 1
  AND (purchase_count >=1 OR withdrawal_count >= 1)
GROUP BY month
ORDER BY month;
```
![image](https://github.com/jef-fortunahamid/CaseStudy4_DataBank/assets/125134025/2dc6c7d5-a0c6-4263-aeea-32bef54a99ad)

> 4. What is the closing balance for each customer at the end of the month? Also show the change in balance each month in the same table output.

```sql
SELECT
    DATE_TRUNC('month', txn_date)::DATE AS month
  , COUNT(customer_id) AS record_count
FROM data_bank.customer_transactions
GROUP BY month
ORDER BY month;
```
![image](https://github.com/jef-fortunahamid/CaseStudy4_DataBank/assets/125134025/8a6566b5-4d04-47e6-b0c8-3bd60fde9333)

```sql
WITH monthly_balances AS (
  SELECT
      customer_id
    , DATE_TRUNC('month', txn_date)::DATE AS month
    , SUM(CASE
            WHEN txn_type = 'deposit' THEN txn_amount
            ELSE (-txn_amount)
            END) AS balance
  FROM data_bank.customer_transactions
  GROUP BY customer_id, month
  ORDER BY customer_id, month
),
generated_months AS (
  SELECT
      DISTINCT customer_id
    , ('2020-01-01'::DATE + GENERATE_SERIES(0, 3) * INTERVAL '1 MONTH')::DATE AS month
  FROM data_bank.customer_transactions
)
SELECT
    gm.customer_id
  , gm.month
  , COALESCE(mb.balance, 0) AS balance_contribution
  , SUM(mb.balance) OVER(
        PARTITION BY gm.customer_id
        ORDER BY gm.month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS ending_balance
FROM generated_months AS gm
LEFT JOIN monthly_balances AS mb 
  ON gm.month = mb.month
  AND gm.customer_id = mb.customer_id
WHERE gm.customer_id BETWEEN 1 AND 3;
```
![image](https://github.com/jef-fortunahamid/CaseStudy4_DataBank/assets/125134025/bec4ad79-8707-4933-bd21-b3a5c8e6ac32)

> 5. Comparing the closing balance of a customer’s first month and the closing balance from their second month, what percentage of customers:
> - Have a negative first month balance?
> - Have a positive first month balance?
> - Increase their opening month’s positive closing balance by more than 5% in the following month?
> - Reduce their opening month’s positive closing balance by more than 5% in the following month?
> - Move from a positive balance in the first month to a negative balance in the second month?
```sql
WITH monthly_balances AS (
  SELECT
      customer_id
    , DATE_TRUNC('month', txn_date)::DATE AS month
    , SUM(CASE
          WHEN txn_type = 'deposit' THEN txn_amount
          WHEN txn_type IN ('withdrawal', 'purchase') THEN -txn_amount
          END) AS balance
  FROM data_bank.customer_transactions
  GROUP BY customer_id, month
  ORDER BY customer_id, month
),
generated_months AS (
  SELECT
      customer_id
    , (DATE_TRUNC('month', MIN(txn_date))::DATE + GENERATE_SERIES(0, 1) * INTERVAL '1 MONTH')::DATE AS month
    , GENERATE_SERIES (1, 2) AS month_number
  FROM data_bank.customer_transactions
  GROUP BY customer_id
),
monthly_transactions AS (
  SELECT
      gm.customer_id
    , gm.month
    , gm.month_number
    , COALESCE(mb.balance, 9001) AS transaction_amount 
  FROM generated_months AS gm 
  LEFT JOIN monthly_balances AS mb 
    ON gm.month = mb.month
    AND gm.customer_id = mb.customer_id
),
monthly_aggregates AS (
  SELECT
      customer_id
    , month_number
    , LAG(transaction_amount) OVER(
                    PARTITION BY customer_id
                    ORDER BY month
                    ) AS previous_month_transaction_amount
    , transaction_amount
  FROM monthly_transactions
),
calculations AS (
  SELECT
      COUNT(customer_id) AS customer_count
    , SUM(CASE WHEN previous_month_transaction_amount > 0 THEN 1 ELSE 0 END) AS positive_first_month
    , SUM(CASE WHEN previous_month_transaction_amount < 0 THEN 1 ELSE 0 END) AS negative_first_month
    , SUM(CASE 
            WHEN previous_month_transaction_amount > 0 
            AND transaction_amount > 0
            AND transaction_amount > 0.5 * previous_month_transaction_amount
            THEN 1 
          ELSE 0 
        END) AS increase_count
    , SUM(CASE 
            WHEN previous_month_transaction_amount > 0 
            AND (transaction_amount < 0
            AND transaction_amount < -0.05 * previous_month_transaction_amount)
            THEN 1 
          ELSE 0 
        END) AS decrease_count
    , SUM(CASE 
            WHEN previous_month_transaction_amount > 0 
            AND transaction_amount < 0
            AND transaction_amount < -previous_month_transaction_amount
            THEN 1 
          ELSE 0 
        END) AS negative_count
    FROM monthly_aggregates
    WHERE previous_month_transaction_amount IS NOT NULL
)
SELECT
    ROUND(100 * positive_first_month / customer_count, 2) AS positive_pc
  , ROUND(100 * negative_first_month / customer_count, 2) AS negative_pc
  , ROUND(100 * increase_count / positive_first_month, 2) AS increase_pc
  , ROUND(100 * decrease_count / positive_first_month, 2) AS decrease_pc
  , ROUND(100 * negative_count / positive_first_month, 2) AS negative_balance_pc
FROM calculations;
```
![image](https://github.com/jef-fortunahamid/CaseStudy4_DataBank/assets/125134025/1e15ea5e-d4e0-44ba-812c-0cda4733eaeb)















