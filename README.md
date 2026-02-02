# Order-to-Insight
End-to-end data pipeline from raw events to business insights

## TL;DR

This project demonstrates an end-to-end data workflow that combines data engineering and data analytics.  
It shows how raw, imperfect event and transaction data is ingested, validated, transformed, modeled, and analyzed to support business decision-making.

The focus is on SQL-centric analytics, realistic data engineering practices, and clear reasoning about what the data does and does not tell us.

Technologies used include SQL, Python, and a lightweight analytical stack suitable for local development.

---

## Project context and problem definition

In many organizations, business decisions rely on data that originates from multiple systems. Orders, payments, and operational events are often collected separately, with inconsistent schemas, missing values, and unreliable timestamps.

This project simulates the work of an internal data team responsible for turning raw order and event data into reliable analytical insights.

The core problem addressed in this project is the following:

How can we transform raw event-based and transactional data into a trustworthy analytical dataset, and how do modeling and transformation choices affect the business conclusions drawn from that data?

---

## Project goals

The main goals of this project are:

1. Demonstrate understanding of the full data lifecycle from ingestion to analysis.
2. Show practical data engineering skills, including data validation, transformation, and modeling.
3. Apply SQL-based analytics to answer realistic business questions.
4. Highlight common data quality issues and analytical pitfalls.
5. Communicate results clearly, including limitations and risks.

This is not a machine learning project. The emphasis is on data foundations, correctness, and interpretability.

---

## Data overview

The project works with a combination of event data and transactional data.

Event data represents system-level events such as order creation, payment confirmation, and shipment updates.

Transactional data represents business-level facts such as completed orders, order values, and products.

The data may be synthetically generated, but it is designed to be structurally realistic and to include typical issues such as missing events, inconsistent timestamps, and partial records.

---

## End-to-end data pipeline

The data pipeline follows a clear and explicit structure:

1. Data ingestion  
   Raw data is loaded or generated using Python and stored in its original form without modification.

2. Data validation and enrichment  
   Basic validation rules are applied to identify missing, inconsistent, or suspicious records. Some enrichment may be performed where appropriate.

3. Data transformation and modeling  
   Raw data is transformed into analytical models using SQL. This includes joining events and transactions, normalizing timestamps, and constructing fact and dimension-style tables.

4. Analysis and insights  
   SQL-based analyses are used to answer business questions such as revenue trends, order performance, and data consistency issues.

5. Interpretation and documentation  
   Results are interpreted with a focus on assumptions, risks, and potential misinterpretations.

---

## Repository structure

The repository is organized to reflect the logical stages of the data pipeline.

The ingestion directory contains Python scripts responsible for loading or generating raw data.

The data directory contains raw and processed datasets, clearly separated to avoid accidental mixing.

The transformations directory contains SQL models used to clean, transform, and model the data.

The analysis directory contains SQL queries used for analytical insights.

The ai directory documents how AI tools were used as a secondary reviewer or validator during the project.

The README file provides context, explanations, and conclusions.

---

## Use of AI tools

AI tools were used in a limited and transparent way.

They were applied primarily as a second analyst to review assumptions, suggest validation checks, and challenge analytical interpretations.

AI was not used to automatically generate results or replace human decision-making. All final modeling and analytical decisions were made explicitly and documented.

---

## What was learned

Through this project, the following key lessons emerged:

Reliable analysis depends more on data quality and modeling choices than on complex algorithms.

Seemingly simple aggregations can lead to incorrect conclusions if event timing and data completeness are not handled carefully.

Clear separation between raw data, transformed data, and analysis greatly improves both correctness and maintainability.

Documenting assumptions and limitations is essential for responsible data work.

---

## What would be done differently in production

In a production environment, this project would be extended with automated data quality checks, monitoring, and scheduling.

Data ingestion and transformations would be orchestrated using a workflow tool.

Versioning, testing, and access controls would be applied more rigorously.

The core analytical logic, however, would remain largely the same.

---

## Risks and potential misinterpretations

Incomplete event data may lead to undercounting or overcounting certain business metrics.

Timestamp inconsistencies can distort time-based analyses if not normalized correctly.

Aggregated metrics may hide important edge cases or operational issues.

All insights should be interpreted with these limitations in mind.
---

## Analysis results and key findings

This section summarizes the analytical results produced from the modeled data layer.  
All results are generated by running the SQL queries in `analysis/insights.sql` against the DuckDB warehouse and written to `data/processed/analysis_results.txt`.

### Business-level snapshot

The dataset contains 5,000 orders in total.

Completed orders: 4,462  
Cancelled orders: 400  
Refunded orders: 138  

Total completed revenue: 662,793.00  
Average order value for completed orders: 148.54  

Interpretation:  
The majority of orders are completed, and the completed revenue and average order value provide a stable baseline for trend and performance analysis. This dataset is suitable for time-based and customer-level analytics once modeling and validation are applied.

### Data quality signals in the modeled layer

The fact table includes explicit data quality flags derived during modeling.

Completed orders missing a payment confirmation event: 124  
Cancelled orders that still have a shipment event: 7  

Interpretation:  
These mismatches demonstrate why transactional status and event data cannot be assumed to be fully consistent. Even when an order is marked as completed in the transaction table, missing payment events introduce uncertainty for funnel analysis, lead time metrics, and operational reporting.

### Event coverage across orders

Orders with order_created event: 100.00 percent  
Orders with payment_confirmed event: 89.48 percent  
Orders with order_shipped event: 89.38 percent  
Orders with order_cancelled event: 8.00 percent  

Interpretation:  
Payment and shipment events are not present for all orders. Analyses that rely solely on event data would undercount completed orders and revenue. This project surfaces this risk explicitly rather than hiding it behind aggregated metrics.

### Operational timing example

Average time from payment confirmation to shipment: 237 minutes  

Interpretation:  
In this dataset the lead time is constant because the synthetic generator uses a fixed offset. In real production data, this metric typically varies and becomes a meaningful operational KPI, but it is also sensitive to missing or misordered events.

### Notable edge case

The top customers by completed revenue include records with a missing customer identifier.

Interpretation:  
Missing customer identifiers are a realistic issue in many systems and can distort customer ranking and segmentation. This project exposes the issue directly, allowing downstream decisions on whether to exclude, impute, or remediate such records upstream.

---

## How to run the project

All steps can be executed locally using Python and DuckDB.

1. Generate raw data and data quality reports  
   ```text
   python ingestion/ingest.py
   ```


2. Build the analytical models  
   ```text
   python run_models.py
   ```


3. Run analytical queries and generate results
   ```text
   python run_analysis.py
   ```


## Project outputs

After running the full pipeline, the following artifacts are produced.

Raw data:
- data/raw/orders.csv
- data/raw/order_events.csv

Processed and analytical outputs:
- data/processed/data_quality_report.csv
- data/processed/failed_samples.csv
- data/processed/warehouse.duckdb
- data/processed/analysis_results.txt
