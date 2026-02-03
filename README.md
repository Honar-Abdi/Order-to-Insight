# Order-to-Insight
End-to-end data pipeline from raw events to business insights

🇫🇮 **Suomenkielinen tiivistelmä:** [README.fi.md](README.fi.md)

## TL;DR

This project demonstrates an end-to-end data workflow that combines data engineering and data analytics.  
It shows how raw, imperfect event and transaction data is ingested, validated, transformed, modeled, and analyzed to support business decision-making.

The focus is on SQL-centric analytics, realistic data engineering practices, and explicit reasoning about what the data does and does not support.

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
5. Communicate results clearly, including assumptions, limitations, and risks.

This is not a machine learning project. The emphasis is on data foundations, correctness, and interpretability.

This project is intended as a learning and portfolio project for data analytics and analytics engineering roles.

---

## Data overview

The project works with a combination of event data and transactional data.

Event data represents system-level events such as order creation, payment confirmation, shipment updates, and cancellations.

Transactional data represents business-level facts such as orders, order values, order status, and customer identifiers.

The data is synthetically generated, but designed to be structurally realistic and to include typical issues such as missing events, inconsistent timestamps, duplicate identifiers, and partial records.

---

## End-to-end data pipeline

The data pipeline follows a clear and explicit structure:

1. Data ingestion  
   Raw data is generated or loaded using Python and written to disk in its original form without modification.

2. Data validation and enrichment  
   Basic data quality rules are applied to identify missing, inconsistent, or suspicious records.  
   Selected data quality issues are propagated into the modeled layer as explicit flags.

3. Data transformation and modeling  
   Raw data is transformed into analytical models using SQL.  
   This includes joining events and transactions, normalizing timestamps, and constructing fact-style tables suitable for analysis.

4. Analysis and insights  
   SQL-based analyses are used to answer business questions such as revenue trends, order performance, event coverage, and operational timing metrics.

5. Interpretation and documentation  
   Results are interpreted with a focus on assumptions, limitations, and potential misinterpretations.

DuckDB is used as the analytical database to keep the project fully local, reproducible, and SQL-centric, while still supporting realistic analytical workloads.

---

## Repository structure

The repository is organized to reflect the logical stages of the data pipeline.

- The `ingestion` directory contains Python scripts responsible for generating or loading raw data and running data quality checks.
- The `data` directory contains raw and processed datasets, clearly separated to avoid accidental mixing.
- The `transformations` directory contains SQL models used to clean, transform, and model the data.
- The `analysis` directory contains SQL queries used to produce analytical insights.
- The `ai` directory documents how AI tools were used as a secondary reviewer during the project.
- This README provides context, explanations, and conclusions.

---

## Use of AI tools

AI tools were used in a limited and transparent way.

They were applied primarily as a second analyst to review assumptions, suggest validation checks, and challenge analytical interpretations.

AI was not used to automatically generate results or replace human decision-making. All final modeling and analytical decisions were made explicitly and documented.

---

## What was learned

Through this project, the following key lessons emerged:

- Reliable analysis depends more on data quality and modeling choices than on complex algorithms.
- Seemingly simple aggregations can lead to incorrect conclusions if event timing and data completeness are not handled carefully.
- Clear separation between raw data, transformed data, and analysis greatly improves both correctness and maintainability.
- Documenting assumptions and limitations is essential for responsible data work.

---

## What would be done differently in production

In a production environment, this project would be extended with automated data quality monitoring, alerting, and scheduling.

Data ingestion and transformations would be orchestrated using a workflow management tool.

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

This section summarizes analytical results produced from the modeled data layer.  
All results are generated by running the SQL queries in `analysis/insights.sql` against the DuckDB warehouse and written to `data/processed/analysis_results.txt`.

**Note:** The numerical values shown below correspond to a concrete example run with `n = 20,000` orders.  
The pipeline is parameterized, and results scale consistently with different dataset sizes.

### Business-level snapshot

The dataset contains **20,000 orders** in total.

Completed orders: **17,848**  
Cancelled orders: **1,600**  
Refunded orders: **552**

Total completed revenue: **2,638,001.00**  
Average order value for completed orders: **147.80**

**Interpretation:**  
The majority of orders are completed (~89%), with cancellation and refund rates consistent with realistic operational systems.  
The completed revenue and average order value provide a stable baseline for trend analysis and downstream reporting.

---

### Daily revenue and order volume

Daily order volume remains stable at approximately **205–206 orders per day**, with completed orders typically ranging between **175 and 195 per day**.

Daily completed revenue fluctuates within a narrow band, mostly between **25,000 and 29,000**, with the final day showing a partial count due to an incomplete ingestion window.

**Interpretation:**  
The absence of unexpected spikes or gaps indicates that the temporal aggregation logic is correct and that event-to-order alignment is functioning as intended.

---

### Data quality signals in the modeled layer

The fact table includes explicit data quality flags derived during modeling.

Completed orders missing a payment confirmation event: **477**  
Cancelled orders that still have a shipment event: **31**

**Interpretation:**  
These inconsistencies demonstrate why transactional status alone cannot be treated as ground truth.  
Missing payment events for completed orders introduce uncertainty for funnel analysis, lead-time calculations, and financial reconciliation.  
By surfacing these issues explicitly in the modeled layer, the pipeline enables informed analytical decisions rather than silently masking data quality risks.

---

### Event coverage across orders

Orders with `order_created` event: **100.00%**  
Orders with `payment_confirmed` event: **89.55%**  
Orders with `order_shipped` event: **89.40%**  
Orders with `order_cancelled` event: **8.00%**

**Interpretation:**  
Not all completed orders have corresponding payment or shipment events.  
Analyses relying solely on event data would therefore undercount completed orders and revenue.  
This project makes such coverage gaps visible instead of allowing them to distort aggregated metrics.

---

### Operational timing example

Average time from payment confirmation to shipment: **237 minutes**  
Minimum: **237 minutes**  
Maximum: **237 minutes**

**Interpretation:**  
The constant lead time reflects the fixed offsets used in the synthetic data generator.  
In real production data this metric typically varies and becomes a meaningful operational KPI, but it is also highly sensitive to missing or misordered events.

---

### Notable edge case: missing customer identifiers

The top customers by completed revenue include records with a missing `customer_id`.

**Interpretation:**  
Missing customer identifiers are a realistic issue in many transactional systems and can distort customer ranking, segmentation, and lifetime value calculations.  
By exposing this issue directly in the analysis, the project highlights the need for either upstream remediation or explicit downstream handling.

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
