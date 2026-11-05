
# E-Commerce Data Engineering Pipeline

An end-to-end e-commerce data engineering pipeline built on Amazon Web Services (AWS), designed to ingest, transform, catalog, and analyze order data.

The project uses Amazon S3 for data storage, AWS Glue and Apache Spark for data transformations, AWS Glue Crawlers for metadata discovery, and Amazon Athena for SQL-based analytics.

A lightweight Apache Airflow DAG is included to demonstrate workflow orchestration across the AWS services.

## 1. Project Overview

The goal of this project is to build a structured data pipeline that transforms raw e-commerce order data into curated datasets ready for analytical queries and business insights.

The pipeline follows a layered data architecture:

- **Raw Layer:** Stores the original order data in JSON format.
- **Transformation Layer:** Processes raw data using AWS Glue and Apache Spark.
- **Curated Layer:** Stores structured transformed datasets and registers them in the AWS Glue Data Catalog.
- **Analytics Layer:** Provides SQL access to curated datasets through Amazon Athena.

## 2. Architecture

```text
             Orders API
                 |
                 v
             AWS Lambda
              Ingestion
                 |
                 v
          Amazon S3 - Raw
          api_raw/orders/
                 |
                 v
        AWS Glue Raw Crawler
              api_raw
                 |
                 v
       AWS Glue Spark Job
     Ecommerce_Transformation
                 |
                 v
        Amazon S3 - Curated
                 |
                 v
      AWS Glue Curated Crawler
               curated
                 |
                 v
        AWS Glue Data Catalog
             curated_db
                 |
                 v
            Amazon Athena
             SQL Analytics

      Apache Airflow DAG
      Workflow Orchestration
```

## 3. Technologies Used

| Technology | Purpose |
|---|---|
| Amazon S3 | Raw and curated data storage |
| AWS Lambda | API data ingestion |
| AWS Glue | Data transformation using Apache Spark |
| AWS Glue Crawlers | Schema discovery and metadata updates |
| AWS Glue Data Catalog | Centralized metadata management |
| Amazon Athena | SQL analytics |
| Python | Transformation and orchestration logic |
| Apache Airflow | Workflow orchestration |

## 4. Data Ingestion

Order data is retrieved from the Orders API through an AWS Lambda ingestion process.

The incoming records are stored in Amazon S3 in JSON format.

**Raw S3 location:**

```text
s3://samsung-aws-task/api_raw/orders/
```

The raw layer preserves the original ingested data before transformation.

## 5. Data Transformation

The transformation process is implemented using an AWS Glue Spark job.

**Glue Job:**

`Ecommerce_Transformation`

The job processes the raw order data and generates structured datasets for the curated layer.

The transformation logic is maintained in the AWS Glue job script.

## 6. Data Cataloging

AWS Glue Crawlers are used to discover dataset schemas and register metadata in the Glue Data Catalog.

The pipeline uses two crawler stages:

| Crawler | Responsibility |
|---|---|
| `api_raw` | Discovers the raw order data structure |
| `curated` | Discovers the transformed datasets |

The curated database referenced by the project is:

`curated_db`

The task documentation identifies analytical tables including:

- `dim_customer`
- `dim_date`
- `fact_order`
- `fact_order_detail`
- `dim_payment_method`
- `dim_shipper`

The exact available tables depend on the output produced by the Glue transformation job and discovered by the crawler.

## 7. Data Analytics with Amazon Athena

Amazon Athena is used to query the curated datasets through SQL without managing database infrastructure.

The task includes business analysis and data-quality requirements such as:

- Identifying duplicate customer identifiers.
- Checking missing product identifiers and surrogate keys.
- Reconciling sales totals.
- Analyzing monthly business metrics.
- Identifying top-performing products.
- Ranking customers based on their activity.
- Analyzing payment method distribution.
- Evaluating shipping performance.

These represent the analytical requirements described in the task documentation. Individual checks should only be considered implemented after their SQL queries have been executed and validated.

## 8. Workflow Orchestration

A lightweight Apache Airflow DAG is included to coordinate the execution of the AWS pipeline.

The DAG is not the main focus of the project. It serves as an orchestration layer that manages dependencies between existing AWS resources.

### DAG Execution Flow

| Order | Task | Description |
|---|---|---|
| 1 | `check_raw_orders` | Checks whether raw order files exist in S3. |
| 2 | `validate_aws_resources` | Validates the required AWS resources and configuration. |
| 3 | `run_raw_crawler` | Starts the raw Glue Crawler. |
| 4 | `run_glue_transformation` | Executes the `Ecommerce_Transformation` Glue job. |
| 5 | `run_curated_crawler` | Starts the curated Glue Crawler. |
| 6 | `validate_curated_tables` | Runs Athena smoke tests against the expected curated tables. |

The DAG is provided as a standalone orchestration script. The Airflow runtime and deployment environment are not part of this repository's main project scope.

## 9. Data Quality and Validation

The pipeline includes an initial Athena validation stage that checks whether the expected curated tables can be queried.

Further data-quality validation can cover:

- Duplicate records.
- Null and missing keys.
- Referential integrity.
- Sales reconciliation.
- Row-count comparisons between processing stages.

A successful table-access smoke test does not replace comprehensive data-quality testing.

## 10. Security Considerations

The AWS services require appropriate IAM permissions to access the relevant resources.

Important security practices include:

- Use IAM roles and policies with least-privilege access.
- Keep AWS credentials outside the source code.
- Never commit access keys, secret keys, or local environment files to GitHub.
- Configure separate permissions for Airflow orchestration and the AWS Glue job execution role.

## 11. Potential Improvements

- Implement comprehensive automated data-quality checks.
- Add pipeline failure notifications.
- Introduce scheduled execution based on the ingestion frequency.
- Improve incremental processing and partition management.
- Add automated tests for transformation logic.
- Introduce CI/CD for pipeline deployment.
- Improve monitoring and operational observability.

## Project Scope

The primary focus of this project is AWS-based data engineering: data ingestion, storage, Spark transformations, metadata management, and SQL analytics.

Apache Airflow is included only as a supporting orchestration component through a DAG script. It is not a separate Airflow infrastructure project.

---

*Cloud Platform:* Amazon Web Services (AWS)  
*Data Processing:* AWS Glue / Apache Spark  
*Storage:* Amazon S3  
**Analytics:** Amazon Athena  
**Orchestration:** Apache Airflow (DAG script)
=======
# AWS_Ecommerce_Data_Pipeline
>>>>>>> f6abc747496c96e16a249bd4cce635c5584bc7cc
