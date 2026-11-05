
from __future__ import annotations

import logging
import os
import time

from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from airflow.providers.amazon.aws.operators.glue_crawler import (
    GlueCrawlerRunOperator,
)


# =========================================================
# 1. PROJECT CONFIGURATION
# =========================================================

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

S3_BUCKET = os.getenv(
    "ECOMMERCE_S3_BUCKET",
    "samsung-aws-task"
)

RAW_PREFIX = os.getenv(
    "ECOMMERCE_RAW_PREFIX",
    "api_raw/orders/"
)

GLUE_JOB_NAME = os.getenv(
    "ECOMMERCE_GLUE_JOB",
    "Ecommerce_Transformation"
)

RAW_CRAWLER_NAME = os.getenv(
    "ECOMMERCE_RAW_CRAWLER",
    "api_raw"
)

CURATED_CRAWLER_NAME = os.getenv(
    "ECOMMERCE_CURATED_CRAWLER",
    "curated"
)

ATHENA_DATABASE = os.getenv(
    "ECOMMERCE_ATHENA_DATABASE",
    "curated_db"
)

ATHENA_OUTPUT = os.getenv(
    "ECOMMERCE_ATHENA_OUTPUT",
    f"s3://{S3_BUCKET}/athena-results/"
)


# =========================================================
# 2. TABLES TO VALIDATE
# =========================================================

REQUIRED_TABLES = [
    "dim_customer",
    "dim_date",
    "fact_order",
    "fact_order_detail",
]


# =========================================================
# 3. DEFAULT ARGUMENTS
# =========================================================

default_args = {

    "owner": "data-engineering",

    "depends_on_past": False,

    "retries": 2,

    "retry_delay": timedelta(minutes=5),

}


# =========================================================
# 4. CHECK RAW DATA IN S3
# =========================================================

def check_raw_orders():

    s3 = S3Hook(
        aws_conn_id=None
    )

    keys = s3.list_keys(
        bucket_name=S3_BUCKET,
        prefix=RAW_PREFIX
    ) or []

    json_files = [
        key
        for key in keys
        if key.lower().endswith(".json")
    ]

    if not json_files:

        raise AirflowException(
            "No JSON files found in "
            f"s3://{S3_BUCKET}/{RAW_PREFIX}"
        )

    logging.info(
        "Raw data check passed. "
        "Found %s JSON files.",
        len(json_files)
    )


# =========================================================
# 5. VALIDATE AWS RESOURCES
# =========================================================

def validate_aws_resources():

    aws_hook = AwsBaseHook(
        aws_conn_id=None,
        client_type="glue",
        region_name=AWS_REGION
    )

    glue = aws_hook.get_conn()

    job = glue.get_job(
        JobName=GLUE_JOB_NAME
    )["Job"]

    raw_crawler = glue.get_crawler(
        Name=RAW_CRAWLER_NAME
    )["Crawler"]

    curated_crawler = glue.get_crawler(
        Name=CURATED_CRAWLER_NAME
    )["Crawler"]

    logging.info(
        "Glue Job found: %s",
        job["Name"]
    )

    logging.info(
        "Raw Crawler found: %s",
        raw_crawler["Name"]
    )

    logging.info(
        "Curated Crawler found: %s",
        curated_crawler["Name"]
    )

    if raw_crawler.get("DatabaseName") != "api_raw":

        raise AirflowException(
            "Raw crawler database must be api_raw."
        )

    if curated_crawler.get("DatabaseName") != ATHENA_DATABASE:

        raise AirflowException(
            "Curated crawler database mismatch. "
            f"Expected {ATHENA_DATABASE}, "
            f"found {curated_crawler.get('DatabaseName')}."
        )


# =========================================================
# 6. ATHENA TABLE VALIDATION
# =========================================================

def validate_curated_tables():

    aws_hook = AwsBaseHook(
        aws_conn_id=None,
        client_type="athena",
        region_name=AWS_REGION
    )

    athena = aws_hook.get_conn()

    for table in REQUIRED_TABLES:

        query = (
            f"SELECT COUNT(*) "
            f"FROM {ATHENA_DATABASE}.{table}"
        )

        response = athena.start_query_execution(

            QueryString=query,

            QueryExecutionContext={
                "Database": ATHENA_DATABASE
            },

            ResultConfiguration={
                "OutputLocation": ATHENA_OUTPUT
            }

        )

        query_id = response["QueryExecutionId"]

        start_time = time.monotonic()

        while True:

            result = athena.get_query_execution(
                QueryExecutionId=query_id
            )

            status = result["QueryExecution"]["Status"]

            state = status["State"]

            if state == "SUCCEEDED":

                query_result = athena.get_query_results(
                    QueryExecutionId=query_id,
                    MaxResults=2
                )

                rows = query_result["ResultSet"]["Rows"]

                count = (
                    rows[1]["Data"][0].get("VarCharValue", "0")
                    if len(rows) > 1
                    else "0"
                )

                logging.info(
                    "Table validation passed: %s | Rows: %s",
                    table,
                    count
                )

                break

            if state in ["FAILED", "CANCELLED"]:

                reason = status.get(
                    "StateChangeReason",
                    "Unknown error"
                )

                raise AirflowException(
                    f"Athena query failed for {table}: {reason}"
                )

            if time.monotonic() - start_time > 1800:

                raise AirflowException(
                    f"Athena query timed out: {table}"
                )

            time.sleep(5)


# =========================================================
# 7. DAG DEFINITION
# =========================================================

with DAG(

    dag_id="ecommerce_data_pipeline",

    description=(
        "E-commerce Orders API pipeline "
        "using AWS Glue, S3 and Athena."
    ),

    default_args=default_args,

    start_date=pendulum.datetime(
        2026, 1, 1, tz="UTC"
    ),

    schedule=None,

    catchup=False,

    max_active_runs=1,

    tags=[
        "ecommerce",
        "aws",
        "glue",
        "s3",
        "athena"
    ],

) as dag:


    # TASK 1
    check_raw_data = PythonOperator(

        task_id="check_raw_orders_in_s3",

        python_callable=check_raw_orders

    )


    # TASK 2
    validate_resources = PythonOperator(

        task_id="validate_aws_resources",

        python_callable=validate_aws_resources

    )


    # TASK 3
    run_raw_crawler = GlueCrawlerRunOperator(

        task_id="run_raw_crawler",

        crawler_name=RAW_CRAWLER_NAME,

        aws_conn_id=None,

        region_name=AWS_REGION,

        wait_for_completion=True,

        poll_interval=15

    )


    # TASK 4
    run_glue_job = GlueJobOperator(

        task_id="run_glue_transformation",

        job_name=GLUE_JOB_NAME,

        aws_conn_id=None,

        region_name=AWS_REGION,

        wait_for_completion=True,

        verbose=True,

        retry_limit=0,

        stop_job_run_on_kill=False,

        job_poll_interval=15

    )


    # TASK 5
    run_curated_crawler = GlueCrawlerRunOperator(

        task_id="run_curated_crawler",

        crawler_name=CURATED_CRAWLER_NAME,

        aws_conn_id=None,

        region_name=AWS_REGION,

        wait_for_completion=True,

        poll_interval=15

    )


    # TASK 6
    validate_athena = PythonOperator(

        task_id="validate_curated_tables",

        python_callable=validate_curated_tables

    )


    # =====================================================
    # PIPELINE EXECUTION ORDER
    # =====================================================

    (
        check_raw_data
        >> validate_resources
        >> run_raw_crawler
        >> run_glue_job
        >> run_curated_crawler
        >> validate_athena
    )
