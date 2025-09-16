from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.models import Variable

db_ip = Variable.get("mysql_ip")
connection_url = f"jdbc:mysql://{db_ip}:3306/clothing_db"
db_password = Variable.get("mysql_password")
PROJECT_ID = "datapipeline-468807"
LOCATION = "us-east1"


BODY = {
    "launch_parameter": {
        "jobName": "df-customers-table",
        "containerSpecGcsPath": "gs://dataflow-templates-us-east1/latest/flex/MySQL_to_BigQuery",
        "parameters": {
            "connectionURL": connection_url,
            "username": "root",
            "password": db_password,
            "query": "SELECT * FROM customers WHERE load_timestamp > (SELECT last_processed_timestamp FROM watermarks WHERE table_name='customers');",
            "outputTable": "datapipeline-468807:landingzone.clothing_db_customers_copy",
            "bigQueryLoadingTemporaryDirectory": "gs://lcw-dataflow-temp-bucket",
            "useColumnAlias": "false",
            "isTruncate": "false",
            "partitionColumnType": "long",
            "fetchSize": "50000",
            "createDisposition": "CREATE_NEVER",
            "useStorageWriteApi": "false",
            "stagingLocation": "gs://dataflow-staging-us-east1-377358662798/staging",
            "autoscalingAlgorithm": "NONE",
            "serviceAccount": "377358662798-compute@developer.gserviceaccount.com",
            "labels": "{\"goog-dataflow-provided-template-version\":\"2025-08-26-00_rc00\",\"goog-dataflow-provided-template-name\":\"mysql_to_bigquery\",\"goog-dataflow-provided-template-type\":\"flex\"}"
        },
        "environment": {
            "numWorkers": 2,
            "tempLocation": "gs://dataflow-staging-us-east1-377358662798/tmp",
            "additionalExperiments": ["use_runner_v2"],
            "additionalUserLabels": {}
        }
    }
}

start_flex_template_job = DataflowStartFlexTemplateOperator(
        task_id="start_flex_template_customer_job",
        project_id=PROJECT_ID,
        body=BODY,
        location=LOCATION,
        append_job_name=False,
        wait_until_finished=True,  # Non-deferrable: DAG waits until job finish
    )

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

update_watermark_task = SQLExecuteQueryOperator(
        task_id="update_watermark_customer",
        conn_id="mysql_conn",
        sql="""
            INSERT INTO watermarks (table_name, last_processed_timestamp)
            VALUES ('customers', UTC_DATE() + INTERVAL 4 HOUR) ON DUPLICATE KEY
            UPDATE
                last_processed_timestamp = UTC_DATE() + INTERVAL 4 HOUR;
            """
    )



