from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

# Define Airflow Variables for connections and settings
db_ip = Variable.get("mysql_ip")
connection_url = f"jdbc:mysql://{db_ip}:3306/clothing_db"
db_password = Variable.get("mysql_password")
# Constant
PROJECT_ID = "datapipeline-468807"
LOCATION = "us-east1"
DATASET_ID = "landingzone"
SOURCE_TABLE = "sales_events"
WATERMARK_TABLE = f"{PROJECT_ID}.{DATASET_ID}.watermark_table"
LANDING_ZONE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.clothing_db_events_batch"

# DAG definition
with models.DAG(
    "dataflow_events_incremental_dag",
    schedule_interval='@once',
    start_date=datetime(2025, 9, 6),
    catchup=False,
    tags=['dataflow', 'incremental'],
) as dag:



    start_flex_template_job = DataflowStartFlexTemplateOperator(
        task_id="start_flex_template_job",
        project_id=PROJECT_ID,
        body={
            "launch_parameter": {
                "jobName": f"df-events-table-{datetime.now().strftime('%Y%m%d%H%M%S')}",
                "containerSpecGcsPath": "gs://dataflow-templates-us-east1/latest/flex/MySQL_to_BigQuery",
                "parameters": {
                    "connectionURL": connection_url,
                    "username": "root",
                    "password": db_password,
                    "query": """SELECT * FROM sales_events  WHERE timestamp > (SELECT last_processed_timestamp FROM watermarks WHERE table_name='sales_events');""",
                    "outputTable": LANDING_ZONE_TABLE,
                    "bigQueryLoadingTemporaryDirectory": "gs://lcw-dataflow-temp-bucket",
                    "useColumnAlias": "false",
                    "isTruncate": "false",
                    "partitionColumnType": "long",
                    "fetchSize": "50000",
                    "createDisposition": "CREATE_NEVER",
                    "useStorageWriteApi": "false",
                    "stagingLocation": "gs://dataflow-staging-us-east1-377358662798/staging",
                    "autoscalingAlgorithm": "NONE",
                    "serviceAccount": "377358662798-compute@developer.gserviceaccount.com"
                },
                "environment": {
                    "numWorkers": 2,
                    "tempLocation": "gs://dataflow-staging-us-east1-377358662798/tmp",
                    "additionalExperiments": ["use_runner_v2"],
                    "additionalUserLabels": {}
                }
            }
        },
        location=LOCATION,
        append_job_name=False,
        wait_until_finished=True,
    )

    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    update_watermark_task = SQLExecuteQueryOperator(
        task_id="update_watermark",
        conn_id="mysql_conn",
        sql="""
            INSERT INTO watermarks (table_name, last_processed_timestamp)
            VALUES ('sales_events', UTC_DATE() + INTERVAL 4 HOUR) ON DUPLICATE KEY
            UPDATE
                last_processed_timestamp = UTC_DATE() + INTERVAL 4 HOUR;
            """
    )
#test
start_flex_template_job >> update_watermark_task
