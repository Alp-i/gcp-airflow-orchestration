from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import pendulum
# Define Airflow Variables for connections and settings
db_ip = Variable.get("mysql_ip")
connection_url = f"jdbc:mysql://{db_ip}:3306/clothing_db"

# Constants
PROJECT_ID = "datapipeline-468807"
LOCATION = "us-east1"
DATASET_ID = "landingzone"
SOURCE_TABLE = "sales_events"
WATERMARK_TABLE = f"{PROJECT_ID}.{DATASET_ID}.watermark_table"
LANDING_ZONE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.clothing_db_sales_events"

# DAG definition
with models.DAG(
    "dataflow_events_incremental_dag",
    schedule_interval='@once',
    start_date=datetime(2025, 9, 6),
    catchup=False,
    tags=['dataflow', 'incremental'],
) as dag:
    def get_watermark_call(**kwargs):
        ti = kwargs['ti']
        # BigQueryExecuteQueryOperator is a simpler way to pull results
        bq_op = BigQueryInsertJobOperator(
        task_id='get_watermark_task',
        configuration={
            "query": {
                "query": f"SELECT last_processed_timestamp FROM `{WATERMARK_TABLE}` WHERE table_name = '{SOURCE_TABLE}'",
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
        location=LOCATION,
        do_xcom_push=True
    )
        result = bq_op.execute(kwargs)
        last_watermark = result[0][0] if result and result[0] else '1970-01-01 00:00:00'
        ti.xcom_push(key='last_watermark', value=last_watermark)



    get_watermark_pyt= PythonOperator(
        task_id='get_watermark_pyt',
        python_callable=get_watermark_call,
        provide_context=True,
    )

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
                    "password": "54092021Aa!",
                    "query": """SELECT * FROM sales_events WHERE timestamp > '{{ ti.xcom_pull(task_ids="get_watermark_pyt", key="last_watermark") }}'""",
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

    # Task 3: Update the watermark in BigQuery
    update_watermark_task = BigQueryInsertJobOperator(
        task_id="update_watermark",
        configuration={
            "query": {
                "query": f"""
                    MERGE `{WATERMARK_TABLE}` AS T
                    USING (
                      SELECT '{SOURCE_TABLE}' AS table_name, MAX(timestamp) AS new_watermark
                      FROM `{LANDING_ZONE_TABLE}`
                    ) AS S
                    ON T.table_name = S.table_name

                    WHEN MATCHED THEN
                      UPDATE SET last_processed_timestamp = S.new_watermark

                    WHEN NOT MATCHED THEN
                      INSERT (table_name, last_processed_timestamp) VALUES(S.table_name, S.new_watermark);
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',
        location=LOCATION
    )

    # Define the task dependencies
    get_watermark_pyt >> start_flex_template_job >> update_watermark_task