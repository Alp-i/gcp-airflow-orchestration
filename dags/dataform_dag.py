
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator
from airflow.utils.dates import days_ago  # optional helper for default start date


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='dataform_workflow_example',  # unique DAG name
    default_args=default_args,
    description='Example DAG to run a Dataform workflow',
    schedule_interval=None,  # set to None for manual / trigger-only execution
    start_date=days_ago(1),  # required start date
    catchup=False,
    tags=['example'],
) as dag:


    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create-workflow-invocation",
        project_id="datapipeline-468807",
        region="us-east1",
        repository_id="rep",
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create-compilation-result')['name'] }}"
        },
    )

    # Optional: define task dependencies if you have other tasks
    # e.g., create_workflow_invocation depends on create_compilation_result
    # create_compilation_result >> create_workflow_invocation
