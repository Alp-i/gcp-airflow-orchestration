
    # Optional: define task dependencies if you have other tasks
    # e.g., create_workflow_invocation depends on create_compilation_result
    # create_compilation_result >> create_workflow_invocation
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator

# ===========================
# Constants - Update these!
# ===========================
PROJECT_ID = "datapipeline-468807"  # Replace with your GCP project ID
REGION = "us-east1"                  # Replace if your Dataform repo is in another region
REPOSITORY_ID = "rep"                # Your Dataform repo ID

# ===========================
# DAG Definition
# ===========================
with DAG(
    dag_id="dataform_wf",
    schedule_interval=None,  # Change to cron if you want automated runs
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task: Create a Dataform Workflow Invocation
    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create_workflow_invocation",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "name": "workflow-schedule"  # <- Replace with your workflow name if different
        },
    )

    # create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
    #    task_id="create-workflow-invocation",
    #    project_id="datapipeline-468807",
    #    region="us-east1",
    #    repository_id="rep",
    #    workflow_invocation={
    #        "compilation_result": "{{ task_instance.xcom_pull('create-compilation-result')['name'] }}"
    #    },
    # )
