from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.operators.dummy import DummyOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from datetime import datetime, timedelta

PROJECT_ID = 'epicfuely777'
REGION = 'us-east4'
DATAPROC_REGION = 'europe-west1'
CLUSTER_NAME = 'ecommerce-batch-cluster'
DATAFLOW_TEMPLATE = 'gs://epicfuely777-dataflow-staging/templates/streaming'
JAR_PATH = 'gs://epicfuely777-dataflow-staging/jars/ecommerce-batch_2.12-1.0.jar'
BQ_TABLE = f'{PROJECT_ID}:raw.events'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_pipeline',
    schedule_interval='0 2 * * *',
    default_args=default_args,
    catchup=False,
    description='E-commerce streaming and batch pipeline'
) as dag:

    start = DummyOperator(task_id='start')

    start_streaming = DataflowStartFlexTemplateOperator(
        task_id='start_streaming_job',
        project_id=PROJECT_ID,
        location=REGION,
        body={
            'launchParameter': {
                'jobName': 'ecommerce-streaming-{{ ds_nodash }}',
                'containerSpecGcsPath': DATAFLOW_TEMPLATE,
                'parameters': {
                    'inputSubscription': f'projects/{PROJECT_ID}/subscriptions/ecommerce-events-sub',
                    'outputTable': BQ_TABLE
                }
            }
        }
    )

    spark_job = {
        'reference': {'projectId': PROJECT_ID},
        'placement': {'clusterName': CLUSTER_NAME},
        'sparkJob': {
            'mainClass': 'com.ecommerce.DailyAggregator',
            'jarFileUris': [JAR_PATH]
        }
    }

    submit_spark = DataprocSubmitJobOperator(
        task_id='submit_spark_job',
        project_id=PROJECT_ID,
        region=DATAPROC_REGION,
        job=spark_job
    )

    ge_validation = GreatExpectationsOperator(
        task_id='validate_raw_events',
        data_context_root_dir='/home/airflow/gcs/data/great_expectations',
        expectation_suite_name='ecommerce.suite',
        batch_kwargs={
            'table': 'events',
            'datasource': 'bigquery'
        }
    )

    end = DummyOperator(task_id='end')

    start >> start_streaming >> submit_spark >> ge_validation >> end
