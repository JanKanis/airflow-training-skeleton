

import airflow.utils.dates
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow_training.operators.http_to_gcs import HttpToGcsOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import *

from airflow.contrib.operators.dataproc_operator import (DataprocClusterCreateOperator, DataProcPySparkOperator, DataprocClusterDeleteOperator)


args = dict(
    owner = "Jan",
    start_date = airflow.utils.dates.days_ago(3),
    #schedule_interval=None,
)

project_id = 'airflowbolcom-may2829-1e4d09f0'
bucket = 'europe-west1-training-airfl-2be0c9a3-bucket'
realestate_datafiles = 'realestate_data/{{ ds }}/properties_{}.json'
realestate_datafile_full = f'gs://{bucket}/{realestate_datafiles.replace("{}","0")}'
pound_rate_file = 'realestate_pound_rates/{{ ds }}/airflow-training-transform-valutas.json'
pound_rate_file_full = f'gs://{bucket}/{pound_rate_file}'
dataproc_output = f'gs://{bucket}/realestate_dataproc_output/{{ ds }}/output.parquet'


def response_check(response):
    return len(response.text) > 0


dag = airflow.DAG(
    dag_id = 'realestate',
    default_args = args,
    schedule_interval="0 0 * * *",
)


with dag:
    pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
        task_id='import_sql_data',
        sql='''SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}' ''',
        bucket=bucket,
        filename=realestate_datafiles,
        postgres_conn_id='realestate postgres',
    )

    http_to_gcs = HttpToGcsOperator(
        task_id = 'load_values',
        endpoint='airflow-training-transform-valutas',
        data= { 'date': '{{ ds }}', 'from': 'GBP', 'to': 'EUR'},
        bucket=bucket,
        filename=pound_rate_file,
        http_conn_id='cloud_function_valutas',
        response_check=response_check,
        log_response=True,
    )


    cluster_name = "analyse-pricing-{{ ds }}"

    dataproc_create_cluster = DataprocClusterCreateOperator(
        task_id='create_cluster',
        cluster_name=cluster_name,
        project_id=project_id,
        num_workers=2,
        zone="europe-west4-a",
    )

    compute_aggregates = DataProcPySparkOperator(
        task_id='build_statistics',
        main="gs://europe-west1-training-airfl-2be0c9a3-bucket/build_statistics.py",
        cluster_name=cluster_name,
        arguments=[
            realestate_datafile_full,
            pound_rate_file_full,
            dataproc_output,
        ],
    )

    dataproc_delete_cluster = DataprocClusterDeleteOperator(
        task_id='delete_cluster',
        cluster_name=cluster_name,
        project_id=project_id,
    )

    [pgsl_to_gcs, http_to_gcs] >> dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster





