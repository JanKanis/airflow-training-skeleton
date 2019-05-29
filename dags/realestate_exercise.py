

import airflow.utils.dates
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow_training.operators.http_to_gcs import HttpToGcsOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import *



args = dict(
    owner = "Jan",
    start_date = airflow.utils.dates.days_ago(3),
    #schedule_interval=None,
)

dag = airflow.DAG(
    dag_id = 'realestate',
    default_args = args,
    schedule_interval="0 0 * * *",
)


with dag:
    pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
        task_id='import_sql_data',
        sql='''SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}' ''',
        bucket='europe-west1-training-airfl-2be0c9a3-bucket',
        filename='realestate_data/{{ ds }}/properties_{}.json',
        postgres_conn_id='realestate postgres',
    )

    http_to_gcs = HttpToGcsOperator(
        task_id = 'load_values',
        endpoint='airflow-training-transform-valutas',
        data= { 'date': '{{ ds }}', 'from': 'GBP', 'to': 'EUR'},
        bucket='europe-west1-training-airfl-2be0c9a3-bucket',
        filename='realestate_pound_rates/{{ ds }}/airflow-training-transform-valutas.json',
        google_cloud_storage_conn_id='cloud_function_valutas',
    )


