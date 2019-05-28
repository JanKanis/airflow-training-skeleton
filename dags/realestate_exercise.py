

import airflow.utils.dates
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import *

from .airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator


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
        task_id='import sql data',
        sql='''SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}' ''',
        bucket='europe-west1-training-airfl-2be0c9a3-bucket',
        filename='realestate_data/{{ ds }}/properties_{}.json',
        postgres_conn_id='realestate postgres',
    )

