

import airflow.utils.dates
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


args = dict(
    owner = "Jan",
    start_date = airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)


dag = airflow.DAG(
    dag_id = 'exercise2-try',
    default_args = args,
)

with dag:
    print_execution_date = BashOperator(
        task_id="print_execution_date", bash_command="echo {{ execution_date }}",
    )

    wait_5 = BashOperator(
        task_id='wait_5',
        bash_command='sleep 5',
    )

    wait_1 = BashOperator(
        task_id='wait_1',
        bash_command='sleep 1',
    )

    wait_10 = BashOperator(
        task_id='wait_10',
        bash_command='sleep 10',
    )

    the_end = DummyOperator(
        task_id='the_end'
    )

    print_execution_date >> [wait_1, wait_5, wait_10] >> the_end

