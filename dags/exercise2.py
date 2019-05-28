

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
    dag_id = 'exercise2b',
    default_args = args,
)

with dag:
    print_execution_date = BashOperator(
        task_id="print_execution_date", bash_command="echo {{ execution_date }}",
    )

    wait_tasks = [
        BashOperator(task_id=f'wait_{i}', bash_command=f'sleep {i}') for i in [1,5,10]
    ]

    the_end = DummyOperator(
        task_id='the_end'
    )

    print_execution_date >> wait_tasks >> the_end

