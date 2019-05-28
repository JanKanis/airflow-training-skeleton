import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id="hello_airflow",
    default_args={
        "owner": "godatadriven",
        "start_date": airflow.utils.dates.days_ago(3),
    },
)


with dag:
    BashOperator(
        task_id="print_exec_date_tweak", bash_command="echo hello world on {{ execution_date }}",
    )
