

import airflow.utils.dates
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import *

from datetime import date
import calendar


args = dict(
    owner = "Jan",
    start_date = airflow.utils.dates.days_ago(14),
    #schedule_interval=None,
)


dag = airflow.DAG(
    dag_id = 'weekdays',
    default_args = args,
)


def now_weekday():
    my_date = date.today()
    return calendar.day_name[my_date.weekday()][:3]

weekday_person_to_mail = dict(
    Mon='Bob',
    Tue='Joe',
    Wed='Alice',
    Thu='Joe',
    Fri='Alice',
    Sat='Alice',
    Sun='Alice',
)




with dag:

    weekday = PythonOperator(
        task_id='print_weekday',
        python_callable=lambda: print(now_weekday())
    )

    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=lambda: email_tasks[weekday_person_to_mail[now_weekday()]].task_id
    )

    email_tasks = dict(
        Bob = DummyOperator('email_bob'),
        Joe = DummyOperator('email_joe'),
        Alice = DummyOperator('email_alice'),
    )

    final = BashOperator(
        task_id='final_task',
        bash_command='echo done',
    )

    weekday >> email_tasks.values() >> final

