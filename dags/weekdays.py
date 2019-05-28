

import airflow.utils.dates
from airflow.utils.trigger_rule import TriggerRule
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


def now_weekday(datetime):
    return calendar.day_name[datetime.weekday()][:3]

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
        python_callable=lambda execution_date, **kwargs: print(now_weekday(execution_date)),
        provide_context=True,
    )

    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=lambda execution_date, **kwargs: email_tasks[weekday_person_to_mail[now_weekday(execution_date)]].task_id,
        provide_context=True,
    )

    email_tasks = dict(
        Bob = DummyOperator(task_id='email_bob'),
        Joe = DummyOperator(task_id='email_joe'),
        Alice = DummyOperator(task_id='email_alice'),
    )

    final = BashOperator(
        task_id='final_task',
        bash_command='echo done',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    weekday >> branch >> email_tasks.values() >> final

