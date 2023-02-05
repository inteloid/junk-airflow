from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from daily_shakes.testoperator import TestOperator

with DAG(
    "DNSReport",
    description="Daily Shakes",
    tags=["app", "shake"],
    default_args={
        "depends_on_past": False,
        "email": ["harut.martirosyan@gmail.com"],
        "email_on_failure": True,
        "email_on_retry": True,
        "email_on_success": True,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "owner_links": {"nvard": "https://airflow.apache.org"},
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    schedule_interval = "@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    ) as dag:
    run_this = TestOperator(
        notebook='/opt/airflow/dags/repo/daily_shakes/report.ipynb',
    )
