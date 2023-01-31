from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from testoperator import TestOperator

with DAG(
	"basho2",
	default_args={
	        "depends_on_past": False,
	        "email": ["airflow@example.com"],
	        "email_on_failure": False,
	        "email_on_retry": False,
	        "retries": 1,
	        "retry_delay": timedelta(minutes=5),
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
	description="A simple tutorial DAG",
	schedule=timedelta(days=1),
	start_date=datetime(2021, 1, 1),
	catchup=False,
	tags=["example1"],
) as dag:
	run_this = TestOperator(
	    name="asd",
	    task_id="run_after_loop",
	    bash_command="echo 1",
	)
