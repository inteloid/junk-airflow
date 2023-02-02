from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from testoperator import TestOperator

with DAG(
	"TestReport",
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
	    task_id='demo-spark-app-id',
	    conn_id='spark_k8s', # Set connection details in the airflow connections. Connection string: k8s://https://<k8s-master-host>:443/?queue=root.default&deploy-mode=cluster
	    application='./dags/repo/pi.py',
	    driver_class_path='/opt/airflow/',
	    jars='/opt/airflow/aws-java-sdk-bundle-1.12.376.jar,/opt/airflow/hadoop-aws-3.3.4.jar',
	    name='spark-on-eks-example',
	    notebook='/opt/airflow/dags/repo/report.ipynb',
	    conf={
	    	'spark.kubernetes.container.image': 'harbor.intent.ai/library/spark-3.3.1-pyspark:v12',
	    	'spark.kubernetes.authenticate.driver.serviceAccountName': 'harut',
	    	'spark.kubernetes.authenticate.executor.serviceAccountName': 'harut',
	    	'spark.kubernetes.namespace': 'engineering-harut',
	    	'spark.kubernetes.file.upload.path':'s3a://jars/',
	    	'spark.hadoop.fs.s3a.access.key': 'zDdnekZrHIyltouc',
	    	'spark.hadoop.fs.s3a.secret.key': '4ZbbjxGjHWpTCnqVaOJDusc70cIn',
	    	'spark.hadoop.fs.s3a.endpoint': 'http://10.1.51.44:30381',
	    	'spark.hadoop.fs.s3a.path.style.access': 'true',
	    	'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
	    	'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
	    	'spark.executor.instances': 3,
	    	'spark.rpc.askTimeout': 36000
	    },
	    verbose=True,
	    application_args=['s3a://test-bucket/input.csv', 's3a://test-bucket/result/spark'],
	    env_vars={
	        'KUBECONaFIG': 'kube_config_path'
	    }
	)
