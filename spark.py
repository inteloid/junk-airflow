import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',    
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    #'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_spark = DAG(
        dag_id = "sparkoperator_demo",
        default_args=default_args,
        # schedule_interval='0 0 * * *',
        schedule_interval='@once',	
        dagrun_timeout=timedelta(minutes=60),
        description='use case of sparkoperator in airflow',
        start_date = airflow.utils.dates.days_ago(1)
)

spark_submit = SparkSubmitOperator(
        task_id='demo-spark-app-id',
        conn_id='spark_k8s', # Set connection details in the airflow connections. Connection string: k8s://https://<k8s-master-host>:443/?queue=root.default&deploy-mode=cluster
        application='./dags/repo/pi.py',
        name='spark-on-eks-example',
        conf={
		'spark.kubernetes.container.image': 'harbor.intent.ai/library/spark-3.3.1-pyspark:latest',
		'spark.kubernetes.authenticate.driver.serviceAccountName': 'spark',
		'spark.kubernetes.authenticate.executor.serviceAccountName': 'spark',
		'spark.kubernetes.file.upload.path':'s3a://jars/',
		'spark.driver.extraJavaOptions': '"-Divy.cache.dir: /tmp -Divy.home: /tmp"',
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
        },
	dag=dag_spark
    )

spark_submit

if __name__ == "__main__":
    dag_spark.cli()

