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
        application='local:///opt/spark/jars/spark-on-eks-example-assembly-v1.0.jar',
        name='spark-on-eks-example',
        java_class='ExampleApp',
        conf={
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
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

