from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
import sys
import json


class TestOperator(SparkSubmitOperator):
    def __init__(self, notebook: str, **kwargs) -> None:
        self.convertNotebook(notebook)
        kwargs['task_id'] ='demo-spark-app-id';
        kwargs['conn_id'] ='spark_k8s';
        kwargs['application'] ='/tmp/__job.py';
        kwargs['driver_class_path'] ='/opt/airflow/';
        kwargs['jars'] ='/opt/airflow/aws-java-sdk-bundle-1.12.376.jar,/opt/airflow/hadoop-aws-3.3.4.jar';
        kwargs['name'] ='spark-on-eks-example';
        kwargs['conf'] = {
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
        };
        kwargs['verbose'] = True;
        kwargs['application_args'] = ['s3a://test-bucket/input.csv', 's3a://test-bucket/result/spark'];
        kwargs['env_vars'] = {
            'KUBECONaFIG': 'kube_config_path'
        };
        super().__init__(**kwargs)

    def execute(self, context):
        super().execute(context)
        
    def convertNotebook(self, notebook):
        print('# file: %s' % notebook)
        print('# vi: filetype=python')
        print('')
        code = json.load(open(notebook))

        pyFileContent = """from pyspark.sql import SparkSession; \n
spark = SparkSession.builder.appName('job-for-pi').getOrCreate(); \n
"""
        for cell in code['cells']:
                # print('# -------- code --------', cell['cell_type'])
                paragraphCode = ""
                for line in cell['source']:
                    paragraphCode = paragraphCode + line + "\n"
                pyFileContent = pyFileContent + 'spark.sql("""' + "\n" +paragraphCode + "\n" + '""");\n'
        f = open("/tmp/__job.py", "w")
        f.write(pyFileContent)
        f.close()
