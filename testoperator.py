from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 

class TestOperator(SparkSubmitOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        super().execute(context)
