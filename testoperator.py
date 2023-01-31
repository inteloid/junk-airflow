from airflow.operators.bash import BashOperator

class TestOperator(BashOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        super().execute(context)
