from airflow.operators.bash import BashOperator

class TestOperator(BashOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(
            task_id="run_after_loop",
	        bash_command="echo 1",
            **kwargs)

    def execute(self, context):
        super(context)
