from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
import sys
import json


class TestOperator(SparkSubmitOperator):
    def __init__(self, name: str, notebook: str, **kwargs) -> None:
        self.convertNotebook(notebook)
        super().__init__(**kwargs)

    def execute(self, context):
        super().execute(context)
        
    def convertNotebook(self, notebook):
        print('# file: %s' % notebook)
        print('# vi: filetype=python')
        print('')
        code = json.load(open(notebook))

        pyFileContent = ""
        for cell in code['cells']:
                # print('# -------- code --------', cell['cell_type'])
                for line in cell['source']:
                    pyFileContent = pyFileContent + 'spark.sql("""' + "\n" +line + "\n" + '""")'
                    print(line, end='')
                print('\n')
        f = open("__job.py", "w")
        f.write(pyFileContent)
        f.close()