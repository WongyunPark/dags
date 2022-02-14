
from tracemalloc import start
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

args = {'owner': 'airflow', 'start_date': days_ago(n=1)}

dag  = DAG(dag_id='3_sj_xcom_dag',
           default_args=args,
           schedule_interval='@once')

#def push(**context):
#    start_value=
#    context["task_instance"].xcom_push(key="start_key", value=start_value)

#def pull(**context):
#    start_value = context['task_instance'].xcom_pull(
#        task_ids='start_task', key="start_key"
#    )
#    print(start_value)
    #return model_id



xcom_push = BashOperator(
    task_id='start_task',
    bash_command="echo success",
    xcom_push=True,
    dag=dag,
)

xcom_pull = BashOperator(
    task_id = 'fetching_data',
    bash_command="echo 'XCom fetched: {{ ti.xcom_pull(task_ids=[\'start_task\']) }}'",
    xcom_push=False,
    dag = dag,
)



xcom_push >> xcom_pull