from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

args = {'owner': 'airflow', 'start_date': days_ago(n=1)}

dag  = DAG(dag_id='114_xcom',
           default_args=args,
           schedule_interval='@once')

downloading_data = BashOperator(
    task_id='downloading_data',
    bash_command='echo "Hello, I am a value!"',
    xcom_push=True,
    dag=dag,
)

fetching_data = BashOperator(
    task_id='fetching_data',
    bash_command="echo 'XCom fetched: {{ ti.xcom_pull(task_ids=[\'downloading_data\']) }}'",
    xcom_push=False,
    dag=dag,
)

downloading_data >> fetching_data