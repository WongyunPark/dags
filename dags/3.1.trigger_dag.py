import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='trigger_test',
    default_args=args,
    schedule_interval="@daily",
    )

first_job = DummyOperator(
    task_id='first_job',
    dag=dag,
    )

options = ['path_A', 'path_B']

def which_path():
  '''
  return the task_id which to be executed
  '''
  if True:
    task_id = 'path_A'
  else:
    task_id = 'path_B'
  return task_id

check_situation = BranchPythonOperator(
    task_id='check_situation',
    python_callable=which_path,
    dag=dag,
    )

first_job >> check_situation

next_job = DummyOperator(
    task_id='next_job',
    trigger_rule='one_success', 
    dag=dag,
    )


for option in options:
    t = DummyOperator(
        task_id=option,
        dag=dag,
        )
    if option == 'path_B':
        dummy_follow = DummyOperator(
            task_id='follow_' + option,
            dag=dag,
			)
        check_situation >> t >> dummy_follow >> next_job
    else:
        check_situation >> t >> next_job