import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

branch_var = Variable.get('dag_number', default_var=1)
branch_var = int(branch_var)


def which_path():
  if branch_var>=5:
    task_id = 'path_A'
  else:
    task_id = 'path_B'
  return task_id

dag = DAG(
    dag_id='branch_operator',
    default_args=args,
    schedule_interval="@daily",
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

options = ['path_A', 'path_B'] 

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=which_path,
    dag=dag,
)

start >> branching

end = DummyOperator(
    task_id='end',
    trigger_rule='one_success',
    dag=dag,
)

for option in options:
    t = DummyOperator(
        task_id=option,
        dag=dag,
    )
    dummy_follow = DummyOperator(
        task_id='follow_' + option,
        dag=dag,
    )
    branching >> t >> dummy_follow >> end