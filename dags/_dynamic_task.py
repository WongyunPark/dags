import uuid
import airflow
from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator

def _train_model(**context):
    model_id = str(uuid.uuid4())
    context["task_instance"].xcom_push(key="model_id", value=model_id)


def _deploy_model(**context):
    model_id = context["task_instance"].xcom_pull(
        task_ids="train_model", key="model_id"
    )
    print(f"Deploying model {model_id}")


with DAG(
    dag_id="_dynamic_task",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:
    start = DummyOperator(task_id="start")

    fetch_sales = DummyOperator(task_id="fetch_sales")
    clean_sales = DummyOperator(task_id="clean_sales")

    fetch_weather = DummyOperator(task_id="fetch_weather")
    clean_weather = DummyOperator(task_id="clean_weather")

    join_datasets = DummyOperator(task_id="join_datasets")

    train_model = PythonOperator(task_id="train_model", python_callable=_train_model, provide_context=True,)

    deploy_model = PythonOperator(task_id="deploy_model", python_callable=_deploy_model, provide_context=True,)

    start >> [fetch_sales, fetch_weather]
    fetch_sales >> clean_sales
    fetch_weather >> clean_weather
    [clean_sales, clean_weather] >> join_datasets
    join_datasets >> train_model >> deploy_model


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 2, 7)
}

number_of_dags = Variable.get('dag_number', default_var=3)
number_of_dags = int(number_of_dags)

def create_dag(dag_id,
               schedule,
               dag_number,
               default_args):

    def hello_world_py(*args):
        print('Hello World')
        print('This is DAG: {}'.format(str(dag_number)))
        
    def sucess_py(*args):
        print("성공했습니다.")
    
    def failure_py(*args):
        print("실패했습니다.")
        

    with DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=schedule) as dag:
        
        start =  DummyOperator(
        task_id='start',
        trigger_rule='all_success'
        )
        
        all_sucess =  DummyOperator(
        task_id='all_sucess',
        trigger_rule='all_success'
        )

        one_sucess = DummyOperator(
        task_id='one_sucess',
        trigger_rule='one_success',
        dag=dag,
        )

        all_failed = DummyOperator(
        task_id='all_failed',
        trigger_rule='all_failed',
        dag=dag,
        )
        
        task_1 = PythonOperator(
            task_id='task_sucess',
            python_callable=hello_world_py,
            dag=dag,
            on_success_callback=sucess_py,
        )

        task_2 = BashOperator(
            task_id='task_fail',
            bash_command="ls fake_dir",
            dag=dag,
            on_failure_callback=failure_py,
        )
    
    start >> task_1 >> [all_sucess, one_sucess, all_failed]
    start >> task_2 >> [all_sucess, one_sucess, all_failed]

    return dag
    

for n in range(number_of_dags):
    dag_id = 'trigger_dag'.format(str(n+1))

    schedule = '@daily'
    dag_number = n
    globals()[dag_id] = create_dag(dag_id,
                                  schedule,
                                  dag_number,
                                  default_args)


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