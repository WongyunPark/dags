from datetime import datetime
from sre_constants import FAILURE
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


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
