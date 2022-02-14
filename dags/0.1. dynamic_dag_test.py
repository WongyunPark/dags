from datetime import datetime
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

    with DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=schedule) as dag:
        
        start =  DummyOperator(
        task_id='start',
        trigger_rule='all_success'
        )
        
        end =  DummyOperator(
        task_id='end',
        trigger_rule='all_success'
        )
        
        task_1 = PythonOperator(
            task_id='pythonoperator',
            python_callable=hello_world_py,
            dag=dag,
        )

        task_2 =  BashOperator(
            task_id='bashoperator',
            bash_command='echo 1',
            dag=dag,
        )
    
    start >> [task_1, task_2] >> end

    return dag
    

for n in range(number_of_dags):
    dag_id = 'dynamic_dag_test_{}'.format(str(n+1))

    schedule = '@daily'
    dag_number = n
    globals()[dag_id] = create_dag(dag_id,
                                  schedule,
                                  dag_number,
                                  default_args)