import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': True,
}

dag = DAG(
    dag_id='example_branch_dop_operator_v3',
    schedule_interval='@once',
    default_args=args,
)


def should_run(**kwargs):
    """
    Determine which dummy_task should be run based on if the execution date minute is even or odd.
    :param dict kwargs: Context
    :return: Id of the task to run
    :rtype: str
    """
    print('------------- exec dttm = {} and minute = {}'.
          format(kwargs['execution_date'], kwargs['execution_date'].minute))
    if kwargs['execution_date'].minute % 2 == 0:
        return "dummy_task_1"
    else:
        return "dummy_task_2"


cond = BranchPythonOperator(
    task_id='condition',
    python_callable=should_run,
    dag=dag,
)

dummy_task_1 = DummyOperator(task_id='dummy_task_1', dag=dag)
dummy_task_2 = DummyOperator(task_id='dummy_task_2', dag=dag)
cond >> [dummy_task_1, dummy_task_2]