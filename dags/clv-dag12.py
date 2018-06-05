from datetime import datetime
from airflow import DAG

from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 1),
    'email': ['eamon@logistio.ie'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id='lifetime_python_operator6', default_args=default_args,
    schedule_interval=None)

run_this = BashOperator(
    task_id='calculate_bgf',
    provide_context=False,
    bash_command='python /usr/local/airflow/dags/scripts/lifetimes2.py',
    dag=dag)
    
dag >> run_this
