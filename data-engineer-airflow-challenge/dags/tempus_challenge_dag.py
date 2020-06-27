from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import challenge as c

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 24),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Object
dag = DAG(
    'tempus_challenge_dag',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # DAG will run daily at 00:00UTC
    catchup=False,
)


get_news_sources = PythonOperator(
    task_id='get_news_sources',
    provide_context=True,
    python_callable=c.NewsAPI.callable,
    params={"language": "en"},
    dag=dag
)



end = DummyOperator(
    task_id='end',
    dag=dag
)

# A visual representation of the following should be viewable at:
# http://localhost:8080/admin/airflow/graph?dag_id=sample_dag
# >> and << operators sets upstream and downstream relationships
# print_date_task is downstream from print_context_task.
# In other words, print_date_task will run after print_context_task
get_news_sources.set_downstream(end)
# print_date_task is upstream from end
# In other words, print_date_task will run before end

