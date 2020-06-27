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

dag = DAG(
    'tempus_challenge_dag',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # DAG will run daily at 00:00UTC
    catchup=False,
)

get_en_news_sources = PythonOperator(
    task_id='get_en_news_sources',
    provide_context=True,
    python_callable=c.get_sources,
    params={'language': 'en'},
    dag=dag
)

save_headlines = PythonOperator(
    task_id='save_headlines',
    provide_context=True,
    python_callable=c.save_headlines,
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

get_en_news_sources.set_downstream(save_headlines)
save_headlines.set_downstream(end)
