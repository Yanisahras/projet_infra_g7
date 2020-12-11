
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_dag_args = {
    'owner': 'groupe7',
    'start_date': datetime(2020, 12, 12, 8),
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '@hourly'
}

dag = DAG(
    dag_id='iabd2_groupe7_data_pipeline_dag',
    default_args=default_dag_args
)

get_albums = BashOperator(
    task_id="get_albums",
    bash_command="mkdir raw_data; mkdir raw_data/playlists; "
                 "python3 /root/airflow/dags/groupe7/projet_infra_g7/data_collect/get_albums.py; "
                 "HADOOP_USER_NAME=groupe7 hdfs dfs -put -f raw_data/playlists /user/groupe7/raw_data/ ",
    dag=dag
)

get_albums