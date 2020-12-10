
IntelliJ IDEAPyCharm
import datetime as dt
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from apispotify import get_data

now = dt.datetime.now() #- dt.timedelta(days=7)
current_time = now.strftime('%Y%m%d')

default_args = {
    'owner':"Group7",
    'start_date': datetime(2020, 12, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False
}

# Create dag
dag = DAG ( 'dag_groupe7',
            default_args=default_args,
            schedule_interval='45 12 * * *'
            )

with dag:


    mkdir = BashOperator(
        task_id = "mkdir",
        bash_command = "mkdir /tmp/data_groupe7",
    )

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=get_data.launch,
        op_kwargs={
            'path':"/tmp/data_groupe7"
        }
    )

    clean = BashOperator(
        task_id = "clean",
        bash_command = "rm -rf /tmp/data_groupe7",
    )

    mkdir_dist = BashOperator(
        task_id = "mkdir_dist",
        bash_command = "hdfs dfs -mkdir /user/iabd2_group7/data/{}".format(current_time)
    )

submit = BashOperator(
    task_id = "submit",
    bash_command = "spark-submit --deploy-mode cluster --master yarn --class Analyse --executor-memory 512M --executor-cores 1 automatisation_infrastructure-1.0-SNAPSHOT-jar-with-dependencies.jar"
)


submit
