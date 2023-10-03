from datetime import timedelta,datetime
from pathlib import Path
from airflow import DAG
#from airflow.operators.python_operator import PythonOperator    #no me funciona este import, asi que hago el import con la linea siguiente
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
from  Youtube_ETL import get_top_videos, conectar_Redshift, insert_data, send_email
dag_path = os.getcwd()      #para obtener el directorio actual de trabajo (current working directory, "CWD") y almacenarlo en la variable dag_path
default_args = {
    'owner': 'ChristianRivasN',
    'email': 'christian.jrivasn@gmail.com',
    'start_date': datetime(2023, 9, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5,
    )
}

ingestion_dag = DAG(
    dag_id='ingestion_data',
    default_args=default_args,
    description='Agrega datos de los 10 videos mas vistos en Youtube',
     schedule_interval=timedelta(days=1),
    catchup=False
)


task_1 = PythonOperator(
    task_id='extract_and_transform_data',
    python_callable=get_top_videos,
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id='load_redshift_connection',
    python_callable=conectar_Redshift,
    dag=ingestion_dag,
)

task_3 = PythonOperator(
    task_id='insert_data_to_redshift',
    python_callable=insert_data,
    dag=ingestion_dag,
)

task_4 = PythonOperator(
    task_id='send_mail_process_successfully_completed',
    python_callable=send_email,
    dag=ingestion_dag,
)



task_1 >> task_2 >> task_3 >> task_4