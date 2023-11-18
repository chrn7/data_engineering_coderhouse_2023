from datetime import timedelta,datetime
from pathlib import Path
from airflow import DAG
#from airflow.operators.python_operator import PythonOperator    #no me funciona este import, asi que hago el import con la linea siguiente
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
from  Youtube_ETL import get_top_videos, connect_to_Redshift, insert_data, verify_max_threshold, send_email
dag_path = os.getcwd()      #para obtener el directorio actual de trabajo (current working directory, "CWD") y almacenarlo en la variable dag_path
default_args = {
    'owner': 'ChristianRivasN',
    #'email': 'christian.jrivasn@gmail.com',
    'start_date': datetime(2023, 9, 30),
    'retries': 4,                         # 4 reintentos si falla el proceso
    'retry_delay': timedelta(minutes=5   # 5 minutos de espera antes de cualquier re intento
    )
}

ingestion_dag = DAG(
    dag_id='Pipeline_ingestion_data_from_Youtube_API',
    default_args=default_args,
    description='Adds data of the Top 10 videos with more views from Youtube',
    schedule_interval='@daily',          
    #schedule_interval=timedelta(days=1),     # Se puede poner el schedule_interval de esta forma o la de la anterior linea, es lo mismo
    catchup=False
)


task_1 = PythonOperator(
    task_id='extract_from_Youtube_API_and_transform_data',
    python_callable=get_top_videos,
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id='connect_to_Redshift_and_create_table_if_not_exist',
    python_callable=connect_to_Redshift,
    dag=ingestion_dag,
)

task_3 = PythonOperator(
    task_id='insert_data_to_Redshift_Table',
    python_callable=insert_data,
    dag=ingestion_dag,
)

task_4 = PythonOperator(
    task_id='send_email_if_threshold_is_surpassed',
    python_callable=verify_max_threshold,
    dag=ingestion_dag,
)

task_5 = PythonOperator(
    task_id='send_email_process_successfully_completed',
    python_callable=send_email,
    dag=ingestion_dag,
)



task_1 >> task_2 >> task_3 >> task_5
task_1 >> task_4