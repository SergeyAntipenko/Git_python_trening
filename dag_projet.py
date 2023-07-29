from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


import sys 
import pathlib
import configparser


from Project.db_tables_created  import Db
from Project.egrul_process import Process
from Project.vacancies_hh import Vacancies_grep

Config = configparser.ConfigParser()
Config.read(pathlib.Path(pathlib.PurePath(__file__).parents[0],'Project/config.ini'))



default_args = {
    'owner': 'santipenko'
    # ,
    # 'retries': 5,
    # 'retry_delay': timedelta(minutes=2)
}


with DAG(
    dag_id='project_egrul',
    default_args=default_args,
    description='project  ',
    start_date=datetime(2023, 7, 15, 8),
    schedule_interval='@daily'
) as dag:

    create_table_task1 = PythonOperator(
        task_id='create_table',
        python_callable= Db.create_table,
        op_args=[Config.get('DB','connect_string')],
        dag=dag
    )
    dowload_egrul_task2 = PythonOperator(
        task_id='dowload_egrul',
        python_callable= Process.dowload_file,
        
        op_args=[Config.get('EGRULPROCESS','url'),Config.get('EGRULPROCESS','file_name')],

        dag=dag
    )
    import_egrul_task2_1 = PythonOperator(
        task_id='import_egrul',
        python_callable= Process.insert_file,
      
        op_args=[Config.get('DB','connect_string'),Config.get('EGRULPROCESS','file_name'),Config.get('EGRULPROCESS','codeOKEVD')],

        dag=dag
    )
    import_vacancies_task3 = PythonOperator(
        task_id='import_vacancies',
        python_callable=Vacancies_grep.get_vacancies,
        op_args=[Config.get('DB','connect_string'),Config.get('HH','url_html'),Config.get('HH','api_url'),Config.get('HH','text'),Config.get('HH','per_page'),Config.get('HH','search_field')]
    )
    insert_skill_task4 = PythonOperator(
        task_id='insert_skill',
 
        python_callable=Vacancies_grep.insert_skill,
        op_args=[Config.get('DB','connect_string')]

    )
    create_top_task5= PythonOperator(
        task_id='create_top',
        python_callable= Vacancies_grep.top_skill,
        op_args=[Config.get('DB','connect_string')]
    )

    create_table_task1.set_downstream(dowload_egrul_task2) 
    create_table_task1.set_downstream(import_vacancies_task3)
    dowload_egrul_task2.set_downstream(import_egrul_task2_1)
    import_vacancies_task3.set_downstream(insert_skill_task4)
    import_egrul_task2_1.set_downstream(create_top_task5)
    insert_skill_task4.set_downstream(create_top_task5)