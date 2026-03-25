from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id='test_etl',start_date=datetime(2026, 2, 1), schedule_interval=None, catchup=False) as dag:
    prueba = BashOperator(task_id='presentar_mensaje', bash_command="echo 'ESTO ES UNA PRUEBA---'")