from airflow import DAG
from utils.functions import *

# read from json
filepath = '/usr/local/airflow/dags/dag_config/dag_config.json'
my_conf = read_dag_definition(filepath)

# build_dag
my_dag = dag_builder(my_conf)
