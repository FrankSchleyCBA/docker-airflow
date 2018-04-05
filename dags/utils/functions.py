from datetime import datetime, timedelta
import dateutil.parser
import json
from json import JSONDecoder
import re

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import ExternalTaskSensor

def datetime_praser(json_dict):
    for (key, value) in json_dict.items():
        if isinstance(value, str) and re.search("d1", value):
            json_dict[key] = datetime.strptime(value[3:], "%Y-%m-%dT%H:%M:%S")
        elif isinstance(value, str) and re.search("d2", value):
            json_dict[key] = timedelta(seconds=int(value[3:]))
        else:
            json_dict[key] = value
    return json_dict

def read_dag_definition(filepath):
    """Reads in a DAG definition. DAGs are defined in JSON
    raises a FileNotFoundError if the filepath doesn't exist
    raises a InvalidConfigError if the dag definition is invalid

    :filepath: filepath to dag definition
    :returns: dict

    """
    with open(filepath) as f:
        content = f.read()
        if not content:
            return {}
        else:
            try:
                return json.loads(content, object_hook=datetime_praser)
            except json.decoder.JSONDecodeError as e:
                raise InvalidConfigError("The json format of '{filepath}' is invalid.")


class InvalidConfigError(Exception):
    pass



def dag_builder(conf):
    """Return a DAG given a configuration"""
    dag = DAG(dag_id=conf['dag_id'], schedule_interval=conf['schedule_interval'], start_date=conf['start_date'], catchup=conf['catchup'], default_args=conf['default_args'])
    task_conf = conf.get('tasks', [])
    dep_conf = conf.get('dependencies', [])
    tasks = {}
    if task_conf:
        tasks = attach_tasks(dag, task_conf)
    if dep_conf:
        build_flow(dep_conf, tasks)
    return dag


def attach_tasks(dag, task_conf):
    def build_task(task_def):
        operator = operator_factory(task_def['operator_type'])
        return operator(dag=dag, task_id=task_def['task_id'], **task_def['parameters'])
    task_dict = {}
    for task in task_conf:
        task_dict[task.get("task_id")] = build_task(task)
    return task_dict

def build_flow(dep_conf, task_dict):
    for source_key in dep_conf:
        dest_task = task_dict.get(source_key)
        source_task_keys = dep_conf.get(source_key)
        for key in source_task_keys:
            source_task = task_dict.get(key)
            if (source_task != None and dest_task != None):
                source_task >> dest_task

def operator_factory(name):
    operators = {
            'dummy': DummyOperator,
            'bash': BashOperator,
            'external_sensor': ExternalTaskSensor,
            }
    return operators[name]
