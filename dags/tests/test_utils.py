#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import json
from utils import *

class TestReadingDagDefinition(object):

    """Test reading a config file under differnt scenarios"""

    def  test_non_existent_file_should_raise_exception(self):
        with pytest.raises(FileNotFoundError):
            read_dag_definition(filepath="does_not_exist")

    def test_empty_file_returns_empty_dict(self, tmpdir):
        filepath = self.create_dag_def(tmpdir, content="")
        assert {} == read_dag_definition(filepath)

    def test_valid_json_returns_a_dict(self,tmpdir):
        data = {'key': 'value'}
        json_string = json.dumps(data)
        filepath = self.create_dag_def(tmpdir, json_string)
        assert data == read_dag_definition(filepath)

    def test_invalid_json_raises_invalidconfigerror(self, tmpdir):
        invalid_json = "INVALID"
        filepath = self.create_dag_def(tmpdir, invalid_json)
        with pytest.raises(InvalidConfigError):
            read_dag_definition(filepath)

    def create_dag_def(self, tmpdir, content):
        filepath = tmpdir.join("config.json")
        filepath.write(content)
        return filepath


class TestBuildingDagFromConfig(object):

    """Build a dag with zero or more tasks"""

    def test_build_dag_without_tasks(self):
        conf = self.base_conf()
        dag = dag_builder(conf)
        assert dag.dag_id == 'first_dag'

    def test_build_dag_with_one_task(self):
        conf = self.base_conf()
        task_conf = [{'task_id': 'dummy_1',
                    'operator_type': 'dummy',
                    'parameters': {}}]
        conf['tasks'] = task_conf

        dag = dag_builder(conf)

        assert len(dag.tasks) == 1

    def test_build_dag_with_dependency_tasks(self):
        conf = self.base_conf()
        task_conf = [{'task_id': 'dummy_1', 'operator_type': 'dummy', 'parameters': {}},
                     {'task_id': 'bash_2', 'operator_type': 'bash','parameters': {'bash_command': 'echo \"Hello World\"'}},
                     {'task_id': 'dummy_3', 'operator_type': 'dummy', 'parameters': {}}]
        task_dep = {"bash_2": ['dummy_1', 'dummy_3']}
        conf['tasks'] = task_conf
        conf['dependencies'] = task_dep
        dag = dag_builder(conf)
        dummy_1, bash_2, dummy_3 = dag.tasks
        self._check_downstream_ids(ups=[dummy_1, dummy_3], down=bash_2)

    def _check_downstream_ids(self, ups, down):
        #assert up._downstream_task_ids == [t.task_id for t in downs]
        assert down._upstream_task_ids == [t.task_id for t in ups]

    def base_conf(self):
        return {'dag_id': 'first_dag',
                'schedule_interval': '0 12 * * *',
                'start_date': '2018-03-24',
                'default_args': {
                                 'owner': 'airflow',
                                 'email': 'support@cba.com.au',
                                 'catchup': False}}
