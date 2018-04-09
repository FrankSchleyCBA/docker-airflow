import logging

from airflow.models import BaseOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)
#Test comment
class CustomOperator(BaseOperator):

    @apply_defaults
    def __init__(self, my_operator_param, *args, **kwargs):
        self.operator_param = my_operator_param
        super(CustomOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Hello World!")
        log.info('operator_param: %s', self.operator_param)


class CustomSensor(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(CustomSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        """Determines whether the task is successful or not
        if True: continue with the dag
        if False: call poke again
        if Exception: call poke again until the max number of retries has been reached
        """
        current_minute = datetime.now().minute
        if current_minute % 3 != 0:
            log.info("Current minute (%s) not is divisible by 3, sensor will retry.", current_minute)
            return False
        log.info("Current minute (%s) is divisible by 3, sensor finishing.", current_minute)
        return True


class MyFirstPlugin(AirflowPlugin):
    name = "my_first_plugin"
    operators = [CustomOperator, CustomSensor]
