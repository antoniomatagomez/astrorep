from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, DagRun
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


class ExternalDagChecker(BaseOperator):
    """
    Checks if the last dag run (before this run) of a DAG was successful.

    :param external_dag_id: The dag_id that contains the task you want to
        wait for
    :type external_dag_id: str
    """
    template_fields = ('external_dag_id',)

    ui_color = '#19647e'

    @apply_defaults
    def __init__(self,
                 external_dag_id,
                 *args,
                 **kwargs):
        super(ExternalDagChecker, self).__init__(*args, **kwargs)
        self.external_dag_id = external_dag_id

    @provide_session
    def execute(self, context=None, session=None):
        self.log.info(f"Checking status of {self.external_dag_id}")
        dag_run = DagRun

        state = session.query(dag_run.state).filter(
            dag_run.dag_id == self.external_dag_id,
            dag_run.execution_date <= context["dag_run"].execution_date,
        ).order_by(dag_run.start_date.desc().nullslast()).first()[0]

        self.log.info(f"external dag state: {state}.")

        if not state == State.SUCCESS:
            raise AirflowException(f"The external DAG {self.external_dag_id} was not successful")
        self.log.info(f"{self.external_dag_id} is ok.")
