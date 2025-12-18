from sqlalchemy import literal
from croniter import croniter
from datetime import timedelta, datetime, timezone

from airflow.models import DagRun
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


class ExternalDagsSensor(BaseSensorOperator):
    """
    Waits for a different DAG to complete for a specific execution_date window

    :param external_dag_id: The dag_id that contains the task you want to
        wait for
    :type external_dag_id: str
    :param allowed_states: list of allowed states, default is ``['success']``
    :type allowed_states: list
    :param execution_delta: how far back to look, the default is the same execution_date as the current task or DAG.
        For yesterday, use [positive!] datetime.timedelta(days=1). Either
        execution_delta or execution_date_fn can be passed to
        ExternalTaskSensor, but not both.
    :type execution_delta: datetime.timedelta
    :param execution_date_fn: function that receives the current execution date
        and returns the desired start of the window in which to query. Either execution_delta
        or execution_date_fn can be passed to ExternalTaskSensor, but not both.
    :type execution_date_fn: callable
    """

    template_fields = ['external_dag_id']

    ui_color = '#19647e'

    @apply_defaults
    def __init__(self,
                 external_dag_id,
                 allowed_states=None,
                 execution_delta=None,
                 execution_date_fn=None,
                 *args,
                 **kwargs):
        super(ExternalDagsSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]

        if execution_delta is not None and execution_date_fn is not None:
            raise ValueError(
                'Only one of `execution_delta` or `execution_date_fn` may '
                'be provided to ExternalTaskSensor; not both.')

        self.execution_delta = execution_delta
        self.execution_date_fn = execution_date_fn
        self.external_dag_id = external_dag_id

    @provide_session
    def poke(self, context, session=None):
        if self.execution_delta:
            if isinstance(self.execution_delta, timedelta):
                dttm = context['execution_date'] - self.execution_delta
            else:
                itr = croniter(self.execution_delta, context['execution_date'], ret_type=datetime)
                dttm = itr.get_prev(datetime)
        elif self.execution_date_fn:
            dttm = self.execution_date_fn(context['execution_date'])
        else:
            dttm = datetime.min.replace(tzinfo=timezone.utc)

        self.log.info(f"Poking for {self.external_dag_id} between {dttm} and {context['execution_date']}. ")

        dag_run = DagRun

        # check if any dag runs do not have the allowed state in the window
        q = session.query(dag_run.state).filter(
            dag_run.dag_id == self.external_dag_id,
            dag_run.state.notin_(self.allowed_states),
            dag_run.execution_date >= dttm,
            dag_run.execution_date <= context['execution_date'],
        )

        exists = session.query(literal(True)).filter(q.exists()).scalar()

        session.commit()
        return not exists
