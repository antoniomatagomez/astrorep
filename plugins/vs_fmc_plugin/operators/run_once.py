from airflow.operators.python import PythonOperator
from airflow.models import SkipMixin
from airflow.utils.db import provide_session
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance as TI
from airflow.models import Log
from airflow.utils.state import State
from airflow.models import Variable


class RunOnceBranchOperator(PythonOperator, SkipMixin):
    """
    Branches based on whether a task has been run successfully for this dag run.
    This makes it such that when the task status of tasks is reset, the tasks that where successful will not run again.
    """

    ui_color = "#80dff2"  # cyan

    def __init__(self, run_once_task_id=None, skip_task_id=None, *args, **kwargs):
        kwargs["python_callable"] = lambda x: x
        super(RunOnceBranchOperator, self).__init__(*args, **kwargs)
        self.run_once_task_id = run_once_task_id
        self.skip_task_id = skip_task_id

    @provide_session
    def execute(self, context, session=None):
        print("execute runonce operator")
        self.log.info("execute once run operator")

        rerun_all_tasks = Variable.get("rerun_all_tasks") == False
        logs = session.query(Log).filter(
            Log.task_id == self.run_once_task_id,
            Log.dag_id == context["dag_run"].dag_id,
            Log.execution_date == context["dag_run"].execution_date,
            Log.event.in_(("success", "failed")),
        ).order_by(Log.dttm.desc()).all()

        self.log.info("log tasks: " + str([l.task_id for l in logs]))
        self.log.info("log events: " + str([l.event for l in logs]))

        if len(logs) == 0:
            previous_success = False
        else:
            previous_success = logs[0].event == "success"

        if previous_success and (not rerun_all_tasks):
            self.log.info(f"Found existing task run ({previous_success}) with state success. "
                          "Therefore skip the direct downstream task!")
            branch = self.skip_task_id
        else:
            self.log.info("Found no existing task run with state success. "
                          "Therefore run the direct downstream task")
            branch = self.run_once_task_id

        self.log.info(f"Following branch {branch}")
        self.log.info("Marking other directly downstream tasks as skipped")

        downstream_tasks = context["task"].downstream_list
        self.log.info(f"Downstream task_ids {downstream_tasks}")

        skip_tasks = [t for t in downstream_tasks if t.task_id != branch]
        if downstream_tasks:
            self.skip(context["dag_run"], context["ti"].execution_date, skip_tasks)

        self.log.info("Done.")


class JoinOperator(PythonOperator, SkipMixin):
    """
    Joins the 2 branches of RunOnceBranchOperator together.
    Since only one of the 2 branches will be run, and the other skipped, use this operator with use trigger_rule="all_done".
    This operator will thus run when one of the tasks is skipped and the other ended, so it checks whether one of the 2 tasks failed,
    if one failed then this task will also fail thus propagating the failure state and otherwise this tasks will run successfully.
    """

    ui_color = "#80dff2"  # cyan

    def __init__(self, *args, **kwargs):
        kwargs["python_callable"] = lambda x: x
        super(JoinOperator, self).__init__(*args, **kwargs)

    @provide_session
    def execute(self, context, session=None):
        print("execute join operator")
        self.log.info("execute join operator")

        ti = session.query(TI).filter(
            TI.task_id.in_(context["task"].upstream_task_ids),
            TI.dag_id == context["dag_run"].dag_id,
            TI.execution_date == context["dag_run"].execution_date,
        ).all()

        failed = [t for t in ti if t.state in (State.FAILED, State.UPSTREAM_FAILED)]

        if failed:
            self.log.info(f"The join task failed bacause the parent task {failed} failed.")
            raise AirflowException("One of the parent tasks failed")

        self.log.info("Done.")
