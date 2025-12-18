import time

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from vs_fmc_plugin.hooks.matillion_hook import MatillionHook


class MatillionOperator(BaseOperator):
	"""
	Operator for executing scenarios in matillion
	"""
	ui_color = "#679b41"  # Matillion green

	@apply_defaults
	def __init__(self, job_name, matillion_conn_id, **kwargs):
		super().__init__(**kwargs)
		self.job_name = job_name
		self.matillion_conn_id = matillion_conn_id

	def execute(self, context):
		self.log.info(f"Running Matillion job {self.job_name}.")
		self.matillion_hook = MatillionHook(matillion_conn_id=self.matillion_conn_id)
		self.job_id = self.matillion_hook.run_job(self.job_name)

		while True:
			state, message = self.matillion_hook.check_job(self.job_id)
			if state == "SUCCESS":
				self.log.info(f"Completed job {self.job_name}.")
				break
			elif state in ("CANCELLED", "DISABLED"):
				self.log.info(f"Job {self.job_name} was {state}.")
				break
			elif state == "FAILED":
				self.log.error(f"Job {self.job_name} failed: \n{message}")
				raise AirflowException(f"Job {self.job_name} failed: {message}")
			time.sleep(5)

		self.log.info(f"Completed job {self.job_name}.")

	def on_kill(self) -> None:
		self.matillion_hook.cancel_job(self.job_id)
		self.log.info(f"Cancelled job {self.job_name}.")
