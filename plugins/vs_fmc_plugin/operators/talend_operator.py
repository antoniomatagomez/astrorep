from airflow.operators.bash import BashOperator
from airflow.utils.decorators import apply_defaults

from vs_fmc_plugin.hooks.talend_hook import TalendHook

from pathlib import Path


class TalendOperator(BashOperator):
    """
    Operator for executing Talend jobs
    """
    ui_color = "#f7864a"  # light Orange

    @apply_defaults
    def __init__(self, job_name, talend_conn_id, *args, **kwargs):
        self.job_name = job_name
        self.talend_conn_id = talend_conn_id
        super(TalendOperator, self).__init__(bash_command="false", *args, **kwargs)

    def execute(self, context):
        self.log.info(f"Generating start Talend job command for job {self.job_name}.")
        talend_hook = TalendHook(talend_conn_id=self.talend_conn_id)

        jar_path = Path(talend_hook.path) / f"{self.job_name}.jar"
        self.bash_command = f"java {jar_path}"

        self.log.info(f"Starting job {self.job_name}.")
        super(TalendOperator, self).execute(context)
        self.log.info(f"Completed job {self.job_name}.")
