from airflow.operators.bash import BashOperator
from airflow.utils.decorators import apply_defaults

from vs_fmc_plugin.hooks.odi_hook import OdiHook


class OdiOperator(BashOperator):
    """
    Operator for executing scenarios in odi
    """
    ui_color = "#f7864a"  # light Orange

    @apply_defaults
    def __init__(self, scen_name, odi_conn_id, *args, **kwargs):
        self.scen_name = scen_name
        self.odi_conn_id = odi_conn_id
        super(OdiOperator, self).__init__(bash_command="false", *args, **kwargs)

    def execute(self, context):
        self.log.info(f"Generating start scenario command for scenario {self.scen_name}.")
        odi_hook = OdiHook(odi_conn_id=self.odi_conn_id)

        start_scen_script = f"sh {odi_hook.odi_path}/agent/bin/startscenremote.sh"
        self.bash_command = f"{start_scen_script} {self.scen_name} -1 {odi_hook.context_code}" \
                            f" {odi_hook.work_repository} {odi_hook.agent_url} {odi_hook.odi_user} {odi_hook.odi_password}" \
                            f" -l 6 -n {self.task_id}"

        self.log.info(f"Starting scenario {self.scen_name}.")
        super(OdiOperator, self).execute(context)
        self.log.info(f"Completed scenario {self.scen_name}.")
