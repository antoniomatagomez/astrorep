import json

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DbtOperator(BaseOperator):
    """
    Execute dbt models

    :param selectors: An array of selectors to determine which models to execute
    :type selectors: str
    :param job_name: Name of the dbt job (Only needed for cloud connections)
    :type job_name: str
    :param load_type: load type (INIT or INCR) (optional)
    :type load_type: str
    :param source: Name of the source (optional)
    :type source: str
    :param conn_id: connection_id string
    :type conn_id: str
    """
    
    @apply_defaults
    def __init__(self,
                 selectors,
                 job_name,
                 dbt_conn_id='dbt_default',
                 load_type=None,
                 source=None,
                 **kwargs):
        super().__init__(**kwargs)
        self._selectors = selectors
        self._conn_id = dbt_conn_id
        self._load_type = load_type
        self._source = source
        self._job_name = job_name
        
        self._variables = {}
        if load_type: self._variables["load_type"] = load_type
        if source: self._variables["source"] = source
        
        from airflow.models.connection import Connection
        self._conn_type = Connection.get_connection_from_secrets(dbt_conn_id).conn_type
    
    def execute(self, context):
        """
        Call the hook matching the selected connection type
        """
        if self._conn_type == 'dbt_cloud':
            from vs_fmc_plugin.hooks.dbt_cloud_hook import DbtCloudHook, get_response_data, DbtCloudJobRunException
            self._hook = DbtCloudHook(dbt_cloud_conn_id=self._conn_id)
            command = f"""dbt run --select {",".join(self._selectors)} --vars "{json.dumps(self._variables)}" """
            job = [job["id"] for job in get_response_data(self._hook.list_jobs()) if job["name"] == self._job_name]
            if not job:
                job_id = self._hook.create_job(self._job_name, command).json()["data"]["id"]
            else:
                job_id = job[0]
            self.run_id = self._hook.trigger_job_run(job_id=job_id, cause=f"VaultSpeed FMC execution").json()["data"]["id"]
            
            if self._hook.wait_for_job_run_status(self.run_id):
                self.log.info("Job run %s has completed successfully.", str(self.run_id))
            else:
                raise DbtCloudJobRunException(f"Job run {self.run_id} has failed or has been cancelled.")
        
        elif self._conn_type == 'dbt_cli':
            from vs_fmc_plugin.hooks.dbt_cli_hook import DbtCliHook
            self._hook = DbtCliHook(dbt_conn_id=self._conn_id)
            self._hook.run_cli(selectors=self._selectors, variables=self._variables)
        
        else:
            raise Exception(f"The connection {self._conn_id} of type {self._conn_type} can not be used to execute dbt.")
    
    def on_kill(self):
        if self._conn_type == 'dbt_cloud':
            self._hook.cancel_job_run(self.run_id)
        elif self._conn_type == 'dbt_cli':
            self._hook.kill()
