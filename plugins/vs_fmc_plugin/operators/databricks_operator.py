from airflow.utils.context import Context
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, _handle_databricks_operator_execution
from vs_fmc_plugin.hooks.databricks_hook import VSDatabricksHook as DatabricksHook


class VSDatabricksSubmitRunOperator(DatabricksSubmitRunOperator):
    """
    Use a custom hook for databricks connections
    """

    # the caller arg was added on newer versions of the operator
    def _get_hook(self, caller: str = "VSDatabricksSubmitRunOperator") -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args
            # in the newer version the caller is normally also send here, but that is incompatible with older versions,
            # the arg does have a default so we can leave it out
        )
    
    def execute(self, context: Context):
        hook = self._get_hook()
        
        if "cluster_id" in hook.databricks_conn.extra_dejson:
            self.json["existing_cluster_id"] = hook.databricks_conn.extra_dejson["cluster_id"]
        
        self.run_id = hook.submit_run(self.json)
        _handle_databricks_operator_execution(self, hook, self.log, context)
