from airflow.utils.context import Context
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


class VSBigQueryExecuteQueryOperator(BigQueryExecuteQueryOperator):
    """
    Use a custom hook for Bigquery connections
    """
    
    def execute(self, context: Context):
        from vs_fmc_plugin.hooks.bigquery_hook import VSBigqueryHook as BigQueryHook
        if self.hook is None:
            self.hook = BigQueryHook(
              gcp_conn_id=self.gcp_conn_id,
              use_legacy_sql=self.use_legacy_sql,
              location=self.location,
              impersonation_chain=self.impersonation_chain
            )
        super().execute(context)
