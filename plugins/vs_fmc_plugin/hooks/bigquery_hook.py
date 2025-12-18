from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

from airflow import version

from google.cloud.bigquery import Client
from google.api_core.gapic_v1.client_info import ClientInfo

class VSBigqueryHook(BigQueryHook):
    """
    Send VaultSpeed as the user agent for Bigquery connections
    """

    def get_client(self, project_id: str = PROVIDE_PROJECT_ID, location: str = None) -> Client:
        """Get an authenticated BigQuery Client.

        :param project_id: Project ID for the project which the client acts on behalf of.
        :param location: Default location for jobs / datasets / tables.
        """
        return Client(
            client_info=ClientInfo(client_library_version="airflow_v" + version.version,
                                   user_agent="VaultSpeed ADI/1.0 (GPN:VaultSpeed)"),
            project=project_id,
            location=location,
            credentials=self.get_credentials(),
        )
