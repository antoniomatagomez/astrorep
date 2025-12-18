import airflow.providers.databricks.hooks.databricks as hook
from airflow.compat.functools import cached_property

# for older versions of the databricks hook we can modify the user agent this way
hook.USER_AGENT_HEADER = {"user-agent": "VaultSpeed"}


# for newer versions of the databricks hook we can modify the user agent this way
class VSDatabricksHook(hook.DatabricksHook):
	"""
	Send VaultSpeed as the user agent for databricks connections
	"""
	
	@cached_property
	def user_agent_value(self) -> str:
		return "VaultSpeed"
