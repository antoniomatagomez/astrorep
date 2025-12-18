from typing import Iterable, Mapping, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.exceptions import AirflowException


class JdbcOperator(BaseOperator):
	"""
	Executes sql code in a database using jdbc driver.
	Requires jaydebeapi.
	.. seealso::
			For more information on how to use this operator, take a look at the guide:
			:ref:`howto/operator:JdbcOperator`
	:param sql: the sql code to be executed. (templated)
	:type sql: Can receive a str representing a sql statement,
			a list of str (sql statements), or reference to a template file.
			Template reference are recognized by str ending in '.sql'
	:param jdbc_conn_id: reference to a predefined database
	:type jdbc_conn_id: str
	:param autocommit: if True, each command is automatically committed.
			(default value: False)
	:type autocommit: bool
	:param parameters: (optional) the parameters to render the SQL query with.
	:type parameters: dict or iterable
	"""
	
	template_fields = ('sql',)
	template_ext = ('.sql',)
	ui_color = '#ededed'
	
	def __init__(
		self,
		*,
		sql: str,
		jdbc_conn_id: str = 'jdbc_default',
		autocommit: bool = False,
		parameters: Optional[Union[Mapping, Iterable]] = None,
		**kwargs,
	) -> None:
		super().__init__(**kwargs)
		self.parameters = parameters
		self.sql = sql
		self.jdbc_conn_id = jdbc_conn_id
		self.autocommit = autocommit
		self.hook = None
	
	def execute(self, context) -> None:
		try:
			self.log.info('Executing: %s', self.sql)
			hook = JdbcHook(jdbc_conn_id=self.jdbc_conn_id)
			hook.run(self.sql, self.autocommit, parameters=self.parameters)
		except Exception as e:
			self.log.error(e, exc_info=e)
			raise AirflowException("JDBC query failed")
