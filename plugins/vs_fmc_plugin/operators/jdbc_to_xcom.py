from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class JdbcToXcom(BaseOperator):
    """
    Executes sql code in a database using jdbc driver and pushes the first record to xcom.
    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in ".sql"
    :param jdbc_conn_id: reference to a predefined database
    :type jdbc_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: mapping or iterable
    """

    template_fields = ("sql",)
    template_ext = (".sql",)
    ui_color = "#a58be5"  # light purple

    @apply_defaults
    def __init__(self, sql, jdbc_conn_id="jdbc_default", parameters=None, *args, **kwargs):
        super(JdbcToXcom, self).__init__(*args, **kwargs)
        self.parameters = parameters

        self.sql = sql
        self.jdbc_conn_id = jdbc_conn_id
        self.autocommit = True

    def execute(self, context):
        from vs_fmc_plugin.hooks.jdbc_hook import JdbcHook
        try:
            self.log.info("Executing: %s", self.sql)
            hook = JdbcHook(jdbc_conn_id=self.jdbc_conn_id)
            result = hook.get_first(sql=self.sql, parameters=self.parameters)
            self.log.info("Result: %s", result)
            return result
        except Exception as e:
            self.log.error(e, exc_info=e)
            raise AirflowException(e)
