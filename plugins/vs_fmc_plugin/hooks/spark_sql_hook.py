import subprocess

from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException


class SparkSqlHook(BaseHook):
    """
    This hook is a wrapper around the spark-sql binary. It requires that the
    "spark-sql" binary is in the PATH.

    :param sql: The SQL query to execute
    :type sql: str
    :type conf: str (format: PROP=VALUE)
    :param conn_id: connection_id string
    :type conn_id: str
    :param verbose: Whether to pass the verbose flag to spark-sql
    :type verbose: bool
    """

    conn_name_attr = 'spark_conn_id'
    default_conn_name = 'spark_sql_default'
    conn_type = 'spark_sql_vs'
    hook_name = 'Spark SQL VaultSpeed'

    @staticmethod
    def get_connection_form_widgets():
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__spark_sql_vs__conf": StringField(lazy_gettext('Config'), widget=BS3TextFieldWidget()),
            "extra__spark_sql_vs__total_executor_cores": StringField(lazy_gettext('Total Executor Cores'), widget=BS3TextFieldWidget()),
            "extra__spark_sql_vs__executor_cores": StringField(lazy_gettext('Executor Cores'), widget=BS3TextFieldWidget()),
            "extra__spark_sql_vs__executor_memory": StringField(lazy_gettext('Executor Memory'), widget=BS3TextFieldWidget()),
            "extra__spark_sql_vs__keytab": StringField(lazy_gettext('Key File'), widget=BS3TextFieldWidget()),
            "extra__spark_sql_vs__num_executors": StringField(lazy_gettext('Number of Executors'), widget=BS3TextFieldWidget()),
            "extra__spark_sql_vs__yarn_queue": StringField(lazy_gettext('Yarn Queue'), widget=BS3TextFieldWidget())
        }

    @staticmethod
    def get_ui_field_behaviour():
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['port', 'extra', 'schema', 'login', 'password'],
            "relabeling": {},
            "placeholders": {
                'host': 'url of the Spark master (spark://host:port, mesos://host:port, yarn, or local)',
                'extra__spark_sql_vs__total_executor_cores': '(Standalone & Mesos only) Total cores for all executors (Default: all the available cores on the worker)',
                'extra__spark_sql_vs__executor_cores': '(Standalone & YARN only) Number of cores per executor (Default: 2)',
                'extra__spark_sql_vs__executor_memory': 'Memory per executor (e.g. 1000M, 2G) (Default: 1G)',
                'extra__spark_sql_vs__keytab': 'Full path to the file that contains the keytab',
                'extra__spark_sql_vs__num_executors': 'Number of executors to launch',
                'extra__spark_sql_vs__yarn_queue': 'The YARN queue to submit to (Default: "default")',
                'extra__spark_sql_vs__conf': 'arbitrary Spark configuration property (format: PROP=VALUE)'
            },
        }

    def __init__(self,
                 sql,
                 conn_id='spark_sql_default',
                 verbose=True,
                 name='default-name'
                 ):
        self._sql = sql
        self.conn_id = conn_id
        self._verbose = verbose
        self._name = name
        self._sp = None

        _conn = self.get_connection(self.conn_id)
        self._master = _conn.host
        self._conf = _conn.extra_dejson.get('extra__spark_sql_vs__conf') or _conn.extra_dejson.get('conf')
        self._total_executor_cores = _conn.extra_dejson.get('extra__spark_sql_vs__total_executor_cores') or _conn.extra_dejson.get('total_executor_cores')
        self._num_executors = _conn.extra_dejson.get('extra__spark_sql_vs__num_executors') or _conn.extra_dejson.get('num_executors')
        self._executor_cores = _conn.extra_dejson.get('extra__spark_sql_vs__executor_cores') or _conn.extra_dejson.get('executor_cores')
        self._executor_memory = _conn.extra_dejson.get('extra__spark_sql_vs__executor_memory') or _conn.extra_dejson.get('executor_memory')
        self._keytab = _conn.extra_dejson.get('extra__spark_sql_vs__keytab') or _conn.extra_dejson.get('keytab')
        self._principal = _conn.extra_dejson.get('extra__spark_sql_vs__principal') or _conn.extra_dejson.get('principal')
        self._yarn_queue = _conn.extra_dejson.get('extra__spark_sql_vs__yarn_queue') or _conn.extra_dejson.get('yarn_queue')

    def _prepare_command(self, cmd):
        """
        Construct the spark-sql command to execute. Verbose output is enabled
        as default.

        :param cmd: command to append to the spark-sql command
        :type cmd: str or list[str]
        :return: full command to be executed
        """
        connection_cmd = ["spark-sql"]
        if self._conf:
            for conf_el in self._conf.split(","):
                connection_cmd += ["--conf", conf_el]
        if self._total_executor_cores:
            connection_cmd += ["--total-executor-cores", str(self._total_executor_cores)]
        if self._executor_cores:
            connection_cmd += ["--executor-cores", str(self._executor_cores)]
        if self._executor_memory:
            connection_cmd += ["--executor-memory", self._executor_memory]
        if self._keytab:
            connection_cmd += ["--keytab", self._keytab]
        if self._principal:
            connection_cmd += ["--principal", self._principal]
        if self._num_executors:
            connection_cmd += ["--num-executors", str(self._num_executors)]
        if self._sql:
            sql = self._sql.strip()
            if sql.endswith(".sql") or sql.endswith(".hql"):
                connection_cmd += ["-f", sql]
            else:
                connection_cmd += ["-e", sql]
        if self._master:
            connection_cmd += ["--master", self._master]
        if self._name:
            connection_cmd += ["--name", self._name]
        if self._verbose:
            connection_cmd += ["--verbose"]
        if self._yarn_queue:
            connection_cmd += ["--queue", self._yarn_queue]

        if isinstance(cmd, str):
            connection_cmd += cmd.split()
        elif isinstance(cmd, list):
            connection_cmd += cmd
        else:
            raise AirflowException("Invalid additional command: {}".format(cmd))

        self.log.debug("Spark-Sql cmd: %s", connection_cmd)

        return connection_cmd

    def run(self, cmd="", **kwargs):
        """
        Remote Popen (actually execute the Spark-sql query)

        :param cmd: command to append to the spark-sql command
        :type cmd: str or list[str]
        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        :type kwargs: dict
        """
        spark_sql_cmd = self._prepare_command(cmd)
        self._sp = subprocess.Popen(spark_sql_cmd,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT,
                                    **kwargs)

        for line in iter(self._sp.stdout.readline, ''):
            self.log.info(line)

        returncode = self._sp.wait()

        if returncode:
            raise AirflowException(
                "Cannot execute {} on {}. Process exit code: {}.".format(
                    cmd, self.conn_id, returncode

                )
            )

    def kill(self):
        if self._sp and self._sp.poll() is None:
            self.log.info("Killing the Spark-Sql job")
            self._sp.kill()
