from airflow.hooks.dbapi import DbApiHook
from airflow.models.connection import Connection
from airflow.exceptions import AirflowException
from typing import Any, Dict, Optional, Union
from contextlib import closing
import jaydebeapi


class SingleStoreHook(DbApiHook):
    """
    A client to interact with SingleStore database.

    This hook requires the singlestore_conn_id connection. The singlestore host, login,
    and, password field must be setup in the connection. Other inputs can be defined
    in the connection or hook instantiation.

    """
    conn_name_attr = 'singlestore_conn_id'
    default_conn_name = 'singlestore_default'
    conn_type = 'singlestore'
    hook_name = 'SingleStore'
    supports_autocommit = True

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__singlestore__drv_path": StringField(lazy_gettext('Driver Path'), widget=BS3TextFieldWidget()),
            "extra__singlestore__drv_clsname": StringField(
                lazy_gettext('Driver Class'), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['port', 'schema', 'extra'],
            "relabeling": {'host': 'Connection URL'},
        }

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def get_conn(self) -> jaydebeapi.Connection:
        conn: Connection = self.get_connection(getattr(self, self.conn_name_attr))
        host: str = conn.host
        login: str = conn.login
        psw: str = conn.password
        jdbc_driver_loc: Optional[str] = conn.extra_dejson.get('extra__singlestore__drv_path') or conn.extra_dejson.get('drv_path')
        jdbc_driver_name: Optional[str] = conn.extra_dejson.get('extra__singlestore__drv_clsname') or conn.extra_dejson.get('drv_clsname')
        driver_args = []
        # For some connections, login and password are optional.
        # In this case the driver arguments should be an empty array instead of an array of None values
        if login: driver_args.append(str(login))
        if psw: driver_args.append(str(psw))

        conn = jaydebeapi.connect(
            jclassname=jdbc_driver_name,
            url=str(host),
            driver_args=driver_args,
            jars=jdbc_driver_loc.split(",") if jdbc_driver_loc else None,
        )
        return conn

    def set_autocommit(self, conn: jaydebeapi.Connection, autocommit: bool) -> None:
        """
        Enable or disable autocommit for the given connection.

        :param conn: The connection.
        :type conn: connection object
        :param autocommit: The connection's autocommit setting.
        :type autocommit: bool
        """
        conn.jconn.setAutoCommit(autocommit)

    def get_autocommit(self, conn: jaydebeapi.Connection) -> bool:
        """
        Get autocommit setting for the provided connection.
        Return True if conn.autocommit is set to True.
        Return False if conn.autocommit is not set or set to False

        :param conn: The connection.
        :type conn: connection object
        :return: connection autocommit setting.
        :rtype: bool
        """
        return conn.jconn.getAutoCommit()

    def run(self, sql: Union[str, list], autocommit: bool = False, parameters: Optional[dict] = None):
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially

        :param sql: the sql string to be executed with possibly multiple statements,
          or a list of sql statements to execute
        :type sql: str or list
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :type autocommit: bool
        :param parameters: The parameters to render the SQL query with.
        :type parameters: dict or iterable
        """
        try:
            with self.get_conn() as conn:
                conn = self.get_conn()
                self.set_autocommit(conn, autocommit)
                if not self.get_autocommit(conn):
                    with closing(conn.cursor()) as cur:
                        cur.execute("BEGIN;")

                if isinstance(sql, str):
                    with closing(conn.cursor()) as cur:
                            cur.execute(sql, parameters)
                            self.log.info("Rows affected: %s", cur.rowcount)

                elif isinstance(sql, list):
                    self.log.debug("Executing %d statements against SingleStore DB", len(sql))
                    with closing(conn.cursor()) as cur:
                        for sql_statement in sql:

                            self.log.info("Running statement: %s, parameters: %s", sql_statement, parameters)
                            if parameters:
                                cur.execute(sql_statement, parameters)
                            else:
                                cur.execute(sql_statement)
                            self.log.info("Rows affected: %s", cur.rowcount)

                # If autocommit was set to False for db that supports autocommit,
                # or if db does not supports autocommit, we do a manual commit.
                if not self.get_autocommit(conn):
                    conn.commit()
                    
        except Exception as e:
            with self.get_conn() as conn:
                conn = self.get_conn()
                with closing(conn.cursor()) as cur:
                        cur.execute("ROLLBACK;")
            self.log.error(e, exc_info=e)
            raise AirflowException("SingleStore query failed")
