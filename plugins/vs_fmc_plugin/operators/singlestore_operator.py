from typing import Iterable, Mapping, Any, Optional, Union
from airflow.models import BaseOperator
from vs_fmc_plugin.hooks.singlestore_hook import SingleStoreHook
from airflow.exceptions import AirflowException


class SingleStoreOperator(BaseOperator):
    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    def __init__(
            self,
            *,
            sql: str,
            singlestore_conn_id: str = 'singlestore_default',
            autocommit: bool = False,
            parameters: Optional[Union[Mapping, Iterable]] = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.parameters = parameters
        self.sql = sql
        self.singlestore_conn_id = singlestore_conn_id
        self.autocommit = autocommit

    def get_hook(self) -> SingleStoreHook:
        """
        Create and return SingleStoreHook.
        :return: a SingleStoreHook instance.
        :rtype: SingleStoreHook
        """
        return SingleStoreHook(
            singlestore_conn_id=self.singlestore_conn_id,
        )

    def execute(self, context: Any) -> None:
        try:
            """Run query on singlestore"""
            self.log.info('Executing: %s', self.sql)
            hook = self.get_hook()
            hook.run(self.sql, autocommit=self.autocommit, parameters=self.parameters)
        except Exception as e:
            self.log.error(e, exc_info=e)
            raise AirflowException("SingleStore query failed")
