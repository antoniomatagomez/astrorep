from airflow.hooks.base import BaseHook


class TalendHook(BaseHook):
    """
    Interact with Talend
    """

    conn_name_attr = 'talend_conn_id'
    default_conn_name = 'talend_default'
    conn_type = 'Talend'
    hook_name = 'Talend'

    @staticmethod
    def get_ui_field_behaviour():
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['port', 'extra', 'schema', 'login', 'password'],
            "relabeling": {},
            "placeholders": {
                'host': 'Path to the directory where the JAR files are stored'
            },
        }

    def __init__(self, talend_conn_id='talend_default'):
        super(TalendHook, self).__init__()
        self.talend_conn_id = talend_conn_id

        conn = self.get_connection(self.talend_conn_id)

        self.path = conn.host
