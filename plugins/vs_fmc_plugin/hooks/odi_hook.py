from airflow.hooks.base import BaseHook


class OdiHook(BaseHook):
    """
    Interact with ODI
    """

    conn_name_attr = 'odi_conn_id'
    default_conn_name = 'odi_default'
    conn_type = 'odi'
    hook_name = 'ODI'

    @staticmethod
    def get_connection_form_widgets():
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__odi__work_repository": StringField(lazy_gettext('Work repository'), widget=BS3TextFieldWidget()),
            "extra__odi__path": StringField(lazy_gettext('Path'), widget=BS3TextFieldWidget())
        }

    @staticmethod
    def get_ui_field_behaviour():
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['port', 'extra'],
            "relabeling": {'schema': 'Context Code', 'host': 'Agent URL'},
            "placeholders": {
                'host': 'URL of the ODI run-time agent that will run this session. If you want to check if the url is correct add /OdiInvoke and this should give you a webpage in your browser with an overview of the Web Services.',
                'schema': 'Code of the execution context ("GLOBAL" is the default).',
                'login': 'Name of the user used to run the ODI sessions.',
                'password': 'ODI user password',
                'extra__odi__work_repository': 'Name of the work repository containing the scenario',
                'extra__odi__path': 'path to the Oracle Data Integrator installation (such that the following path is valid: ODI_path/agent/bin/startscenremote.sh)'
            },
        }

    def __init__(self, odi_conn_id='odi_default'):
        super(OdiHook, self).__init__()
        self.odi_conn_id = odi_conn_id

        conn = self.get_connection(self.odi_conn_id)

        self.agent_url = conn.host
        self.odi_user = conn.login
        self.odi_password = conn.password
        self.context_code = conn.schema
        self.work_repository = conn.extra_dejson.get('extra__odi__work_repository') or conn.extra_dejson.get('work_repository')
        self.odi_path = conn.extra_dejson.get('extra__odi__path') or conn.extra_dejson.get('path')
