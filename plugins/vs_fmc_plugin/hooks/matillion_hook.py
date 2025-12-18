from typing import Any, Tuple

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
import requests
from requests.auth import HTTPBasicAuth


class MatillionHook(BaseHook):
    """
    Interact with Matillion API
    """

    conn_name_attr = 'matillion_conn_id'
    default_conn_name = 'matillion_default'
    conn_type = 'matillion'
    hook_name = 'Matillion'

    @staticmethod
    def get_connection_form_widgets():
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__matillion__group_name": StringField(lazy_gettext('Group Name'), widget=BS3TextFieldWidget()),
            "extra__matillion__project_name": StringField(lazy_gettext('Project Name'), widget=BS3TextFieldWidget())
        }

    @staticmethod
    def get_ui_field_behaviour():
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['port', 'extra', 'schema'],
            "relabeling": {'host': 'Matillion URL'},
            "placeholders": {
                'host': 'URL of the Matillion instance e.g. "http://localhost:3000".',
                'login': 'Matillion user name.',
                'password': 'Matillion password.',
                'extra__matillion__group_name': 'Matillion Group name.',
                'extra__matillion__project_name': 'Matillion Project name.'
            },
        }

    def __init__(self, matillion_conn_id='matillion_conn_id'):
        super(MatillionHook, self).__init__()

        conn = self.get_connection(matillion_conn_id)
        self.url = conn.host
        self.user = conn.login
        self.password = conn.password
        self.group = conn.extra_dejson.get('extra__matillion__group_name') or conn.extra_dejson.get('group_name')
        self.project = conn.extra_dejson.get('extra__matillion__project_name') or conn.extra_dejson.get('project_name')

    def get_conn(self) -> Any:
        pass

    def run_job(self, job_name: str) -> int:
        """
        :param job_name: Name of the job to execute
        :return: task_id: Id of the job execution
        """
        url = f"{self.url}/rest/v1/group/name/{self.group}/project/name/{self.project}/version/name/default/job/name/{job_name}/run"

        response = requests.post(url, auth=HTTPBasicAuth(self.user, self.password))
        self.check_response(response)
        return int(response.json()["id"])

    def check_job(self, task_id: int) -> Tuple[str, str]:
        """
        :param task_id: Id of the job execution
        :return:
            job state, which can be one of:
                "NONE","QUEUED","RUNNING","SUCCESS","NOT_VALIDATED","FAILED","CANCELLING","CANCELLED","DISABLED"
            job message, contains the error message for a failed job
        """
        url = f"{self.url}/rest/v1/group/name/{self.group}/project/name/{self.project}/task/id/{task_id}"
        response = requests.get(url, auth=HTTPBasicAuth(self.user, self.password))
        self.check_response(response)
        message = response.json()["message"] or ", \n".join(
            [f"component {task['componentName']} {task['state']}: {task['message']}" for task in response.json()["tasks"] if
             task['message']])
        return response.json()["state"], message

    def cancel_job(self, job_id: int):
        """
        :param job_id: Id of the job execution
        :return: None
        """
        url = f"{self.url}/rest/v1/group/name/{self.group}/project/name/{self.project}/task/id/{job_id}/cancel"
        response = requests.post(url, auth=HTTPBasicAuth(self.user, self.password))
        self.check_response(response)

    def check_response(self, response):
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            self.log.error("HTTP error: %s", response.reason)
            self.log.error(response.text)
            raise AirflowException(str(response.status_code) + ":" + response.reason)
