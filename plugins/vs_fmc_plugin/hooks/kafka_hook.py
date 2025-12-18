from airflow.hooks.base import BaseHook


class KafkaHook(BaseHook):
    """
    Interact with Confluent Kafka
    """

    conn_name_attr = 'kafka_conn_id'
    default_conn_name = 'kafka_default'
    conn_type = 'kafka'
    hook_name = 'Kafka'

    @staticmethod
    def get_connection_form_widgets():
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__kafka__sasl_mechanisms": StringField(lazy_gettext('SASL Mechanisms'), widget=BS3TextFieldWidget()),
            "extra__kafka__security_protocol": StringField(lazy_gettext('Security Protocol'), widget=BS3TextFieldWidget()),
            "extra__kafka__consumer_group_id": StringField(lazy_gettext('Consumer Group Id'), widget=BS3TextFieldWidget()),
            "extra__kafka__schema_registry_url": StringField(lazy_gettext('Schema Registry URL'), widget=BS3TextFieldWidget()),
            "extra__kafka__max_number_of_messages": StringField(lazy_gettext('Max Number of Messages'), widget=BS3TextFieldWidget()),
            "extra__kafka__message_polling_timeout": StringField(lazy_gettext('Message Polling Timeout'), widget=BS3TextFieldWidget())
        }

    @staticmethod
    def get_ui_field_behaviour():
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['port', 'extra'],
            "relabeling": {'schema': 'Context Code', 'host': 'Kafka servers'},
            "placeholders": {
                'host': 'Comma separated list of kafka boostrap server urls.',
                'schema': 'Code of the execution context ("GLOBAL" is the default).',
                'login': 'User name when using SASL authentication.',
                'password': 'User password when using SASL authentication.',
                'extra__kafka__sasl_mechanisms': 'Mechanism for SASL authentication.',
                'extra__kafka__security_protocol': 'Security Protocol',
                'extra__kafka__consumer_group_id': 'Kafka consumer group id',
                'extra__kafka__schema_registry_url': 'Kafka schema registry url',
                'extra__kafka__max_number_of_messages': 'Batch size for retrieving kafka messages (default -1, meaning no limit).',
                'extra__kafka__message_polling_timeout': 'Consumer message polling timeout in seconds (default 5.)'
            },
        }

    def __init__(self, kafka_conn_id='kafka_default'):
        super(KafkaHook, self).__init__()
        self.kafka_conn_id = kafka_conn_id

        conn = self.get_connection(self.kafka_conn_id)

        self.kafka_servers = conn.host
        self.kafka_username = conn.login
        self.kafka_password = conn.password
        self.context_code = conn.schema
        self.consumer_group_id = conn.extra_dejson.get('extra__kafka__consumer_group_id') or conn.extra_dejson.get(
            'consumer_group_id') or "fmc_default_group_id"
        self.schema_registry_url = conn.extra_dejson.get('extra__kafka__schema_registry_url') or conn.extra_dejson.get('schema_registry_url')
        self.max_number_of_messages = int(
            conn.extra_dejson.get('extra__kafka__max_number_of_messages') or conn.extra_dejson.get('max_number_of_message') or -1)
        self.message_polling_timeout = int(
            conn.extra_dejson.get('extra__kafka__message_polling_timeout') or conn.extra_dejson.get('message_polling_timeout') or 5)
        self.sasl_mechanisms = conn.extra_dejson.get('extra__kafka__sasl_mechanisms') or conn.extra_dejson.get('sasl_mechanisms')
        self.security_protocol = conn.extra_dejson.get('extra__kafka__security_protocol') or conn.extra_dejson.get('security_protocol')
        self.sasl_username = conn.extra_dejson.get('extra__kafka__sasl_username') or conn.extra_dejson.get('username')
        self.sasl_password = conn.extra_dejson.get('extra__kafka__sasl_password') or conn.extra_dejson.get('password')

    def get_consumer_config(self):
        consumer_config = {
            'bootstrap.servers': self.kafka_servers,
            'schema.registry.url': self.schema_registry_url,
            'group.id': self.consumer_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'false',
        }

        #  Authenticate Kafka connection using SASL:
        if self.sasl_mechanisms is not None and self.security_protocol is not None and self.sasl_username is not None and self.sasl_password is not None:
            consumer_config['sasl.mechanisms'] = self.sasl_mechanisms
            consumer_config['security.protocol'] = self.security_protocol
            consumer_config['sasl.username'] = self.sasl_username
            consumer_config['sasl.password'] = self.sasl_password

        return consumer_config
