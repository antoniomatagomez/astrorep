from airflow.plugins_manager import AirflowPlugin
from flask_admin.base import MenuLink

vaultspeed_menu_item = {"name": "Generate code with VaultSpeed",
                        "category": "VaultSpeed",
                        "category_icon": "fa-th",
                        "href": "https://app.vaultspeed.com/flow-management-control"}

docs_menu_item = {"name": "VaultSpeed Documentation",
                  "category": "VaultSpeed",
                  "category_icon": "fa-th",
                  "href": "https://docs.vaultspeed.com/space/VPD/3012624432/Airflow"}

vaultspeed_menu_link = MenuLink(
    category='VaultSpeed',
    name='Generate code with VaultSpeed',
    url='https://app.vaultspeed.com/flow-management-control'
)
docs_menu_link = MenuLink(
    category='VaultSpeed',
    name='VaultSpeed Documentation',
    url='https://docs.vaultspeed.com/space/VPD/3012624432/Airflow'
)


class VsFmcPlugin(AirflowPlugin):
    name = "vs_fmc_plugin"
    appbuilder_menu_items = [vaultspeed_menu_item, docs_menu_item]
    menu_links = [vaultspeed_menu_link, docs_menu_link]


def get_provider_info():
    return {
        "package-name": "airflow-provider-vaultspeed",
        "name": "VaultSpeed Provider",
        "description": "A VaultSpeed provider for Apache Airflow.",
        "hook-class-names": [
          "vs_fmc_plugin.hooks.odi_hook.OdiHook",
          "vs_fmc_plugin.hooks.talend_hook.TalendHook",
          "vs_fmc_plugin.hooks.kafka_hook.KafkaHook",
          "vs_fmc_plugin.hooks.spark_sql_hook.SparkSqlHook",
          "vs_fmc_plugin.hooks.matillion_hook.MatillionHook",
          "vs_fmc_plugin.hooks.snowflake_hook.SnowflakeHook",
          "vs_fmc_plugin.hooks.livy_hook.LivyHook",
          "vs_fmc_plugin.hooks.singlestore_hook.SingleStoreHook",
          "vs_fmc_plugin.hooks.dbt_cli_hook.DbtCliHook",
          "vs_fmc_plugin.hooks.dbt_cloud_hook.DbtCloudHook"
        ],
        "connection-types": [
            {"connection-type": "odi", "hook-class-name": "vs_fmc_plugin.hooks.odi_hook.OdiHook"},
            {"connection-type": "talend", "hook-class-name": "vs_fmc_plugin.hooks.talend_hook.TalendHook"},
            {"connection-type": "kafka", "hook-class-name": "vs_fmc_plugin.hooks.kafka_hook.KafkaHook"},
            {"connection-type": "spark_sql_vs", "hook-class-name": "vs_fmc_plugin.hooks.spark_sql_hook.SparkSqlHook"},
            {"connection-type": "matillion", "hook-class-name": "vs_fmc_plugin.hooks.matillion_hook.MatillionHook"},
            {"connection-type": "snowflake", "hook-class-name": "vs_fmc_plugin.hooks.snowflake_hook.SnowflakeHook"},
            {"connection-type": "spark_sql_livy", "hook-class-name": "vs_fmc_plugin.hooks.livy_hook.LivyHook"},
            {"connection-type": "singlestore", "hook-class-name": "vs_fmc_plugin.hooks.singlestore_hook.SingleStoreHook"},
            {"connection-type": "dbt_cli", "hook-class-name": "vs_fmc_plugin.hooks.dbt_cli_hook.DbtCliHook"},
            {"connection-type": "dbt_cloud", "hook-class-name": "vs_fmc_plugin.hooks.dbt_cloud_hook.DbtCloudHook"}
        ],
        "versions": ["5.7.1.2"]
    }
