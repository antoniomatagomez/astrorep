"""
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.4.21, generation date: 2025/12/18 14:36:10
DV_NAME: MOSADV - Release: MOSADV_R9(9) - Comment: MOSADV_R9 - Release date: 2025/12/17 14:57:51, 
BV release: init(1) - Comment: initial release - Release date: 2025/12/17 14:58:05
 """


from datetime import datetime, timedelta
from pathlib import Path
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.state import State
from vs_fmc_plugin.operators.external_dag_checker import ExternalDagChecker
from vs_fmc_plugin.operators.external_dags_sensor import ExternalDagsSensor
from vs_fmc_plugin.operators.snowflake_operator import SnowflakeOperator


default_args = {
	"owner":"Vaultspeed",
	"retries": 3,
	"retry_delay": timedelta(seconds=10),
	"start_date":datetime.strptime("05-09-2024 12:09:00", "%d-%m-%Y %H:%M:%S")
}

path_to_mtd = Path(Variable.get("path_to_metadata"))

MMOSA_BV_INiT = DAG(
	dag_id="MMOSA_BV_INiT", 
	default_args=default_args,
	description="MMOSA_BV_INiT", 
	schedule_interval="@once", 
	catchup=False, 
	concurrency=3, 
	max_active_runs=1,
	tags=["VaultSpeed", "MOSADV", "BV"]
)

# insert load metadata
fmc_mtd = SnowflakeOperator(
	task_id="fmc_mtd", 
	snowflake_conn_id="MOSA", 
	sql=f"""CALL "MOSA_PROJECT_PROC"."SET_FMC_MTD_BV_INIT_MOSADV"('{{{{ dag_run.dag_id }}}}', '{{{{ dag_run.id }}}}', '{{{{ execution_date.strftime(\"%Y-%m-%d %H:%M:%S.%f\") }}}}');""", 
	autocommit=False, 
	dag=MMOSA_BV_INiT
)


# Create the check source load tasks
wait_for_MMOSA_MKTG_INiT = ExternalDagsSensor(
	task_id="wait_for_MMOSA_MKTG_INiT", 
	external_dag_id="MMOSA_MKTG_INiT", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	dag=MMOSA_BV_INiT
)

check_MMOSA_MKTG_INiT = ExternalDagChecker(
	task_id="check_MMOSA_MKTG_INiT", 
	external_dag_id="MMOSA_MKTG_INiT", 
	dag=MMOSA_BV_INiT
)

wait_for_MMOSA_MKTG_INiT >> check_MMOSA_MKTG_INiT >> fmc_mtd

wait_for_MMOSA_SALES_INiT = ExternalDagsSensor(
	task_id="wait_for_MMOSA_SALES_INiT", 
	external_dag_id="MMOSA_SALES_INiT", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	dag=MMOSA_BV_INiT
)

check_MMOSA_SALES_INiT = ExternalDagChecker(
	task_id="check_MMOSA_SALES_INiT", 
	external_dag_id="MMOSA_SALES_INiT", 
	dag=MMOSA_BV_INiT
)

wait_for_MMOSA_SALES_INiT >> check_MMOSA_SALES_INiT >> fmc_mtd



# Create BV mapping tasks
if (path_to_mtd / "770_BV_mappings_MMOSA_BV_INiT_20251218_143610.json").exists():
	with open(path_to_mtd / "770_BV_mappings_MMOSA_BV_INiT_20251218_143610.json") as file: 
		mappings = json.load(file)

else:
	with open(path_to_mtd / "BV_mappings_MMOSA_BV_INiT.json") as file: 
		mappings = json.load(file)

tasks = {"fmc_mtd":fmc_mtd}

for map, info in mappings.items():
	task = SnowflakeOperator(
		task_id=map, 
		snowflake_conn_id="MOSA", 
		sql=f"""CALL {info["map_schema"]}."{map}"();""", 
		autocommit=False, 
		dag=MMOSA_BV_INiT
	)
	
	for dep in info["dependencies"]:
		task << tasks[dep]
	
	tasks[map] = task
	


# Create BV analyse tasks
end_task = DummyOperator(
	task_id="end_analyse", 
	dag=MMOSA_BV_INiT
)

# Set end of load dependency
if (path_to_mtd / "770_BV_mtd_MMOSA_BV_INiT_20251218_143610.json").exists():
	with open(path_to_mtd / "770_BV_mtd_MMOSA_BV_INiT_20251218_143610.json") as file: 
		analyze_data = json.load(file)
else:
	with open(path_to_mtd / "BV_mtd_MMOSA_BV_INiT.json") as file: 
		analyze_data = json.load(file)

for table, data in analyze_data.items():
	for dep in data["dependencies"]:
		end_task << tasks[dep.split("/")[-1]]


# Create PL mapping tasks
if (path_to_mtd / "pl_mappings_MMOSA_BV_INiT.json").exists():
	with open(path_to_mtd / "pl_mappings_MMOSA_BV_INiT.json") as file: 
		pl_mappings = json.load(file)

	for layer, mappings in pl_mappings.items():
		next_layer_task = DummyOperator(
			task_id=f"{layer}_done", 
			dag=MMOSA_BV_INiT
		)
		
		for map_list in mappings:
			if not isinstance(map_list, list): map_list = [map_list]
			dep_list = [end_task]
			for i, map in enumerate(map_list):
				task = SnowflakeOperator(
					task_id=map, 
					snowflake_conn_id="MOSA", 
					sql=f"""CALL "MOSA_PROJECT_PL_PROC"."{map}"();""", 
					autocommit=False, 
					dag=MMOSA_BV_INiT
				)
				
				dep_list.append(task)
				task << dep_list[i]
			next_layer_task << dep_list[-1]
			
		end_task = next_layer_task

# End tasks
# Save load status tasks
fmc_load_fail = SnowflakeOperator(
	task_id="fmc_load_fail", 
	snowflake_conn_id="MOSA", 
	sql=f"""CALL "MOSA_PROJECT_PROC"."FMC_UPD_RUN_STATUS_BV_MOSADV"('{{{{ dag_run.id }}}}', '0');""", 
	autocommit=False, 
	trigger_rule="one_failed", 
	dag=MMOSA_BV_INiT
)
fmc_load_fail << end_task

fmc_load_success = SnowflakeOperator(
	task_id="fmc_load_success", 
	snowflake_conn_id="MOSA", 
	sql=f"""CALL "MOSA_PROJECT_PROC"."FMC_UPD_RUN_STATUS_BV_MOSADV"('{{{{ dag_run.id }}}}', '1');""", 
	autocommit=False, 
	dag=MMOSA_BV_INiT
)
fmc_load_success << end_task

