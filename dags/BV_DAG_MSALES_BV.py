"""
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 6.0.0.4, generation date: 2026/03/10 17:25:36
DV_NAME: MOSA_DV - Release: MOSA_DV_R4(4) - Comment: MOSA_DV_R4 - Release date: 2026/03/10 17:19:56, 
BV release: PIT_RELEASE(2) - Comment: pit_release - Release date: 2026/03/10 17:20:51
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
	"start_date":datetime.strptime("10-03-2026 13:03:00", "%d-%m-%Y %H:%M:%S")
}

path_to_mtd = Path(Variable.get("path_to_metadata"))

MSALES_BV = DAG(
	dag_id="MSALES_BV", 
	default_args=default_args,
	description="MSALES_BV", 
	schedule_interval="@once", 
	catchup=False, 
	concurrency=3, 
	max_active_runs=1,
	tags=["VaultSpeed", "MOSA_DV", "BV"]
)

# insert load metadata
fmc_mtd = SnowflakeOperator(
	task_id="fmc_mtd", 
	snowflake_conn_id="toni2", 
	sql=f"""CALL "MOSA_PROJECT_PROC"."SET_FMC_MTD_BV_INIT_MOSA_DV"('{{{{ dag_run.dag_id }}}}', '{{{{ dag_run.id }}}}', '{{{{ execution_date.strftime(\"%Y-%m-%d %H:%M:%S.%f\") }}}}');""", 
	autocommit=False, 
	dag=MSALES_BV
)


# Create the check source load tasks
wait_for_MSALES_INIT = ExternalDagsSensor(
	task_id="wait_for_MSALES_INIT", 
	external_dag_id="MSALES_INIT", 
	allowed_states=[State.SUCCESS, State.FAILED], 
	dag=MSALES_BV
)

check_MSALES_INIT = ExternalDagChecker(
	task_id="check_MSALES_INIT", 
	external_dag_id="MSALES_INIT", 
	dag=MSALES_BV
)

wait_for_MSALES_INIT >> check_MSALES_INIT >> fmc_mtd



# Create BV mapping tasks
if (path_to_mtd / "798_BV_mappings_MSALES_BV_20260310_172536.json").exists():
	with open(path_to_mtd / "798_BV_mappings_MSALES_BV_20260310_172536.json") as file: 
		mappings = json.load(file)

else:
	with open(path_to_mtd / "BV_mappings_MSALES_BV.json") as file: 
		mappings = json.load(file)

tasks = {"fmc_mtd":fmc_mtd}

for map, info in mappings.items():
	task = SnowflakeOperator(
		task_id=map, 
		snowflake_conn_id="toni2", 
		sql=f"""CALL {info["map_schema"]}."{map}"();""", 
		autocommit=False, 
		dag=MSALES_BV
	)
	
	for dep in info["dependencies"]:
		task << tasks[dep]
	
	tasks[map] = task
	


# Create BV analyse tasks
end_task = DummyOperator(
	task_id="end_analyse", 
	dag=MSALES_BV
)

# Set end of load dependency
if (path_to_mtd / "798_BV_mtd_MSALES_BV_20260310_172536.json").exists():
	with open(path_to_mtd / "798_BV_mtd_MSALES_BV_20260310_172536.json") as file: 
		analyze_data = json.load(file)
else:
	with open(path_to_mtd / "BV_mtd_MSALES_BV.json") as file: 
		analyze_data = json.load(file)

for table, data in analyze_data.items():
	for dep in data["dependencies"]:
		end_task << tasks[dep.split("/")[-1]]


# Create PL mapping tasks
if (path_to_mtd / "pl_mappings_MSALES_BV.json").exists():
	with open(path_to_mtd / "pl_mappings_MSALES_BV.json") as file: 
		pl_mappings = json.load(file)

	for layer, mappings in pl_mappings.items():
		next_layer_task = DummyOperator(
			task_id=f"{layer}_done", 
			dag=MSALES_BV
		)
		
		for map_list in mappings:
			if not isinstance(map_list, list): map_list = [map_list]
			dep_list = [end_task]
			for i, map in enumerate(map_list):
				task = SnowflakeOperator(
					task_id=map, 
					snowflake_conn_id="toni2", 
					sql=f"""CALL "MOSA_PROJECT_PL_PROC"."{map}"();""", 
					autocommit=False, 
					dag=MSALES_BV
				)
				
				dep_list.append(task)
				task << dep_list[i]
			next_layer_task << dep_list[-1]
			
		end_task = next_layer_task

# End tasks
# Save load status tasks
fmc_load_fail = SnowflakeOperator(
	task_id="fmc_load_fail", 
	snowflake_conn_id="toni2", 
	sql=f"""CALL "MOSA_PROJECT_PROC"."FMC_UPD_RUN_STATUS_BV_MOSA_DV"('{{{{ dag_run.id }}}}', '0');""", 
	autocommit=False, 
	trigger_rule="one_failed", 
	dag=MSALES_BV
)
fmc_load_fail << end_task

fmc_load_success = SnowflakeOperator(
	task_id="fmc_load_success", 
	snowflake_conn_id="toni2", 
	sql=f"""CALL "MOSA_PROJECT_PROC"."FMC_UPD_RUN_STATUS_BV_MOSA_DV"('{{{{ dag_run.id }}}}', '1');""", 
	autocommit=False, 
	dag=MSALES_BV
)
fmc_load_success << end_task

