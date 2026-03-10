"""
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 6.0.0.4, generation date: 2026/03/10 17:23:11
DV_NAME: MOSA_DV - Release: MOSA_DV_R4(4) - Comment: MOSA_DV_R4 - Release date: 2026/03/10 17:19:56, 
SRC_NAME: MSALES - Release: MSALES(4) - Comment: MSALES_R4 - Release date: 2026/03/10 17:16:56
 """


from datetime import datetime, timedelta
from pathlib import Path
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from vs_fmc_plugin.operators.snowflake_operator import SnowflakeOperator


default_args = {
	"owner":"Vaultspeed",
	"retries": 3,
	"retry_delay": timedelta(seconds=10),
	"start_date":datetime.strptime("09-02-2026 12:00:00", "%d-%m-%Y %H:%M:%S")
}

path_to_mtd = Path(Variable.get("path_to_metadata"))

if (path_to_mtd / "796_mappings_MSALES_INIT_20260310_172311.json").exists():
	with open(path_to_mtd / "796_mappings_MSALES_INIT_20260310_172311.json") as file: 
		mappings = json.load(file)

else:
	with open(path_to_mtd / "mappings_MSALES_INIT.json") as file: 
		mappings = json.load(file)

MSALES_INIT = DAG(
	dag_id="MSALES_INIT", 
	default_args=default_args,
	description="MSALES_INIT", 
	schedule_interval="@once", 
	concurrency=3, 
	catchup=False, 
	max_active_runs=1,
	tags=["VaultSpeed", "MSLS", "MOSA_DV"]
)

# Create initial fmc tasks
# insert load metadata
fmc_mtd = SnowflakeOperator(
	task_id="fmc_mtd", 
	snowflake_conn_id="toni2", 
	sql=f"""CALL "MOSA_PROJECT_PROC"."SET_FMC_MTD_FL_INIT_MSLS"('{{{{ dag_run.dag_id }}}}', '{{{{ dag_run.id }}}}', '{{{{ execution_date.strftime(\"%Y-%m-%d %H:%M:%S.%f\") }}}}');""", 
	autocommit=False, 
	dag=MSALES_INIT
)

tasks = {"fmc_mtd":fmc_mtd}

# Create mapping tasks
for map, info in mappings.items():
	task = SnowflakeOperator(
		task_id=map, 
		snowflake_conn_id="toni2", 
		sql=f"""CALL {info["map_schema"]}."{map}"();""", 
		autocommit=False, 
		dag=MSALES_INIT
	)
	
	for dep in info["dependencies"]:
		task << tasks[dep]
	
	tasks[map] = task
	

# task to indicate the end of a load
end_task = DummyOperator(
	task_id="end_of_load", 
	dag=MSALES_INIT
)

# Set end of load dependency
if (path_to_mtd / "796_FL_mtd_MSALES_INIT_20260310_172311.json").exists():
	with open(path_to_mtd / "796_FL_mtd_MSALES_INIT_20260310_172311.json") as file: 
		analyze_data = json.load(file)
else:
	with open(path_to_mtd / "FL_mtd_MSALES_INIT.json") as file: 
		analyze_data = json.load(file)

for table, data in analyze_data.items():
	for dep in data["dependencies"]:
		end_task << tasks[dep.split("/")[-1]]

# Save load status tasks
fmc_load_fail = SnowflakeOperator(
	task_id="fmc_load_fail", 
	snowflake_conn_id="toni2", 
	sql=f"""CALL "MOSA_PROJECT_PROC"."FMC_UPD_RUN_STATUS_FL_MSLS"('{{{{ dag_run.id }}}}', '0');""", 
	autocommit=False, 
	trigger_rule="one_failed", 
	dag=MSALES_INIT
)
fmc_load_fail << end_task

fmc_load_success = SnowflakeOperator(
	task_id="fmc_load_success", 
	snowflake_conn_id="toni2", 
	sql=f"""CALL "MOSA_PROJECT_PROC"."FMC_UPD_RUN_STATUS_FL_MSLS"('{{{{ dag_run.id }}}}', '1');""", 
	autocommit=False, 
	dag=MSALES_INIT
)
fmc_load_success << end_task

