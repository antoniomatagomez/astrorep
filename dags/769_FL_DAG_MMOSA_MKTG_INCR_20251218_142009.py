"""
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 5.7.4.21, generation date: 2025/12/18 14:20:09
DV_NAME: MOSADV - Release: MOSADV_R9(9) - Comment: MOSADV_R9 - Release date: 2025/12/17 14:57:51, 
SRC_NAME: MMARKETING - Release: MMARKETING(7) - Comment: Mmktg_R7 - Release date: 2025/12/17 14:57:02
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
	"start_date":datetime.strptime("12-11-2024 22:00:00", "%d-%m-%Y %H:%M:%S")
}

path_to_mtd = Path(Variable.get("path_to_metadata"))

if (path_to_mtd / "769_mappings_MMOSA_MKTG_INCR_20251218_142009.json").exists():
	with open(path_to_mtd / "769_mappings_MMOSA_MKTG_INCR_20251218_142009.json") as file: 
		mappings = json.load(file)

else:
	with open(path_to_mtd / "mappings_MMOSA_MKTG_INCR.json") as file: 
		mappings = json.load(file)

MMOSA_MKTG_INCR = DAG(
	dag_id="MMOSA_MKTG_INCR", 
	default_args=default_args,
	description="MMOSA_MKTG_INCR", 
	schedule_interval="*/5 * * * *", 
	concurrency=4, 
	catchup=False, 
	max_active_runs=1,
	tags=["VaultSpeed", "MMKTG", "MOSADV"]
)

# Create incremental fmc tasks
# insert load metadata
fmc_mtd = SnowflakeOperator(
	task_id="fmc_mtd", 
	snowflake_conn_id="MOSA", 
	sql=f"""CALL "MOSA_PROJECT_PROC"."SET_FMC_MTD_FL_INCR_MMKTG"('{{{{ dag_run.dag_id }}}}', '{{{{ dag_run.id }}}}', '{{{{ data_interval_end.strftime(\"%Y-%m-%d %H:%M:%S.%f\") }}}}');""", 
	autocommit=False, 
	dag=MMOSA_MKTG_INCR
)

tasks = {"fmc_mtd":fmc_mtd}

# Create mapping tasks
for map, info in mappings.items():
	task = SnowflakeOperator(
		task_id=map, 
		snowflake_conn_id="MOSA", 
		sql=f"""CALL {info["map_schema"]}."{map}"();""", 
		autocommit=False, 
		dag=MMOSA_MKTG_INCR
	)
	
	for dep in info["dependencies"]:
		task << tasks[dep]
	
	tasks[map] = task
	

# task to indicate the end of a load
end_task = DummyOperator(
	task_id="end_of_load", 
	dag=MMOSA_MKTG_INCR
)

# Set end of load dependency
if (path_to_mtd / "769_FL_mtd_MMOSA_MKTG_INCR_20251218_142009.json").exists():
	with open(path_to_mtd / "769_FL_mtd_MMOSA_MKTG_INCR_20251218_142009.json") as file: 
		analyze_data = json.load(file)
else:
	with open(path_to_mtd / "FL_mtd_MMOSA_MKTG_INCR.json") as file: 
		analyze_data = json.load(file)

for table, data in analyze_data.items():
	for dep in data["dependencies"]:
		end_task << tasks[dep.split("/")[-1]]

# Save load status tasks
fmc_load_fail = SnowflakeOperator(
	task_id="fmc_load_fail", 
	snowflake_conn_id="MOSA", 
	sql=f"""CALL "MOSA_PROJECT_PROC"."FMC_UPD_RUN_STATUS_FL_MMKTG"('{{{{ dag_run.id }}}}', '0');""", 
	autocommit=False, 
	trigger_rule="one_failed", 
	dag=MMOSA_MKTG_INCR
)
fmc_load_fail << end_task

fmc_load_success = SnowflakeOperator(
	task_id="fmc_load_success", 
	snowflake_conn_id="MOSA", 
	sql=f"""CALL "MOSA_PROJECT_PROC"."FMC_UPD_RUN_STATUS_FL_MMKTG"('{{{{ dag_run.id }}}}', '1');""", 
	autocommit=False, 
	dag=MMOSA_MKTG_INCR
)
fmc_load_success << end_task

