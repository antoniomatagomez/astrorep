"""
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 6.0.0.4, generation date: 2026/03/10 11:43:48
DV_NAME: IVSP - Release: I_VSP_R5_ADD(5) - Comment: I_VSP_R5_ADD - Release date: 2025/06/06 21:07:03, 
SRC_NAME: I_SALES - Release: I_SALES(5) - Comment: ISales_R5_ADD - Release date: 2025/06/06 21:05:50
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
	"start_date":datetime.strptime("20-05-2025 05:00:00", "%d-%m-%Y %H:%M:%S")
}

path_to_mtd = Path(Variable.get("path_to_metadata"))

if (path_to_mtd / "792_mappings_IVSP_INIT_20260310_114348.json").exists():
	with open(path_to_mtd / "792_mappings_IVSP_INIT_20260310_114348.json") as file: 
		mappings = json.load(file)

else:
	with open(path_to_mtd / "mappings_IVSP_INIT.json") as file: 
		mappings = json.load(file)

IVSP_INIT = DAG(
	dag_id="IVSP_INIT", 
	default_args=default_args,
	description="IVSP_INIT", 
	schedule_interval="@once", 
	concurrency=3, 
	catchup=False, 
	max_active_runs=1,
	tags=["VaultSpeed", "ISLS", "I_VSP"]
)

# Create initial fmc tasks
# insert load metadata
fmc_mtd = SnowflakeOperator(
	task_id="fmc_mtd", 
	snowflake_conn_id="snow2", 
	sql=f"""CALL "VSP_PROC"."SET_FMC_MTD_FL_INIT_ISLS"('{{{{ dag_run.dag_id }}}}', '{{{{ dag_run.id }}}}', '{{{{ execution_date.strftime(\"%Y-%m-%d %H:%M:%S.%f\") }}}}');""", 
	autocommit=False, 
	dag=IVSP_INIT
)

tasks = {"fmc_mtd":fmc_mtd}

# Create mapping tasks
for map, info in mappings.items():
	task = SnowflakeOperator(
		task_id=map, 
		snowflake_conn_id="snow2", 
		sql=f"""CALL {info["map_schema"]}."{map}"();""", 
		autocommit=False, 
		dag=IVSP_INIT
	)
	
	for dep in info["dependencies"]:
		task << tasks[dep]
	
	tasks[map] = task
	

# task to indicate the end of a load
end_task = DummyOperator(
	task_id="end_of_load", 
	dag=IVSP_INIT
)

# Set end of load dependency
if (path_to_mtd / "792_FL_mtd_IVSP_INIT_20260310_114348.json").exists():
	with open(path_to_mtd / "792_FL_mtd_IVSP_INIT_20260310_114348.json") as file: 
		analyze_data = json.load(file)
else:
	with open(path_to_mtd / "FL_mtd_IVSP_INIT.json") as file: 
		analyze_data = json.load(file)

for table, data in analyze_data.items():
	for dep in data["dependencies"]:
		end_task << tasks[dep.split("/")[-1]]

# Save load status tasks
fmc_load_fail = SnowflakeOperator(
	task_id="fmc_load_fail", 
	snowflake_conn_id="snow2", 
	sql=f"""CALL "VSP_PROC"."FMC_UPD_RUN_STATUS_FL_ISLS"('{{{{ dag_run.id }}}}', '0');""", 
	autocommit=False, 
	trigger_rule="one_failed", 
	dag=IVSP_INIT
)
fmc_load_fail << end_task

fmc_load_success = SnowflakeOperator(
	task_id="fmc_load_success", 
	snowflake_conn_id="snow2", 
	sql=f"""CALL "VSP_PROC"."FMC_UPD_RUN_STATUS_FL_ISLS"('{{{{ dag_run.id }}}}', '1');""", 
	autocommit=False, 
	dag=IVSP_INIT
)
fmc_load_success << end_task

