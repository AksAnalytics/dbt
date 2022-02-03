{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl.status_history
(
	run_id INTEGER NOT NULL DEFAULT IDENTITY(911236, 0) 
	,job_nm VARCHAR(100)   
	,run_date DATE   
	,run_seq INTEGER   
	,job_status VARCHAR(100)   
	,curr_mon_rows_del INTEGER   
	,curr_mon_rows_ins INTEGER   
	,prev_mon_rows_del INTEGER   
	,prev_mon_rows_ins INTEGER   
	,start_time TIMESTAMP WITHOUT TIME ZONE   
	,end_time TIMESTAMP WITHOUT TIME ZONE   
	,PRIMARY KEY (run_id)
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}