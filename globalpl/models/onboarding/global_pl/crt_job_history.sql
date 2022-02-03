{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl.job_history
(
	run_id INTEGER NOT NULL  
	,job_id INTEGER   
	,job_name VARCHAR(100)   
	,table_name VARCHAR(100)   
	,run_date VARCHAR(100)   
	,run_seq INTEGER   
	,job_status VARCHAR(100)   
	,etl_crte_user VARCHAR(100)   
	,etl_crte_ts DATE   
	,etl_updt_user VARCHAR(100)   
	,etl_updt_ts DATE   
	,PRIMARY KEY (run_id)
)
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}