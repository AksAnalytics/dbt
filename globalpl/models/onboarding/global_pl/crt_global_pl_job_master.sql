{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl.job_master
(
	job_id INTEGER NOT NULL  
	,job_name VARCHAR(100)   
	,table_name VARCHAR(100)   
	,frequency VARCHAR(100)   
	,job_state VARCHAR(100)   
	,etl_crte_user VARCHAR(100)   
	,etl_crte_ts DATE   
	,etl_updt_user VARCHAR(100)   
	,etl_updt_ts DATE   
	,PRIMARY KEY (job_id)
)
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}