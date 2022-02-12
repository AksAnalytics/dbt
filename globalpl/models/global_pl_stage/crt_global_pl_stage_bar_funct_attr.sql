{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl_stage.bar_funct_attr
(
	bar_function VARCHAR(250) NOT NULL  
	,bar_function_grp VARCHAR(250)   
	,functiontype VARCHAR(250)   
	,hive_loaddatetime VARCHAR(250)   
	,etl_crte_user VARCHAR(100)   
	,etl_crte_ts VARCHAR(100)   
	,PRIMARY KEY (bar_function)
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}