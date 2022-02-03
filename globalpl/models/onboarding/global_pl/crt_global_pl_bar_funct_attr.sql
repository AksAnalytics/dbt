{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl.bar_funct_attr
(
	bar_function VARCHAR(250) NOT NULL  
	,bar_function_grp VARCHAR(250)   
	,functiontype VARCHAR(250)   
	,etl_crte_user VARCHAR(100)   
	,etl_crte_ts DATE   
	,etl_updt_user VARCHAR(100)   
	,etl_updt_ts DATE   
	,PRIMARY KEY (bar_function)
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}