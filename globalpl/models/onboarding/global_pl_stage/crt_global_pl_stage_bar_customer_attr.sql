{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl_stage.bar_customer_attr
(
	bar_customer VARCHAR(250) NOT NULL  
	,bar_customer_desc VARCHAR(250)   
	,bar_customer_lvl1 VARCHAR(250)   
	,bar_customer_lvl2 VARCHAR(250)   
	,bar_customer_lvl3 VARCHAR(250)   
	,bar_customer_lvl4 VARCHAR(250)   
	,hive_loaddatetime VARCHAR(250)   
	,etl_crte_user VARCHAR(100)   
	,etl_crte_ts VARCHAR(100)   
	,PRIMARY KEY (bar_customer)
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}