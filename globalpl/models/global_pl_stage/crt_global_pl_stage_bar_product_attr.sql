{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl_stage.bar_product_attr
(
	bar_product VARCHAR(250) NOT NULL  
	,bar_product_desc VARCHAR(250)   
	,bar_product_lvl1 VARCHAR(250)   
	,bar_product_lvl2 VARCHAR(250)   
	,bar_product_lvl3 VARCHAR(250)   
	,bar_product_lvl4 VARCHAR(250)   
	,bar_product_lvl5 VARCHAR(250)   
	,hive_loaddatetime VARCHAR(250)   
	,etl_crte_user VARCHAR(100)   
	,etl_crte_ts VARCHAR(100)   
	,PRIMARY KEY (bar_product)
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}