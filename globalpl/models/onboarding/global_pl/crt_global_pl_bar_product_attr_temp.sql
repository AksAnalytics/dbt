{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl.bar_product_attr_temp
(
	bar_product VARCHAR(250)   
	,bar_product_desc VARCHAR(250)   
	,bar_product_lvl1 VARCHAR(250)   
	,bar_product_lvl2 VARCHAR(250)   
	,bar_product_lvl3 VARCHAR(250)   
	,bar_product_lvl4 VARCHAR(250)   
	,bar_product_lvl5 VARCHAR(250)   
	,etl_crte_user VARCHAR(100)   
	,etl_crte_ts DATE   
	,etl_updt_user VARCHAR(100)   
	,etl_updt_ts DATE   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}