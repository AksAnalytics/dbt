{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl.bar_entity_attr_temp
(
	bar_entity VARCHAR(250)   
	,bar_entity_desc VARCHAR(250)   
	,bar_entity_currency VARCHAR(250)   
	,bar_entity_lvl1 VARCHAR(250)   
	,bar_entity_lvl2 VARCHAR(250)   
	,bar_entity_lvl3 VARCHAR(250)   
	,bar_entity_lvl4 VARCHAR(250)   
	,bar_entity_region VARCHAR(250)   
	,etl_crte_user VARCHAR(100)   
	,etl_crte_ts DATE   
	,etl_updt_user VARCHAR(100)   
	,etl_updt_ts DATE   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}