{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS bods.bar_funct_attr_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   
	,eventdts VARCHAR(65535)   
	,rec_src VARCHAR(65535)   
	,row_sqn BIGINT   
	,hash_full_record VARCHAR(65535)   
	,bar_function VARCHAR(65535)   
	,bar_function_grp VARCHAR(65535)   
	,functiontype VARCHAR(65535)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}