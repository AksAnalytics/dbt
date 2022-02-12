{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.entity ( 
	name                 varchar(20)    ,
	description          varchar(200)    ,
	level4               varchar(100)    ,
	level5               varchar(30)    ,
	level6               varchar(30)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}