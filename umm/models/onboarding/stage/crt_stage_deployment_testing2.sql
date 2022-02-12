{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.deployment_testing2 ( 
	range_start_date     date    ,
	range_end_date       date    ,
	shiptocust           varchar(256)    ,
	soldtocust           varchar(256)    ,
	bar_custno           varchar(256)    ,
	material             varchar(256)    ,
	bar_product          varchar(256)    ,
	total_bar_amt        decimal(18,0)    ,
	total_bar_volume     decimal(18,0)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}