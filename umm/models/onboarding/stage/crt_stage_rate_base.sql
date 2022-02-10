{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.rate_base ( 
	range_start_date     date    ,
	range_end_date       date    ,
	source_system        varchar(10)    ,
	bar_entity           varchar(5)    ,
	soldtocust           varchar(10)    ,
	bar_custno           varchar(20)    ,
	material             varchar(30)    ,
	bar_product          varchar(22)    ,
	bar_currtype         varchar(6)    ,
	total_bar_amt        decimal(18,0)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}