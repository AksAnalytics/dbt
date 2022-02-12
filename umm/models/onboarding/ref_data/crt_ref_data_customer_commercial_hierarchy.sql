{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.customer_commercial_hierarchy ( 
	base_customer        varchar(50)  NOT NULL  ,
	major_customer       varchar(50)    ,
	market               varchar(50)    ,
	channel              varchar(50)    ,
	segment              varchar(50)    ,
	total_customer       varchar(50)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}