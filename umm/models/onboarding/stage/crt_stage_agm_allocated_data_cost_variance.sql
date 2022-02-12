{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.agm_allocated_data_cost_variance ( 
	acct_category        varchar(40)    ,
	super_sbu            varchar(65535)    ,
	alloc_amt            decimal(38,16)    ,
	material             varchar(30)    ,
	soldtocust           varchar(10)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}