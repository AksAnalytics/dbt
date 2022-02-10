{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.demand_group_to_bar_customer_mapping ( 
	demand_group         varchar(30)    ,
	bar_customer         varchar(30)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}