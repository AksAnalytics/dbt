{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.parent_product_hierarchy_allocation_mapping ( 
	member_type          varchar(10)    ,
	name                 varchar(50)    ,
	superior1            varchar(50)    ,
	superior2            varchar(50)    ,
	superior3            varchar(50)    ,
	start_date           date    ,
	end_date             date    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}