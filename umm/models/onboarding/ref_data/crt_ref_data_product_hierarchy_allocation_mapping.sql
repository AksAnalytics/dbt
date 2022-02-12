{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.product_hierarchy_allocation_mapping ( 
	allocation_mapping_key integer identity   ,
	membertype           varchar(10)    ,
	name                 varchar(200)    ,
	superior1            varchar(200)    ,
	superior2            varchar(200)    ,
	superior3            varchar(200)    ,
	description          varchar(400)    ,
	plnlevel             varchar(20)    ,
	generation           integer    ,
	start_date           date    ,
	end_date             date    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}