{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.product_commercial_hierarchy ( 
	gpp_portfolio        varchar(50)  NOT NULL  ,
	gts                  varchar(50)    ,
	super_bu             varchar(50)    ,
	subcategory          varchar(50)    ,
	category             varchar(50)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}