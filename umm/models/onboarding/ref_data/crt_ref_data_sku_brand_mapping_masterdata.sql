{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.sku_brand_mapping_masterdata ( 
	material             varchar(50)  NOT NULL  ,
	brand_code           varchar(50)  NOT NULL  ,
	brand_map            varchar(50)  NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}