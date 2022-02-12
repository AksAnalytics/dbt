{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.volume_conv_sku ( 
	sku                  varchar(10)    ,
	conversionrate       decimal(19,2)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}