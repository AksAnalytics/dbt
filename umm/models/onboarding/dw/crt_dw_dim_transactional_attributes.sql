{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS dw.dim_transactional_attributes ( 
	dim_transactional_attributes_id varchar(300)  NOT NULL  ,
	pcr                  varchar(300)  NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}