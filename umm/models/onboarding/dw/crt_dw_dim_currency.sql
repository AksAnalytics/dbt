{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS dw.dim_currency ( 
	currency_cd          varchar(20)  NOT NULL  ,
	currency_format      varchar(20)    ,
	currency_sort        integer    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}