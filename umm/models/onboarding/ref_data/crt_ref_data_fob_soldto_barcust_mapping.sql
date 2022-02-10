{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.fob_soldto_barcust_mapping ( 
	soldtocust           varchar(20)  NOT NULL  ,
	bar_custno           varchar(30)  NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}