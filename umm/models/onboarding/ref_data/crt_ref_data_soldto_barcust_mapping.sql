{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.soldto_barcust_mapping ( 
	soldtocust           varchar(10)  NOT NULL  ,
	bar_custno           varchar(20)  NOT NULL  ,
	start_date           date  NOT NULL  ,
	end_date             date  NOT NULL  ,
	current_flag         smallint  NOT NULL  ,
	audit_loadts         timestamp  NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}