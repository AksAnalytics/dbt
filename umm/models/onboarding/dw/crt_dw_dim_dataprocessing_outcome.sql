{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS dw.dim_dataprocessing_outcome ( 
	dataprocessing_outcome_key bigint    ,
	dataprocessing_outcome_id integer    ,
	dataprocessing_outcome_desc varchar(200)    ,
	dataprocessing_phase varchar(10)    ,
	start_date           date    ,
	end_date             date    ,
	audit_loadts         timestamp    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}