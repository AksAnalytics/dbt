{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl.testsp
(
	id INTEGER  DEFAULT "identity"(985663, 0, '1,1'::text) 
	,dtrun VARCHAR(100)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}