{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl.testsp
(
	id INTEGER  DEFAULT IDENTITY(985663, 0) 
	,dtrun VARCHAR(100)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}