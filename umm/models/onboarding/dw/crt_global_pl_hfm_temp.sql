{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl.hfm_temp
(
	bar_period VARCHAR(20)   
	,bar_year VARCHAR(20)   
	,bar_function VARCHAR(20)   
	,bar_amt NUMERIC(20,10)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}