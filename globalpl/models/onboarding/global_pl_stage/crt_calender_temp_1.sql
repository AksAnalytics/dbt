{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl_stage.calender_temp_1
(
	id INTEGER   
	,date DATE   
	,"year" SMALLINT   
	,"month" SMALLINT   
	,"day" SMALLINT   
	,quarter SMALLINT   
	,week SMALLINT   
	,day_name VARCHAR(9)   
	,month_name VARCHAR(9)   
	,holiday_flag BOOLEAN   
	,weekend_flag BOOLEAN   
	,fiscper VARCHAR(15)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}