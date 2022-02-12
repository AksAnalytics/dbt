{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS dw.dim_business_unit ( 
	business_unit_key    bigint identity NOT NULL  ,
	bar_entity           varchar(20)    ,
	bar_entity_description varchar(200)    ,
	geography            varchar(100)    ,
	region               varchar(30)    ,
	subregion            varchar(30)    ,
	start_date           date  NOT NULL  ,
	end_date             date  NOT NULL  ,
	audit_loadts         date  NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}