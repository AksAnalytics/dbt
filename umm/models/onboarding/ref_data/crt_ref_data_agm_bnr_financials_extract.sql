{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.agm_bnr_financials_extract (
	scenario             varchar(50)    ,
	brand                varchar(50)    ,
	customer             varchar(50)    ,
	shipto_geography     varchar(50)    ,
	func                 varchar(50)    ,
	entity               varchar(50)    ,
	product              varchar(50)    ,
	fiscal_month_id      integer  NOT NULL  ,
	account              varchar(50)    ,
	amt_local_cur        decimal(25,9)    ,
	amt_reported         decimal(25,9)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}