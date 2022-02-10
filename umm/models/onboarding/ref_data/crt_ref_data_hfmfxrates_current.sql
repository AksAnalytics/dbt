{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.hfmfxrates_current ( 
	id                   integer identity NOT NULL  ,
	fiscal_month_begin_date date    ,
	bar_year             integer    ,
	bar_period           varchar(3)    ,
	fiscal_month_id      integer    ,
	fxrate               decimal(38,10)    ,
	from_currtype        varchar(10)    ,
	to_currtype          varchar(10)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}