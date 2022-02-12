{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.rsa_bible ( 
	source_system        varchar(50)    ,
	demand_group         varchar(30)    ,
	division             varchar(30)    ,
	brand                varchar(30)    ,
	sku                  varchar(30)    ,
	fiscal_month_id      integer    ,
	amt                  decimal(38,8)    ,
	amt_str              varchar(30)    ,
	pcr                  varchar(300)    ,
	mgsv                 varchar(30)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}