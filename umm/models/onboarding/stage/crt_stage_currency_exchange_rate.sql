{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.currency_exchange_rate ( 
	yearmonthid          integer  NOT NULL  ,
	fromcurrencycode     varchar(10)  NOT NULL  ,
	tocurrencycode       varchar(10)  NOT NULL  ,
	rate                 decimal(12,6)  NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}