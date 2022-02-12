{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.pnl_acct ( 
	bar_acct             varchar(10)  NOT NULL  ,
	bar_acct_desc        varchar(100)    ,
	acct_type            varchar(20)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}