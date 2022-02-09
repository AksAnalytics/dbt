{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl.bar_acct_attr
(
	bar_account VARCHAR(250) NOT NULL  
	,bar_account_desc VARCHAR(250)   
	,bar_acct_type_lvl1 VARCHAR(250)   
	,bar_acct_type_lvl2 VARCHAR(250)   
	,bar_acct_type_lvl3 VARCHAR(250)   
	,bar_acct_type_lvl4 VARCHAR(250)   
	,indirect_flag VARCHAR(250)   
	,flipsign VARCHAR(250)   
	,etl_crte_user VARCHAR(100)   
	,etl_crte_ts DATE   
	,etl_updt_user VARCHAR(100)   
	,etl_updt_ts DATE   
	,PRIMARY KEY (bar_account)
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}