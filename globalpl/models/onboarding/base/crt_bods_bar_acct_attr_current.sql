{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS bods.bar_acct_attr_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   
	,eventdts VARCHAR(65535)   
	,rec_src VARCHAR(65535)   
	,row_sqn BIGINT   
	,hash_full_record VARCHAR(65535)   
	,bar_account VARCHAR(65535)   
	,bar_account_desc VARCHAR(65535)   
	,bar_acct_type_lvl1 VARCHAR(65535)   
	,bar_acct_type_lvl2 VARCHAR(65535)   
	,bar_acct_type_lvl3 VARCHAR(65535)   
	,bar_acct_type_lvl4 VARCHAR(65535)   
	,indirect_flag VARCHAR(65535)   
	,flipsign VARCHAR(65535)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}