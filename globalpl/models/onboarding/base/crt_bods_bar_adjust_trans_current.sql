{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS bods.bar_adjust_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   
	,eventdts VARCHAR(65535)   
	,rec_src VARCHAR(65535)   
	,row_sqn BIGINT   
	,hash_full_record VARCHAR(65535)   
	,id VARCHAR(65535)   
	,ticket VARCHAR(65535)   
	,note VARCHAR(65535)   
	,fiscper VARCHAR(65535)   
	,user_entity VARCHAR(65535)   
	,user_acct VARCHAR(65535)   
	,user_function VARCHAR(65535)   
	,user_custno VARCHAR(65535)   
	,user_product VARCHAR(65535)   
	,user_shipto VARCHAR(65535)   
	,user_brand VARCHAR(65535)   
	,user_amt NUMERIC(38,10)   
	,data_group VARCHAR(65535)   
	,update_user VARCHAR(65535)   
	,bar_acct VARCHAR(65535)   
	,bar_shipto VARCHAR(65535)   
	,bar_entity VARCHAR(65535)   
	,bar_function VARCHAR(65535)   
	,bar_product VARCHAR(65535)   
	,bar_brand VARCHAR(65535)   
	,bar_scenario VARCHAR(65535)   
	,bar_custno VARCHAR(65535)   
	,bar_year VARCHAR(65535)   
	,bar_period VARCHAR(65535)   
	,bar_currtype VARCHAR(65535)   
	,bar_bu VARCHAR(65535)   
	,bar_amt NUMERIC(38,10)   
	,runid VARCHAR(65535)   
	,loaddatetime VARCHAR(65535)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}