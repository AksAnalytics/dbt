{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS bods.orch_ppe_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   
	,eventdts VARCHAR(65535)   
	,rec_src VARCHAR(65535)   
	,row_sqn BIGINT   
	,hash_full_record VARCHAR(65535)   
	,id BIGINT   
	,fiscper BIGINT   
	,fyr_id BIGINT   
	,fmth_nbr BIGINT   
	,co_key VARCHAR(65535)   
	,co_cd VARCHAR(65535)   
	,acct VARCHAR(65535)   
	,icp_cd VARCHAR(65535)   
	,txn_id VARCHAR(65535)   
	,post_dte VARCHAR(65535)   
	,crncy_cd VARCHAR(65535)   
	,amt NUMERIC(38,10)   
	,usd_amt NUMERIC(38,10)   
	,int_functype VARCHAR(65535)   
	,bar_acct VARCHAR(65535)   
	,bar_function VARCHAR(65535)   
	,bar_entity VARCHAR(65535)   
	,bar_shipto VARCHAR(65535)   
	,bar_product VARCHAR(65535)   
	,bar_brand VARCHAR(65535)   
	,bar_custno VARCHAR(65535)   
	,bar_scenario VARCHAR(65535)   
	,bar_year VARCHAR(65535)   
	,bar_period VARCHAR(65535)   
	,bar_currtype BIGINT   
	,bar_amt NUMERIC(38,10)   
	,bar_bu VARCHAR(65535)   
	,runid BIGINT   
	,loaddatetime VARCHAR(65535)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}