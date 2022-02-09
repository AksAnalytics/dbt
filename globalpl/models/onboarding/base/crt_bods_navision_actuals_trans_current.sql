{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS bods.navision_actuals_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   
	,eventdts VARCHAR(65535)   
	,rec_src VARCHAR(65535)   
	,row_sqn BIGINT   
	,hash_full_record VARCHAR(65535)   
	,id NUMERIC(38,10)   
	,rectype VARCHAR(65535)   
	,year VARCHAR(65535)   
	,period VARCHAR(65535)   
	,accttype VARCHAR(65535)   
	,entity VARCHAR(65535)   
	,acct VARCHAR(65535)   
	,func VARCHAR(65535)   
	,currkey VARCHAR(65535)   
	,amt NUMERIC(38,10)   
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
	,bar_currtype VARCHAR(65535)   
	,bar_amt NUMERIC(38,10)   
	,bar_bu VARCHAR(65535)   
	,posting_date VARCHAR(65535)   
	,customer VARCHAR(65535)   
	,product VARCHAR(65535)   
	,ship_to VARCHAR(65535)   
	,brand VARCHAR(65535)   
	,usd_amount VARCHAR(65535)   
	,runid VARCHAR(65535)   
	,loaddatetime VARCHAR(65535)   
	,period_partition VARCHAR(65535)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}