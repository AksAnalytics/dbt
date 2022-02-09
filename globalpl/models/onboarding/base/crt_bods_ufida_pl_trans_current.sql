{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS bods.ufida_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   
	,eventdts VARCHAR(65535)   
	,rec_src VARCHAR(65535)   
	,row_sqn BIGINT   
	,hash_full_record VARCHAR(65535)   
	,id BIGINT   
	,fiscper VARCHAR(65535)   
	,year VARCHAR(65535)   
	,period VARCHAR(65535)   
	,entity VARCHAR(65535)   
	,glaccount_code VARCHAR(65535)   
	,glaccount_name VARCHAR(65535)   
	,customer_code VARCHAR(65535)   
	,department_code VARCHAR(65535)   
	,posting_date VARCHAR(65535)   
	,person_id VARCHAR(65535)   
	,currency_code VARCHAR(65535)   
	,vouchno VARCHAR(65535)   
	,product_code VARCHAR(65535)   
	,cdccode VARCHAR(65535)   
	,gemcusdchod VARCHAR(65535)   
	,gppport VARCHAR(65535)   
	,amount NUMERIC(38,10)   
	,amount_rmb NUMERIC(38,10)   
	,bar_entity VARCHAR(65535)   
	,bar_acct VARCHAR(65535)   
	,bar_function VARCHAR(65535)   
	,bar_custno VARCHAR(65535)   
	,bar_shipto VARCHAR(65535)   
	,bar_product VARCHAR(65535)   
	,bar_brand VARCHAR(65535)   
	,bar_scenario VARCHAR(65535)   
	,bar_year VARCHAR(65535)   
	,bar_period VARCHAR(65535)   
	,bar_currtype VARCHAR(65535)   
	,bar_bu VARCHAR(65535)   
	,bar_amt NUMERIC(38,10)   
	,product_desc VARCHAR(65535)   
	,product_qty NUMERIC(38,10)   
	,runid VARCHAR(65535)   
	,loaddatetime VARCHAR(65535)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}