{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS bods.baan_powers_cn_pl_trans_archive_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   
	,eventdts VARCHAR(65535)   
	,rec_src VARCHAR(65535)   
	,row_sqn BIGINT   
	,hash_full_record VARCHAR(65535)   
	,id BIGINT   
	,fiscper BIGINT   
	,"year" BIGINT   
	,period BIGINT   
	,cocode VARCHAR(65535)   
	,acct VARCHAR(65535)   
	,doc_num VARCHAR(65535)   
	,doc_line_num VARCHAR(65535)   
	,seq_num VARCHAR(65535)   
	,background_seq_num VARCHAR(65535)   
	,posting_date VARCHAR(65535)   
	,costctr VARCHAR(65535)   
	,amount NUMERIC(38,10)   
	,usd_amount NUMERIC(38,10)   
	,bar_entity VARCHAR(65535)   
	,bar_entity_description VARCHAR(65535)   
	,bar_acct VARCHAR(65535)   
	,bar_scenario VARCHAR(65535)   
	,bar_function VARCHAR(65535)   
	,bar_shipto VARCHAR(65535)   
	,bar_product VARCHAR(65535)   
	,bar_custno VARCHAR(65535)   
	,bar_brand VARCHAR(65535)   
	,bar_year VARCHAR(65535)   
	,bar_period VARCHAR(65535)   
	,bar_currtype VARCHAR(65535)   
	,bar_bu VARCHAR(65535)   
	,bar_amt NUMERIC(38,10)   
	,runid BIGINT   
	,loaddatetime VARCHAR(65535)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}