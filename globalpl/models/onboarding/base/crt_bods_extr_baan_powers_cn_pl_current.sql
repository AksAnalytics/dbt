{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS bods.extr_baan_powers_cn_pl_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   
	,eventdts VARCHAR(65535)   
	,rec_src VARCHAR(65535)   
	,row_sqn BIGINT   
	,hash_full_record VARCHAR(65535)   
	,extr_baan_powers_cn_pl_id BIGINT   
	,fiscper BIGINT   
	,year BIGINT   
	,period BIGINT   
	,co_code VARCHAR(65535)   
	,account VARCHAR(65535)   
	,doc_num VARCHAR(65535)   
	,doc_line_num BIGINT   
	,seq_num BIGINT   
	,background_seq_num BIGINT   
	,posting_date VARCHAR(65535)   
	,cost_center VARCHAR(65535)   
	,currency VARCHAR(65535)   
	,amount NUMERIC(38,10)   
	,usd_amount NUMERIC(38,10)   
	,runid BIGINT   
	,loaddatetime VARCHAR(65535)   
	,ins_dtm VARCHAR(65535)   
	,upd_dtm VARCHAR(65535)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}