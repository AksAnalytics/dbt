{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS bods.nelson_asmp_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   
	,eventdts VARCHAR(65535)   
	,rec_src VARCHAR(65535)   
	,row_sqn BIGINT   
	,hash_full_record VARCHAR(65535)   
	,id BIGINT   
	,fiscper VARCHAR(65535)   
	,company VARCHAR(65535)   
	,fiscal_year VARCHAR(65535)   
	,fiscal_period VARCHAR(65535)   
	,posted_date VARCHAR(65535)   
	,je_date VARCHAR(65535)   
	,posted_by VARCHAR(65535)   
	,posted VARCHAR(65535)   
	,journal_num VARCHAR(65535)   
	,journal_line VARCHAR(65535)   
	,description VARCHAR(65535)   
	,debit_amount NUMERIC(38,10)   
	,credit_amount NUMERIC(38,10)   
	,ar_invoice_num VARCHAR(65535)   
	,gl_account VARCHAR(65535)   
	,account_desc VARCHAR(65535)   
	,groupid VARCHAR(65535)   
	,source_module VARCHAR(65535)   
	,journal_code VARCHAR(65535)   
	,statistical VARCHAR(65535)   
	,rundate_4 VARCHAR(65535)   
	,bar_geo VARCHAR(65535)   
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
	,runid BIGINT   
	,loaddatetime VARCHAR(65535)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}