{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS bods.byd_pl_trans_archive_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   
	,eventdts VARCHAR(65535)   
	,rec_src VARCHAR(65535)   
	,row_sqn BIGINT   
	,hash_full_record VARCHAR(65535)   
	,id BIGINT   
	,fiscper VARCHAR(65535)   
	,fiscal_year VARCHAR(65535)   
	,accounting_period VARCHAR(65535)   
	,company_id VARCHAR(65535)   
	,gl_account VARCHAR(65535)   
	,chart_of_accounts VARCHAR(65535)   
	,cost_center VARCHAR(65535)   
	,profit_center VARCHAR(65535)   
	,segment VARCHAR(65535)   
	,project_id VARCHAR(65535)   
	,ship_to_customer VARCHAR(65535)   
	,bill_to_customer VARCHAR(65535)   
	,payer VARCHAR(65535)   
	,product_id VARCHAR(65535)   
	,site VARCHAR(65535)   
	,ship_to_country VARCHAR(65535)   
	,cost_center_country VARCHAR(65535)   
	,gpp_div VARCHAR(65535)   
	,journal_entry VARCHAR(65535)   
	,journal_entry_item VARCHAR(65535)   
	,source_document_id VARCHAR(65535)   
	,document_type VARCHAR(65535)   
	,business_transaction_type VARCHAR(65535)   
	,posting_date VARCHAR(65535)   
	,company_currency VARCHAR(65535)   
	,amount_in_company_currency VARCHAR(38)   
	,customer_channel_code VARCHAR(65535)   
	,int_brandgrp VARCHAR(65535)   
	,int_functype VARCHAR(65535)   
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
	,bar_amt VARCHAR(38)   
	,invoiced_quantity VARCHAR(38)   
	,runid VARCHAR(65535)   
	,loaddatetime VARCHAR(65535)   
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}