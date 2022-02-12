{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS bods.qad_cca_pl_trans_current
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
	,qad_entity VARCHAR(65535)   
	,acct VARCHAR(65535)   
	,profit_center VARCHAR(65535)   
	,costctr VARCHAR(65535)   
	,functional_area VARCHAR(65535)   
	,site VARCHAR(65535)   
	,sold_to_customer VARCHAR(65535)   
	,bill_to_customer VARCHAR(65535)   
	,product VARCHAR(65535)   
	,ship_to VARCHAR(65535)   
	,ship_to_customer_country VARCHAR(65535)   
	,sold_to_customer_chanel VARCHAR(65535)   
	,currency_code VARCHAR(65535)   
	,transaction_date VARCHAR(65535)   
	,posting_date VARCHAR(65535)   
	,document_type VARCHAR(65535)   
	,document_id VARCHAR(65535)   
	,amount NUMERIC(38,10)   
	,usd_amount NUMERIC(38,10)   
	,product_line VARCHAR(65535)   
	,product_group VARCHAR(65535)   
	,product_class VARCHAR(65535)   
	,product_sub_class VARCHAR(65535)   
	,product_category VARCHAR(65535)   
	,bar_shipto VARCHAR(65535)   
	,bar_entity VARCHAR(65535)   
	,int_entitytype VARCHAR(65535)   
	,bar_acct VARCHAR(65535)   
	,bar_function VARCHAR(65535)   
	,bar_product VARCHAR(65535)   
	,bar_custno VARCHAR(65535)   
	,bar_brand VARCHAR(65535)   
	,bar_scenario VARCHAR(65535)   
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