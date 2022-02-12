{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS bods.jde_na_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   
	,eventdts VARCHAR(65535)   
	,rec_src VARCHAR(65535)   
	,row_sqn BIGINT   
	,hash_full_record VARCHAR(65535)   
	,id VARCHAR(65535)   
	,company VARCHAR(65535)   
	,year VARCHAR(65535)   
	,amount NUMERIC(38,10)   
	,entity VARCHAR(65535)   
	,fiscper VARCHAR(65535)   
	,period VARCHAR(65535)   
	,account VARCHAR(65535)   
	,account_sub VARCHAR(65535)   
	,leg_num VARCHAR(65535)   
	,hyperion VARCHAR(65535)   
	,hyperion_code VARCHAR(65535)   
	,plant VARCHAR(65535)   
	,sold_to_customer NUMERIC(38,10)   
	,end_customer VARCHAR(65535)   
	,customer NUMERIC(38,10)   
	,parent NUMERIC(38,10)   
	,used_customer NUMERIC(38,10)   
	,short_id VARCHAR(65535)   
	,product NUMERIC(38,10)   
	,ship_to VARCHAR(65535)   
	,brand VARCHAR(65535)   
	,quantity NUMERIC(38,10)   
	,currency_code VARCHAR(65535)   
	,transaction_date VARCHAR(65535)   
	,posting_date VARCHAR(65535)   
	,document_type VARCHAR(65535)   
	,document_id VARCHAR(65535)   
	,usd_amount NUMERIC(38,10)   
	,int_functype VARCHAR(65535)   
	,product_class VARCHAR(65535)   
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