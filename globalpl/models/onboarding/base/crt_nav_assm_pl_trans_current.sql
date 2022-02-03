{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS bods.nav_assm_pl_trans_current
(
	loaddts TIMESTAMP WITHOUT TIME ZONE   
	,eventdts VARCHAR(65535)   
	,rec_src VARCHAR(65535)   
	,row_sqn BIGINT   
	,hash_full_record VARCHAR(65535)   
	,id VARCHAR(65535)   
	,"account" VARCHAR(65535)   
	,period VARCHAR(65535)   
	,department_exp VARCHAR(65535)   
	,expenses_type VARCHAR(65535)   
	,sold_to_customer VARCHAR(65535)   
	,sold_to_customer_name VARCHAR(65535)   
	,sold_to_customer__channel VARCHAR(65535)   
	,bill_to_customer VARCHAR(65535)   
	,product VARCHAR(65535)   
	,ship_to_customer VARCHAR(65535)   
	,qty NUMERIC(38,10)   
	,currency_code VARCHAR(65535)   
	,transaction_date VARCHAR(65535)   
	,job_number VARCHAR(65535)   
	,document_type VARCHAR(65535)   
	,document_id VARCHAR(65535)   
	,amount NUMERIC(38,10)   
	,fiscper VARCHAR(65535)   
	,int_functype VARCHAR(65535)   
	,product_group_code VARCHAR(65535)   
	,dimension_code_prod VARCHAR(65535)   
	,dimension_code_cust VARCHAR(65535)   
	,customer_group_code VARCHAR(65535)   
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