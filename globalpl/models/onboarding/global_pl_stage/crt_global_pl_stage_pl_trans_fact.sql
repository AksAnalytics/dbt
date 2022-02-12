{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl_stage.pl_trans_fact
(
	bar_account VARCHAR(500)   
	,bar_amt_lc NUMERIC(38,10)   
	,bar_brand VARCHAR(500)   
	,bar_bu VARCHAR(500)   
	,bar_currtype VARCHAR(500)   
	,bar_customer VARCHAR(500)   
	,bar_entity VARCHAR(500)   
	,bar_function VARCHAR(500)   
	,bar_period VARCHAR(500)   
	,bar_product VARCHAR(500)   
	,bar_scenario VARCHAR(500)   
	,bar_shipto VARCHAR(500)   
	,bar_year VARCHAR(500)   
	,bar_fiscal_period VARCHAR(500)   
	,erp_account VARCHAR(500)   
	,erp_brand_code VARCHAR(500)   
	,erp_business_area VARCHAR(500)   
	,erp_company_code VARCHAR(500)   
	,erp_cost_center VARCHAR(500)   
	,erp_doc_type VARCHAR(500)   
	,erp_doc_line_num VARCHAR(500)   
	,erp_doc_num VARCHAR(500)   
	,erp_document_text VARCHAR(1000)   
	,erp_vendor VARCHAR(500)   
	,erp_material VARCHAR(500)   
	,erp_customer_parent VARCHAR(500)   
	,erp_posting_date VARCHAR(500)   
	,erp_quantity NUMERIC(38,10)   
	,erp_quantity_uom VARCHAR(500)   
	,erp_ref_doc_type VARCHAR(500)   
	,erp_ref_doc_line_num VARCHAR(500)   
	,erp_ref_doc_num VARCHAR(500)   
	,erp_profit_center VARCHAR(500)   
	,erp_sales_group VARCHAR(500)   
	,erp_sales_office VARCHAR(500)   
	,erp_customer_ship_to VARCHAR(500)   
	,erp_customer_sold_to VARCHAR(500)   
	,erp_plant VARCHAR(500)   
	,bar_bods_loaddatetime VARCHAR(500)   
	,erp_chartaccts VARCHAR(500)   
	,bar_bods_record_id VARCHAR(500) NOT NULL  
	,erp_source VARCHAR(500)   
	,bar_s_entity_currency VARCHAR(50)   
	,bar_s_curr_rate_actual NUMERIC(38,10)   
	,bar_amt_usd NUMERIC(38,10)   
	,etl_crte_user VARCHAR(50)   
	,etl_crte_ts VARCHAR(50)   
	,hive_loaddatetime VARCHAR(250)   
	,PRIMARY KEY (bar_bods_record_id)
)
DISTSTYLE KEY
 DISTKEY (bar_bods_record_id)
 SORTKEY (
	bar_account
	, bar_customer
	, bar_entity
	, bar_product
	, bar_period
	, bar_year
	, bar_bods_record_id
	, erp_source
	)
;
ALTER TABLE global_pl_stage.pl_trans_fact owner to base_admin; 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}