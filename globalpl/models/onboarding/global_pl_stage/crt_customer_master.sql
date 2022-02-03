{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl_stage.customer_master
(
	erp_customer_number VARCHAR(500) NOT NULL  
	,erp_customer_address_code VARCHAR(500)   
	,erp_customer_industry_code_1 VARCHAR(500)   
	,erp_customer_industry_code_2 VARCHAR(500)   
	,erp_customer_industry_code_3 VARCHAR(500)   
	,erp_customer_industry_code_4 VARCHAR(500)   
	,erp_customer_industry_code_5 VARCHAR(500)   
	,erp_customer_industry_key VARCHAR(500)   
	,erp_customer_city_code VARCHAR(500)   
	,erp_customer_county_code VARCHAR(500)   
	,erp_customer_country VARCHAR(500)   
	,erp_customer_name VARCHAR(500)   
	,erp_customer_city VARCHAR(500)   
	,erp_customer_district VARCHAR(500)   
	,erp_customer_po_box VARCHAR(500)   
	,erp_customer_po_box_postal_code VARCHAR(500)   
	,erp_customer_postal_code VARCHAR(500)   
	,erp_customer_region VARCHAR(500)   
	,erp_customer_regional_market VARCHAR(500)   
	,erp_source VARCHAR(500) NOT NULL  
	,erp_customer_address VARCHAR(500)   
	,etl_crte_user VARCHAR(500)   
	,etl_crte_ts VARCHAR(500)   
	,hive_loaddatetime VARCHAR(500)   
	,PRIMARY KEY (erp_customer_number, erp_source)
)
DISTSTYLE ALL
 SORTKEY (
	erp_customer_number
	, erp_customer_name
	)
;
ALTER TABLE global_pl_stage.customer_master owner to base_admin; 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}