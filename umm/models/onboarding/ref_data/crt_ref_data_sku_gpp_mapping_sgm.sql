{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.sku_gpp_mapping_sgm ( 
	ss_fiscal_month_id   integer    ,
	material             varchar(50)  NOT NULL  ,
	gpp_portfolio        varchar(50)  NOT NULL  ,
	audit_loadts         timestamp  NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}