{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.sku_barbrand_mapping_sgm ( 
	ss_fiscal_month_id   integer    ,
	material             varchar(30)  NOT NULL  ,
	bar_brand            varchar(22)  NOT NULL  ,
	audit_loadts         timestamp  NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}