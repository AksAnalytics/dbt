{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.sku_gpp_mapping ( 
	material             varchar(30)  NOT NULL  ,
	gpp_portfolio        varchar(10)  NOT NULL  ,
	gpp_division         varchar(2)  NOT NULL  ,
	start_date           date  NOT NULL  ,
	end_date             date  NOT NULL  ,
	current_flag         smallint  NOT NULL  ,
	audit_loadts         timestamp  NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}