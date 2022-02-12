{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.data_processing_rule_agm ( 
	data_processing_ruleid integer  NOT NULL  ,
	bar_acct_category    varchar(100)    ,
	dataprocessing_group varchar(200)    ,
	dataprocessing_rule_description varchar(400)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}