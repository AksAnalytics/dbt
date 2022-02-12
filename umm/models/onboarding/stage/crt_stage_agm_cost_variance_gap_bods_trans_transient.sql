{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.agm_cost_variance_gap_bods_trans_transient ( 
	super_sbu            varchar(100)    ,
	material             varchar(30)    ,
	acct_category        varchar(40)    ,
	amt_usd              decimal(38,18)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}