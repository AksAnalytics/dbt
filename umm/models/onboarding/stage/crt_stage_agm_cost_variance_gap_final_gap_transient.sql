{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.agm_cost_variance_gap_final_gap_transient ( 
	acct_category        varchar(40)    ,
	super_sbu            varchar(100)    ,
	bar_cost             decimal(38,9)    ,
	calc_cost            decimal(38,8)    ,
	bar_bods             decimal(20,8)    ,
	prcnt_of_gap         decimal(38,4)    ,
	gap_to_alloc         decimal(38,12)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}