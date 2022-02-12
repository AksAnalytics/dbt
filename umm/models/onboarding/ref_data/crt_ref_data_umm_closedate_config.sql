{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.umm_closedate_config ( 
	rownumber            bigint    ,
	fiscal_month_id      integer    ,
	fiscal_close_date    timestamp    ,
	fiscal_month_enddate timestamp    ,
	fiscal_wklyjob_start_date timestamp    ,
	finance_close_date   timestamp    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}