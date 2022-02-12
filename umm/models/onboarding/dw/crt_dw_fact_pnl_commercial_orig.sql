{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS dw.fact_pnl_commercial_orig ( 
	org_tranagg_id       bigint    ,
	posting_week_enddate date    ,
	fiscal_month_id      integer    ,
	bar_currtype         varchar(6)    ,
	amt                  decimal(38,8)    ,
	amt_usd              decimal(38,8)    ,
	tran_volume          decimal(19,8)    ,
	sales_volume         decimal(38,8)    ,
	uom                  varchar(10)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}