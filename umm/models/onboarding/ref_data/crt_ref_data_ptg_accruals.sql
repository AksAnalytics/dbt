{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS ref_data.ptg_accruals ( 
	gl_acct              varchar(50)  NOT NULL  ,
	amt                  decimal(19,8)  NOT NULL  ,
	amt_usd              decimal(19,8)  NOT NULL  ,
	currkey              varchar(10)  NOT NULL  ,
	fiscal_month_id      integer  NOT NULL  ,
	posting_week_enddate date  NOT NULL  ,
	audit_loadts         timestamp  NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}