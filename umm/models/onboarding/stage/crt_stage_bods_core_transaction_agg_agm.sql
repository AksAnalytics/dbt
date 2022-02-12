{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.bods_core_transaction_agg_agm ( 
	org_tranagg_agm_id   bigint identity NOT NULL  ,
	audit_rec_src        varchar(10)  NOT NULL  ,
	fiscal_month_id      integer  NOT NULL  ,
	posting_week_enddate date  NOT NULL  ,
	bar_entity           varchar(5)  NOT NULL  ,
	bar_acct_category    varchar(50)    ,
	bar_acct             varchar(10)  NOT NULL  ,
	shiptocust           varchar(50)    ,
	soldtocust           varchar(50)    ,
	bar_custno           varchar(50)    ,
	material             varchar(50)    ,
	bar_product          varchar(50)    ,
	bar_brand            varchar(50)    ,
	bar_amt              decimal(38,8)  NOT NULL  ,
	bar_amt_usd          decimal(38,8)  NOT NULL  ,
	bar_currtype         varchar(10)  NOT NULL  ,
	tran_volume          decimal(38,8)  NOT NULL  ,
	uom                  varchar(20)    ,
	dataprocessing_ruleid integer  NOT NULL  ,
	audit_loadts         date DEFAULT current_date NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}