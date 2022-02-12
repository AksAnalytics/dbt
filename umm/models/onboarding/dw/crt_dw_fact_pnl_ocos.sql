{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS dw.fact_pnl_ocos ( 
	fact_pnl_ocos_id     bigint identity NOT NULL  ,
	org_tranagg_id       bigint    ,
	dataprocessing_ruleid integer    ,
	dataprocessing_outcome_key integer    ,
	bar_acct             varchar(30)    ,
	bar_currtype         varchar(6)    ,
	posting_week_enddate date    ,
	fiscal_month_id      integer    ,
	scenario_id          integer    ,
	source_system_id     integer    ,
	business_unit_key    bigint    ,
	customer_key         bigint    ,
	product_key          bigint    ,
	soldtocust           varchar(30)    ,
	shiptocust           varchar(30)    ,
	bar_custno           varchar(30)    ,
	material             varchar(30)    ,
	bar_product          varchar(30)    ,
	bar_brand            varchar(30)    ,
	cost_pool            varchar(10)    ,
	super_sbu            varchar(100)    ,
	amt                  decimal(38,8)    ,
	amt_usd              decimal(38,8)    ,
	reported_inventory_adjustment decimal(38,8)    ,
	reported_warranty_cost decimal(38,8)    ,
	reported_duty_tariffs decimal(38,8)    ,
	reported_freight     decimal(38,8)    ,
	reported_ppv         decimal(38,8)    ,
	reported_labor_overhead decimal(38,8)    ,
	tran_volume          decimal(19,8)    ,
	sales_volume         decimal(38,8)    ,
	uom                  varchar(10)    ,
	audit_loadts         date  NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}