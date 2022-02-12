{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS dw.fact_pnl_commercial_stacked ( 
	fact_pnl_commercial_stacked_id bigint identity NOT NULL  ,
	org_tranagg_id       bigint    ,
	org_dataprocessing_ruleid integer    ,
	mapped_dataprocessing_ruleid integer    ,
	dataprocessing_outcome_key integer    ,
	bar_acct             varchar(6)    ,
	bar_currtype         varchar(6)    ,
	posting_week_enddate date    ,
	fiscal_month_id      integer    ,
	scenario_id          integer    ,
	source_system_id     integer    ,
	business_unit_key    bigint    ,
	customer_key         bigint    ,
	product_key          bigint    ,
	org_bar_custno       varchar(50)    ,
	org_bar_product      varchar(50)    ,
	org_bar_brand        varchar(50)    ,
	mapped_bar_custno    varchar(50)    ,
	mapped_bar_product   varchar(50)    ,
	mapped_bar_brand     varchar(50)    ,
	org_soldtocust       varchar(50)    ,
	org_shiptocust       varchar(50)    ,
	org_material         varchar(50)    ,
	alloc_soldtocust     varchar(50)    ,
	alloc_shiptocust     varchar(50)    ,
	alloc_material       varchar(50)    ,
	alloc_bar_product    varchar(50)    ,
	allocated_flag       boolean    ,
	amt                  decimal(38,8)    ,
	amt_usd              decimal(38,8)    ,
	tran_volume          decimal(19,8)    ,
	sales_volume         decimal(38,8)    ,
	uom                  varchar(10)    ,
	dim_transactional_attributes_id varchar(300)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}