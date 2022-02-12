{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.bods_core_transaction_agg ( 
	org_tranagg_id       bigint identity NOT NULL  ,
	audit_rec_src        varchar(10)  NOT NULL  ,
	bar_year             varchar(4)  NOT NULL  ,
	bar_period           varchar(10)  NOT NULL  ,
	bar_entity           varchar(5)  NOT NULL  ,
	bar_acct             varchar(6)  NOT NULL  ,
	shiptocust           varchar(10)    ,
	soldtocust           varchar(10)    ,
	org_bar_custno       varchar(20)    ,
	mapped_bar_custno    varchar(20)    ,
	material             varchar(30)    ,
	org_bar_product      varchar(22)    ,
	mapped_bar_product   varchar(22)    ,
	org_bar_brand        varchar(22)    ,
	mapped_bar_brand     varchar(22)    ,
	bar_amt              decimal(38,8)  NOT NULL  ,
	bar_currtype         varchar(10)  NOT NULL  ,
	sales_volume         decimal(38,8)  NOT NULL  ,
	tran_volume          decimal(38,8)  NOT NULL  ,
	uom                  varchar(20)    ,
	posting_week_enddate date  NOT NULL  ,
	fiscal_month_id      integer  NOT NULL  ,
	org_dataprocessing_ruleid integer  NOT NULL  ,
	mapped_dataprocessing_ruleid integer  NOT NULL  ,
	audit_loadts         date DEFAULT current_date NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}