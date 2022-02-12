{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.sgm_allocated_data_rule_26 ( 
	audit_rec_sqn        bigint identity NOT NULL  ,
	source_system        varchar(10)    ,
	org_tranagg_id       bigint    ,
	posting_week_enddate date  NOT NULL  ,
	fiscal_month_id      integer  NOT NULL  ,
	bar_entity           varchar(5)    ,
	bar_acct             varchar(6)    ,
	org_bar_brand        varchar(50)    ,
	org_bar_custno       varchar(50)    ,
	org_bar_product      varchar(50)    ,
	mapped_bar_brand     varchar(50)    ,
	mapped_bar_custno    varchar(50)    ,
	mapped_bar_product   varchar(50)    ,
	org_shiptocust       varchar(50)    ,
	org_soldtocust       varchar(50)    ,
	org_material         varchar(50)    ,
	alloc_shiptocust     varchar(50)    ,
	alloc_soldtocust     varchar(50)    ,
	alloc_material       varchar(50)    ,
	alloc_bar_product    varchar(50)    ,
	bar_currtype         varchar(4)    ,
	org_dataprocessing_ruleid integer  NOT NULL  ,
	mapped_dataprocessing_ruleid integer  NOT NULL  ,
	dataprocessing_outcome_id integer  NOT NULL  ,
	dataprocessing_phase varchar(10)    ,
	allocated_amt        decimal(38,8)    ,
	sales_volume         decimal(38,8)    ,
	tran_volume          decimal(38,8)    ,
	uom                  varchar(20)    ,
	audit_loadts         date DEFAULT current_date NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}