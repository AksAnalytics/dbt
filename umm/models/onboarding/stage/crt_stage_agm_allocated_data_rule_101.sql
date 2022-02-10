{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.agm_allocated_data_rule_101 ( 
	source_system        varchar(40)    ,
	fiscal_month_id      integer  NOT NULL  ,
	posting_week_enddate date  NOT NULL  ,
	bar_entity           varchar(5)    ,
	bar_acct             varchar(30)    ,
	material             varchar(50)    ,
	bar_product          varchar(50)    ,
	bar_brand            varchar(50)    ,
	soldtocust           varchar(50)    ,
	shiptocust           varchar(50)    ,
	bar_custno           varchar(50)    ,
	dataprocessing_ruleid integer  NOT NULL  ,
	dataprocessing_outcome_id integer  NOT NULL  ,
	dataprocessing_phase varchar(10)    ,
	bar_currtype         varchar(4)    ,
	super_sbu            varchar(50)    ,
	cost_pool            varchar(10)    ,
	allocated_amt        decimal(38,8)    ,
	allocated_amt_usd    decimal(38,8)    ,
	audit_loadts         date DEFAULT current_date NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}