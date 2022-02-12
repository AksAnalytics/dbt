{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.rate_base_cogs ( 
	audit_rec_src        varchar(10)    ,
	fiscal_month_id      integer    ,
	bar_entity           varchar(5)    ,
	bar_currtype         varchar(10)    ,
	soldtocust           varchar(10)    ,
	shiptocust           varchar(10)    ,
	bar_custno           varchar(20)    ,
	material             varchar(30)    ,
	bar_product          varchar(30)    ,
	bar_brand            varchar(30)    ,
	super_sbu            varchar(50)    ,
	cost_pool            varchar(50)    ,
	total_bar_amt        decimal(38,18)    ,
	total_bar_amt_usd    decimal(38,18)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}