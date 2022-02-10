{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.core_tran_delta_cleansed ( 
	audit_rec_src        varchar(10)    ,
	document_no          varchar(30)    ,
	document_line        varchar(30)    ,
	bar_year             varchar(4)    ,
	bar_period           varchar(10)    ,
	bar_entity           varchar(5)    ,
	bar_acct             varchar(6)    ,
	shiptocust           varchar(10)    ,
	soldtocust           varchar(10)    ,
	material             varchar(30)    ,
	org_bar_custno       varchar(20)    ,
	mapped_bar_custno    varchar(20)    ,
	org_bar_product      varchar(22)    ,
	mapped_bar_product   varchar(22)    ,
	org_bar_brand        varchar(22)    ,
	mapped_bar_brand     varchar(22)    ,
	bar_currtype         varchar(4)    ,
	postdate             date    ,
	posting_week_enddate date    ,
	fiscal_month_id      integer    ,
	bar_amt              decimal(38,8)    ,
	quantity             decimal(38,8)    ,
	quanunit             varchar(10)    ,
	org_dataprocessing_hash varchar(40)    ,
	mapped_dataprocessing_hash varchar(40)    ,
	audit_loadts         timestamp    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}