{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS dw.dim_customer_restatement ( 
	soldto_number        varchar(50)    ,
	base_customer        varchar(50)    ,
	base_customer_desc   varchar(100)    ,
	level01_bar          varchar(100)    ,
	level02_bar          varchar(100)    ,
	level03_bar          varchar(100)    ,
	level04_bar          varchar(100)    ,
	level05_bar          varchar(100)    ,
	level06_bar          varchar(100)    ,
	level07_bar          varchar(100)    ,
	level08_bar          varchar(100)    ,
	level09_bar          varchar(100)    ,
	level10_bar          varchar(100)    ,
	level11_bar          varchar(100)    ,
	demand_group         varchar(100)    ,
	a2                   varchar(100)    ,
	a1                   varchar(100)    ,
	a2_desc              varchar(100)    ,
	a1_desc              varchar(100)    ,
	level01_commercial   varchar(100)    ,
	level02_commercial   varchar(100)    ,
	level03_commercial   varchar(100)    ,
	level04_commercial   varchar(100)    ,
	level05_commercial   varchar(100)    ,
	level06_commercial   varchar(100)    ,
	hierarchy_b_id       varchar(100)    ,
	hierarchy_b_desc     varchar(100)    ,
	hierarchy_c_id       varchar(100)    ,
	hierarchy_c_desc     varchar(100)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}