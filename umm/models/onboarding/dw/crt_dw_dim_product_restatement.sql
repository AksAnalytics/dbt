{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS dw.dim_product_restatement ( 
	material_key         bigint identity NOT NULL  ,
	material             varchar(30)  NOT NULL  ,
	portfolio            varchar(100)  NOT NULL  ,
	portfolio_desc       varchar(200)    ,
	bar_product          varchar(100)    ,
	bar_product_desc     varchar(200)    ,
	member_type          varchar(50)    ,
	generation           integer    ,
	level01_bar          varchar(100)    ,
	level02_bar          varchar(100)    ,
	level03_bar          varchar(100)    ,
	level04_bar          varchar(100)    ,
	level05_bar          varchar(100)    ,
	level06_bar          varchar(100)    ,
	level07_bar          varchar(100)    ,
	level08_bar          varchar(100)    ,
	level09_bar          varchar(100)    ,
	start_date           date  NOT NULL  ,
	end_date             date  NOT NULL  ,
	audit_loadts         timestamp  NOT NULL  
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}