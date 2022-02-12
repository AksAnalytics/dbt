{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS dw.dim_product ( 
	product_key          bigint identity NOT NULL  ,
	product_id           varchar(200)    ,
	material             varchar(100)    ,
	bar_product          varchar(100)    ,
	sku                  varchar(200)    ,
	product_brand        varchar(100)    ,
	portfolio            varchar(100)    ,
	portfolio_desc       varchar(200)    ,
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
	audit_loadts         timestamp  NOT NULL  ,
	gpp_division_code    varchar(10)    ,
	prd_comm_level01_gts varchar(100)    ,
	prd_comm_level02_super_bu varchar(100)    ,
	prd_comm_level03_subcategory varchar(100)    ,
	prd_comm_level04_category varchar(100)    ,
	prd_comm_level05_gpp_portfolio varchar(100)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}