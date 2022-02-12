{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.warranty_cost_allocated_amt_101_transient ( 
	fiscal_month_id      integer    ,
	posting_week_enddate date    ,
	material             varchar(50)    ,
	warranty_amt         decimal(38,12)    ,
	invoice_sales        decimal(38,12)    ,
	cost_pool            varchar(50)    ,
	bar_currtype         varchar(10)    ,
	total_sales_bysbu    decimal(38,12)    ,
	avg_claim_rate       decimal(38,12)    ,
	allocation_rate      decimal(38,12)    ,
	allocated_amt        decimal(38,12)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}