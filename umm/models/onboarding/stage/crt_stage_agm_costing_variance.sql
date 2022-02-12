{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.agm_costing_variance ( 
	sku_match            integer    ,
	fiscal_month_id      integer    ,
	posting_week_enddate date    ,
	material             varchar(50)    ,
	super_sbu            varchar(65535)    ,
	total_qty            decimal(38,8)    ,
	ppv_var              decimal(38,22)    ,
	duty_var             decimal(38,22)    ,
	frght_var            decimal(38,22)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}