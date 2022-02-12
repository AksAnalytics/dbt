{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.agm_1070_costing_base ( 
	matnr                varchar(65535)    ,
	plant                varchar(65535)    ,
	st_fromdate          date    ,
	st_todate            date    ,
	from_currtype        varchar(10)    ,
	standard_tot_matl    decimal(38,17)    ,
	standard_fgt_abs     decimal(38,17)    ,
	standard_duty        decimal(38,17)    ,
	cc_fromdate          date    ,
	cc_todate            date    ,
	current_tot_matl     decimal(38,17)    ,
	current_fgt_abs      decimal(38,17)    ,
	current_duty         decimal(38,17)    ,
	standard_pp          decimal(38,17)    ,
	current_pp           decimal(38,17)    ,
	fgt_abs_var          decimal(38,17)    ,
	duty_var             decimal(38,17)    ,
	ppv_var              decimal(38,17)    ,
	fiscal_month_id      integer    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}