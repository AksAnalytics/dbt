{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS stage.agm_1070_costing ( 
	matnr                varchar(65535)    ,
	fiscal_month_id      integer    ,
	avg_standard_tot_matl decimal(38,17)    ,
	avg_standard_fgt_abs decimal(38,17)    ,
	avg_standard_duty    decimal(38,17)    ,
	avg_current_tot_matl decimal(38,17)    ,
	avg_current_fgt_abs  decimal(38,17)    ,
	avg_current_duty     decimal(38,17)    ,
	avg_standard_pp      decimal(38,17)    ,
	avg_current_pp       decimal(38,17)    ,
	avg_fgt_abs_var      decimal(38,17)    ,
	avg_duty_var         decimal(38,17)    ,
	avg_ppv_var          decimal(38,17)    
 ) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}