{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl.clndr_dim
(
	dy_id INTEGER NOT NULL  
	,dy_dte DATE   
	,absolute_dy_nbr INTEGER   
	,absolute_wk_nbr INTEGER   
	,absolute_mth_nbr INTEGER   
	,absolute_qtr_nbr INTEGER   
	,dy_in_wk_nbr INTEGER   
	,dy_in_wk_name VARCHAR(20)   
	,julian_dy_nbr INTEGER   
	,clndr_wk_nbr INTEGER   
	,clndr_wk_id INTEGER   
	,clndr_mth_nbr INTEGER   
	,clndr_mth_id VARCHAR(20)   
	,clndr_mth_name VARCHAR(20)   
	,clndr_qtr_nbr INTEGER   
	,clndr_qtr_id VARCHAR(20)   
	,clndr_qtr_name VARCHAR(20)   
	,clndr_yr_id INTEGER   
	,clndr_dy_in_mth_nbr INTEGER   
	,is_first_dy_in_clndr_mth_flag VARCHAR(20)   
	,is_last_dy_in_clndr_mth_flag VARCHAR(20)   
	,wk_begin_dte DATE   
	,wk_end_dte INTEGER   
	,fmth_begin_dte INTEGER   
	,fmth_end_dte INTEGER   
	,fqtr_begin_dte INTEGER   
	,fqtr_end_dte INTEGER   
	,fyr_begin_dte INTEGER   
	,fyr_end_dte INTEGER   
	,fwk_nbr INTEGER   
	,fwk_id INTEGER   
	,fwk_cd VARCHAR(20)   
	,fmth_nbr INTEGER   
	,fmth_id INTEGER   
	,fmth_cd VARCHAR(20)   
	,fmth_name VARCHAR(20)   
	,fmth_short_name VARCHAR(20)   
	,fqtr_nbr INTEGER   
	,fqtr_id INTEGER   
	,fqtr_cd VARCHAR(20)   
	,fqtr_name VARCHAR(20)   
	,fyr_id INTEGER   
	,fdy_in_mth_nbr INTEGER   
	,fscl_days_remaining_in_mth INTEGER   
	,fdy_in_qtr_nbr INTEGER   
	,fscl_days_remaining_in_qtr INTEGER   
	,fdy_in_yr_nbr INTEGER   
	,fscl_days_remaining_in_yr INTEGER   
	,fwk_in_mth_nbr INTEGER   
	,fwk_in_qtr INTEGER   
	,is_wk_dy_flag VARCHAR(20)   
	,is_weekend_flag VARCHAR(20)   
	,is_first_dy_of_fwk_flag VARCHAR(20)   
	,is_last_dy_of_fwk_flag VARCHAR(20)   
	,is_first_dy_of_fmth_flag VARCHAR(20)   
	,is_last_dy_of_fmth_flag VARCHAR(20)   
	,is_first_dy_of_fqtr_flag VARCHAR(20)   
	,is_last_dy_of_fqtr_flag VARCHAR(20)   
	,is_first_dy_of_fyr_flag VARCHAR(20)   
	,is_last_dy_of_fyr_flag VARCHAR(20)   
	,season_name VARCHAR(20)   
	,holiday_name VARCHAR(20)   
	,holiday_season_name VARCHAR(20)   
	,holiday_observed_name VARCHAR(20)   
	,special_event_name VARCHAR(20)   
	,etl_batch_id INTEGER   
	,PRIMARY KEY (dy_id)
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}