{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl_stage.temp_pl_trans_fact_stats_1
(
	cal_date VARCHAR(15)   
	,erp_source_list VARCHAR(500)   
	,erp_source VARCHAR(500)   
	,bar_fiscal_period VARCHAR(500)   
	,bar_year VARCHAR(500)   
	,bar_period VARCHAR(500)   
	,cnt_records BIGINT   
	,sum_bar_amt_lc NUMERIC(38,10)   
	,sum_bar_amt_usd NUMERIC(38,10)   
)
DISTSTYLE EVEN
 SORTKEY (
	erp_source_list
	, cal_date
	)
;
ALTER TABLE global_pl_stage.temp_pl_trans_fact_stats_1 owner to base_admin; 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}