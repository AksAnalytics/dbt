{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl.status_master_1
(
	id INTEGER NOT NULL DEFAULT "identity"(911343, 0, '1,1'::text) 
	,tbl_type VARCHAR(100)   
	,job_nm VARCHAR(100) NOT NULL  
	,tgt_tbl_nm VARCHAR(100)   
	,src_tbl_nm VARCHAR(100)   
	,src_col_lst VARCHAR(65535)   
	,tgt_col_lst VARCHAR(65535)   
	,erp_source VARCHAR(100)   
	,frequency VARCHAR(100)   
	,job_state VARCHAR(100)   
	,manual_run VARCHAR(100)   
	,crte_user VARCHAR(100)   
	,crte_ts VARCHAR(100)   
	,PRIMARY KEY (id, job_nm)
) 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}