{% set table_metadata = { 
	 "table_definition":" 
		CREATE TABLE IF NOT EXISTS global_pl.job_master
(
	job_id INTEGER NOT NULL  
	,job_name VARCHAR(100)   
	,table_name VARCHAR(100)   
	,frequency VARCHAR(100)   
	,job_state VARCHAR(100)   
	,etl_crte_user VARCHAR(100)   
	,etl_crte_ts DATE   
	,etl_updt_user VARCHAR(100)   
	,etl_updt_ts DATE   
	,PRIMARY KEY (job_id)
)




CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_664873" AFTER
DELETE
    ON
    global_pl.job_master
FROM
    global_pl.job_history NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_noaction_del"('job_history_job_id_fkey',
    'job_history',
    'job_master',
    'UNSPECIFIED',
    'job_id',
    'job_id');
CREATE CONSTRAINT TRIGGER "RI_ConstraintTrigger_664874" AFTER
UPDATE
    ON
    global_pl.job_master
FROM
    global_pl.job_history NOT DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_noaction_upd"('job_history_job_id_fkey',
    'job_history',
    'job_master',
    'UNSPECIFIED',
    'job_id',
    'job_id'); 
	"
}%} 

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}