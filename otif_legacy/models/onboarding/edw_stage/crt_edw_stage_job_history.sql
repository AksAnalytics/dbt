-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw_stage.job_history
        (
            run_id BIGINT NOT NULL  
            ,job_id INTEGER NOT NULL  
            ,job_name VARCHAR(100) NOT NULL  
            ,table_name VARCHAR(100) NOT NULL  
            ,run_date TIMESTAMP WITHOUT TIME ZONE   
            ,start_timestamp TIMESTAMP WITHOUT TIME ZONE   
            ,end_timestamp TIMESTAMP WITHOUT TIME ZONE   
            ,run_seq INTEGER NOT NULL  
            ,job_status VARCHAR(100)   
            ,etl_crte_user VARCHAR(100)   
            ,etl_crte_ts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_updt_user VARCHAR(100)   
            ,etl_updt_ts TIMESTAMP WITHOUT TIME ZONE   
            ,insert_count BIGINT   
            ,update_count BIGINT   
            ,delete_count BIGINT   
            ,PRIMARY KEY (run_id, job_id, job_name, table_name, run_seq)
        )
    "
}%}

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}