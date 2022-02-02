-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw_stage.job_master
        (
            job_id INTEGER NOT NULL  
            ,job_name VARCHAR(100) NOT NULL  
            ,database VARCHAR(50)   
            ,schema VARCHAR(50)   
            ,source_sys VARCHAR(50)   
            ,table_name VARCHAR(100) NOT NULL  
            ,frequency VARCHAR(100)   
            ,job_state VARCHAR(100)   
            ,last_extract_timestamp TIMESTAMP WITHOUT TIME ZONE   
            ,etl_crte_user VARCHAR(100)   
            ,etl_crte_ts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_updt_user VARCHAR(100)   
            ,etl_updt_ts TIMESTAMP WITHOUT TIME ZONE   
            ,PRIMARY KEY (job_id, job_name, table_name)
        )
    "
}%}

{{ config(materialized = "ephermeral") }}
{% do run_query(table_metadata.table_definition) %}