-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw_stage.consolidated_job_history
        (
            job_status VARCHAR(7)   
            ,end_timestamp TIMESTAMP WITHOUT TIME ZONE   
            ,etl_updt_ts TIMESTAMP WITH TIME ZONE   
            ,insert_count BIGINT   
            ,delete_count BIGINT   
        )
    "
}%}

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}