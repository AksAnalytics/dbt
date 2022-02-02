-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "schema_name": "eondryrun",
    "table_name": "job_history",
    "transient_table": "false",
    "table_definition": "
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
    ",
    "full_refresh_ddl_statements": [

    ]
}%}

{%- set create_table_hook = create_data_mart_table(table_metadata) -%}

{{ config(
    materialized = "table",
    pre_hook = create_table_hook,
    schema = table_metadata.schema_name
)}}


WITH source AS (
    SELECT * FROM {{ table_metadata.table_name }}
),

renamed AS (
    SELECT 
      *
    FROM source
)

SELECT * FROM renamed
