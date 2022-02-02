-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "schema_name": "eondryrun",
    "table_name": "job_master",
    "transient_table": "false",
    "table_definition": "
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
