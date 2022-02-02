-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "schema_name": "eondryrun",
    "table_name": "consolidated_job_history",
    "transient_table": "false",
    "table_definition": "
        (
            job_status VARCHAR(7)   
            ,end_timestamp TIMESTAMP WITHOUT TIME ZONE   
            ,etl_updt_ts TIMESTAMP WITH TIME ZONE   
            ,insert_count BIGINT   
            ,delete_count BIGINT   
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
