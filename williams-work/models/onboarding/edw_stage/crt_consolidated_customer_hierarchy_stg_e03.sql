-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "schema_name": "eondryrun",
    "table_name": "consolidated_customer_hierarchy_stg_e03",
    "transient_table": "false",
    "table_definition": "
        (
            source_sys VARCHAR(3)   
            ,customer VARCHAR(65535)   
            ,kunnr VARCHAR(65535)   
            ,hityp VARCHAR(65535)   
            ,name1 VARCHAR(65535)   
            ,ktokd VARCHAR(65535)   
            ,vkorg VARCHAR(65535)   
            ,hkunnr VARCHAR(65535)   
            ,vtweg VARCHAR(65535)   
            ,spart VARCHAR(65535)   
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
