-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "schema_name": "eondryrun",
    "table_name": "consolidated_customer_hierarchy_flatten_c11",
    "transient_table": "false",
    "table_definition": "
        (
            source_sys VARCHAR(65535)   
            ,customer_cons VARCHAR(65535)   
            ,salesdiv_cons VARCHAR(65535)   
            ,salesorg_cons VARCHAR(65535)   
            ,salesdist_cons VARCHAR(65535)   
            ,level_1a VARCHAR(65535)   
            ,level_2a VARCHAR(65535)   
            ,level_1c VARCHAR(65535)   
            ,level_1ap VARCHAR(65535)   
            ,level_2ap VARCHAR(65535)   
            ,level_1cp VARCHAR(65535)   
            ,etl_crte_ts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_updt_ts TIMESTAMP WITHOUT TIME ZONE   
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
