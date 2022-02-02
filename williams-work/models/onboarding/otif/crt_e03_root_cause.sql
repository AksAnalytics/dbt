-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "schema_name": "eondryrun",
    "table_name": "eo3_root_cause",
    "transient_table": "false",
    "table_definition": "
        (
            source_sys VARCHAR(100)   
            ,salesordnum_cons VARCHAR(100)   
            ,salesorditem_cons VARCHAR(100)   
            ,delivnum_cons VARCHAR(100)   
            ,delivitem_cons VARCHAR(100)   
            ,root_code_l2 VARCHAR(50)   
            ,root_code_l1 VARCHAR(50)   
            ,atcv_flag VARCHAR(1)   
            ,etl_crte_ts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_updt_ts TIMESTAMP WITHOUT TIME ZONE
        )
        CLUSTER BY (salesordnum_cons)
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
