-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "schema_name": "eondryrun",
    "table_name": "consolidated_customer_hierarchy_flatten_e03",
    "transient_table": "false",
    "table_definition": "
        (
            source_sys VARCHAR(65535)   
            ,customer_cons VARCHAR(65535)   
            ,salesdiv_cons VARCHAR(65535)   
            ,salesorg_cons VARCHAR(65535)   
            ,salesdist_cons VARCHAR(65535)   
            ,level_1b VARCHAR(65535)   
            ,level_2b VARCHAR(65535)   
            ,level_3b VARCHAR(65535)   
            ,level_4b VARCHAR(65535)   
            ,level_5b VARCHAR(65535)   
            ,level_6b VARCHAR(65535)   
            ,level_1d VARCHAR(65535)   
            ,level_2d VARCHAR(65535)   
            ,level_3d VARCHAR(65535)   
            ,level_4d VARCHAR(65535)   
            ,level_1bp VARCHAR(65535)   
            ,level_2bp VARCHAR(65535)   
            ,level_3bp VARCHAR(65535)   
            ,level_4bp VARCHAR(65535)   
            ,level_5bp VARCHAR(65535)   
            ,level_6bp VARCHAR(65535)   
            ,level_1dp VARCHAR(65535)   
            ,level_2dp VARCHAR(65535)   
            ,level_3dp VARCHAR(65535)   
            ,level_4dp VARCHAR(65535)   
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
