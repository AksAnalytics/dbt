-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "schema_name": "eondryrun",
    "table_name": "consolidated_customer_hierarchy",
    "transient_table": "false",
    "table_definition": "
        (
            source_sys VARCHAR(3)   
            ,customer_cons VARCHAR(65535)   
            ,parent_cons VARCHAR(65535)   
            ,hier_typ VARCHAR(65535)   
            ,parent_name VARCHAR(65535)   
            ,acct_grp VARCHAR(65535)   
            ,salesorg_cons VARCHAR(65535)   
            ,hier_level INTEGER   
            ,demand_group_logic_level INTEGER   
            ,salesdist_cons VARCHAR(65535)   
            ,salesdiv_cons VARCHAR(65535)   
            ,etl_crte_user VARCHAR(8)   
            ,etl_crte_ts TIMESTAMP WITH TIME ZONE   
            ,etl_updt_user VARCHAR(1)   
            ,etl_updt_ts VARCHAR(1)   
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
