-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "schema_name": "eondryrun",
    "table_name": "conslidated_material_sales",
    "transient_table": "false",
    "table_definition": "
       (
        source_sys VARCHAR(100) NOT NULL  
        ,material_cons VARCHAR(100) NOT NULL  
        ,material_salesorg_cons VARCHAR(100) NOT NULL  
        ,material_salesdiv_cons VARCHAR(100)   
        ,material_cons_status VARCHAR(100)   
        ,loaddts TIMESTAMP WITHOUT TIME ZONE   
        ,etl_crte_user VARCHAR(100)   
        ,etl_crte_ts TIMESTAMP WITHOUT TIME ZONE   
        ,etl_updt_user VARCHAR(100)   
        ,etl_updt_ts TIMESTAMP WITHOUT TIME ZONE   
        ,PRIMARY KEY (source_sys, material_cons, material_salesorg_cons)
    )
        CLUSTER BY (material_cons)
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
