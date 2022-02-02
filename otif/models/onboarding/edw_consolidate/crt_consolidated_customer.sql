-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "schema_name": "eondryrun",
    "table_name": "conslidated_customer",
    "transient_table": "false",
    "table_definition": "
        (
            source_sys VARCHAR(65535) NOT NULL  
            ,customer_cons VARCHAR(65535) NOT NULL  
            ,industkey_cons VARCHAR(65535)   
            ,accgroup_cons VARCHAR(65535)   
            ,custclass_cons VARCHAR(65535)   
            ,country_cons VARCHAR(65535)   
            ,name_cons VARCHAR(65535)   
            ,city_cons VARCHAR(65535)   
            ,region_cons VARCHAR(65535)   
            ,custclass_cons_txt VARCHAR(65535)   
            ,loaddts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_crte_user VARCHAR(65535)   
            ,etl_crte_ts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_updt_user VARCHAR(65535)   
            ,etl_updt_ts TIMESTAMP WITHOUT TIME ZONE   
            ,PRIMARY KEY (source_sys, customer_cons)
        )
        CLUSTER BY (customer_cons)
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
