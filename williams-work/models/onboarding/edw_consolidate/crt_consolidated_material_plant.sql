-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "schema_name": "eondryrun",
    "table_name": "conslidated_material_plant",
    "transient_table": "false",
    "table_definition": "
        (
            source_sys VARCHAR(100) NOT NULL  
            ,material_cons VARCHAR(100) NOT NULL  
            ,materialplant_cons VARCHAR(100) NOT NULL  
            ,materialtext_cons VARCHAR(100)   
            ,plntmatstat_cons VARCHAR(100)   
            ,abcind_cons VARCHAR(100)   
            ,purchgrp_cons VARCHAR(100)   
            ,mrpctrl_cons VARCHAR(100)   
            ,sourcesup_cons VARCHAR(100)   
            ,sourcesup_text_cons VARCHAR(100)   
            ,profitctr_cons VARCHAR(100)   
            ,prod_plnt_rpl_loc VARCHAR(100)   
            ,prod_plnt_rpl_loc_desc VARCHAR(100)   
            ,loaddts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_crte_user VARCHAR(100)   
            ,etl_crte_ts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_updt_user VARCHAR(100)   
            ,etl_updt_ts TIMESTAMP WITHOUT TIME ZONE   
            ,PRIMARY KEY (source_sys, material_cons, materialplant_cons)
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
