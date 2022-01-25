-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "schema_name": "eondryrun",
    "table_name": "conslidated_material",
    "transient_table": "false",
    "table_definition": "
        (
            source_sys VARCHAR(100) NOT NULL  
            ,material_cons VARCHAR(100) NOT NULL  
            ,materialtext_cons VARCHAR(100)   
            ,materialcateg_cons VARCHAR(100)   
            ,materialgrp_cons VARCHAR(100)   
            ,materialtp_cons VARCHAR(100)   
            ,oldmaterial_cons VARCHAR(100)   
            ,sourcesupply_cons VARCHAR(100)   
            ,ean_cons VARCHAR(100)   
            ,createdon_cons VARCHAR(100)   
            ,baseunit_cons VARCHAR(100)   
            ,prodhier_cons VARCHAR(100)   
            ,prodhl1_e03 VARCHAR(100)   
            ,prodhl1text_e03 VARCHAR(100)   
            ,prodhl2_e03 VARCHAR(100)   
            ,prodhl2text_e03 VARCHAR(100)   
            ,prodhl3_e03 VARCHAR(100)   
            ,prodhl3text_e03 VARCHAR(100)   
            ,prodhl4_e03 VARCHAR(100)   
            ,prodhl4text_e03 VARCHAR(100)   
            ,prodhl5_e03 VARCHAR(100)   
            ,prodhl5text_e03 VARCHAR(100)   
            ,prodhl6_e03 VARCHAR(100)   
            ,prodhl6text_e03 VARCHAR(100)   
            ,prodhl1_c11 VARCHAR(100)   
            ,prodhl1text_c11 VARCHAR(100)   
            ,prodhl2_c11 VARCHAR(100)   
            ,prodhl2text_c11 VARCHAR(100)   
            ,prodhl3_c11 VARCHAR(100)   
            ,prodhl3text_c11 VARCHAR(100)   
            ,brand_cons VARCHAR(100)   
            ,basicmaterial_cons VARCHAR(100)   
            ,gppsbu_cons VARCHAR(100)   
            ,gppsbutext_cons VARCHAR(100)   
            ,gppdiv_cons VARCHAR(100)   
            ,gppdivtext_cons VARCHAR(100)   
            ,gppcat_cons VARCHAR(100)   
            ,gppcattext_cons VARCHAR(100)   
            ,gpppor_cons VARCHAR(100)   
            ,gppportext_cons VARCHAR(100)   
            ,materialgrp_cons_txt VARCHAR(100)   
            ,loaddts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_crte_user VARCHAR(100)   
            ,etl_crte_ts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_updt_user VARCHAR(100)   
            ,etl_updt_ts TIMESTAMP WITHOUT TIME ZONE   
            ,PRIMARY KEY (source_sys, material_cons)
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
