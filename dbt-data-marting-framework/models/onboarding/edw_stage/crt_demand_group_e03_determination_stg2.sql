-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "schema_name": "eondryrun",
    "table_name": "demand_group_e03_determination_stg2",
    "transient_table": "false",
    "table_definition": "
       (
            zdmdgrp_factors_trans VARCHAR(65535)   
            ,material_cons VARCHAR(100)   
            ,customer_cons VARCHAR(65535)   
            ,salesorg_cons VARCHAR(100)   
            ,kunnr_trans VARCHAR(65535)   
            ,mandt VARCHAR(65535)   
            ,seqid VARCHAR(65535)   
            ,seqmchid VARCHAR(65535)   
            ,vkorg VARCHAR(65535)   
            ,vtweg VARCHAR(65535)   
            ,brand_cons VARCHAR(65535)   
            ,country_cons VARCHAR(65535)   
            ,sales_office_cons VARCHAR(65535)   
            ,industkey_cons VARCHAR(65535)   
            ,kunnr VARCHAR(65535)   
            ,prodhl1_e03 VARCHAR(65535)   
            ,prodhl2_e03 VARCHAR(65535)   
            ,zdmdgrp VARCHAR(65535)   
            ,zdmdgrp_factors VARCHAR(65535)   
            ,rn BIGINT   
            ,hier_typ VARCHAR(65535)   
            ,hier_level INTEGER   
            ,demand_group_logic_level INTEGER   
            ,dgrp BIGINT   
            ,rownum BIGINT   
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
