-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw_stage.demand_group_e03_determination_stg1
        (
            mandt VARCHAR(65535)   
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
        )
        CLUSTER BY (kunnr)
    "
}%}

{{ config(materialized = "ephermeral") }}
{% do run_query(table_metadata.table_definition) %}