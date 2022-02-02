-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS otif.eo3_root_cause
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
    "
}%}

{{ config(materialized = "ephermeral") }}
{% do run_query(table_metadata.table_definition) %}