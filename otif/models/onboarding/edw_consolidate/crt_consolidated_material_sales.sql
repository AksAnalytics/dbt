-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
       CREATE TABLE IF NOT EXISTS edw.consolidated.consolidated_material_sales
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
    "
}%}

{{ config(materialized = "ephermeral") }}
{% do run_query(table_metadata.table_definition) %}