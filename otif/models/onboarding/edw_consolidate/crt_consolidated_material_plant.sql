-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw.consolidated.consolidated_material_plant
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
    "
}%}

{{ config(materialized = "ephermeral") }}
{% do run_query(table_metadata.table_definition) %}