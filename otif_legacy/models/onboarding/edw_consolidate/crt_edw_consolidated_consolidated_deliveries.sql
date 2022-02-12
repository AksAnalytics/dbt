-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw_consolidated.consolidated_deliveries
        (
            source_sys VARCHAR(100) NOT NULL  
            ,delivnum_cons VARCHAR(100) NOT NULL  
            ,delivitem_cons VARCHAR(100) NOT NULL  
            ,delivtype_cons VARCHAR(100)   
            ,plant_cons VARCHAR(100)   
            ,route_cons VARCHAR(100)   
            ,createdonh_cons VARCHAR(100)   
            ,createdbyh_cons VARCHAR(100)   
            ,changedonh_cons VARCHAR(100)   
            ,changeby_cons VARCHAR(100)   
            ,pickdate_cons VARCHAR(100)   
            ,loaddate_cons VARCHAR(100)   
            ,transpdate_cons VARCHAR(100)   
            ,delivdate_cons VARCHAR(100)   
            ,delivblock_cons VARCHAR(100)   
            ,gidate_cons VARCHAR(100)   
            ,actgidate_cons VARCHAR(100)   
            ,ovdl_est_act_dlv_dte VARCHAR(100)   
            ,wmsconfdate_cons VARCHAR(100)   
            ,odlv_orig_qty NUMERIC(38,10)   
            ,odlv_otif_dte VARCHAR(100)   
            ,delivqty_cons NUMERIC(38,10)   
            ,material_cons VARCHAR(100)   
            ,baseunit_cons VARCHAR(100)   
            ,incoterms1_cons VARCHAR(100)   
            ,incoterms2_cons VARCHAR(100)   
            ,refdoc_cons VARCHAR(100)   
            ,refitem_cons VARCHAR(100)   
            ,loaddts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_crte_user VARCHAR(100)   
            ,etl_crte_ts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_updt_user VARCHAR(100)   
            ,etl_updt_ts TIMESTAMP WITHOUT TIME ZONE   
            ,PRIMARY KEY (source_sys, delivnum_cons, delivitem_cons)
        )
        CLUSTER BY (material_cons)
    "
}%}

{{ config(materialized = "ephemeral") }}
{% do run_query(table_metadata.table_definition) %}