-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS edw.consolidated.consolidated_orders
        (
            source_sys VARCHAR(100) NOT NULL  
            ,salesordnum_cons VARCHAR(100) NOT NULL  
            ,salesorditem_cons VARCHAR(100) NOT NULL  
            ,salesorg_cons VARCHAR(100)   
            ,salesdist_cons VARCHAR(100)   
            ,salesdiv_cons VARCHAR(100)   
            ,salesoff_cons VARCHAR(100)   
            ,doctype_cons VARCHAR(100)   
            ,changeonh_cons VARCHAR(100)   
            ,orderreason_cons VARCHAR(100)   
            ,createdonh_cons VARCHAR(100)   
            ,createdtimeh_cons VARCHAR(100)   
            ,soldto_cons VARCHAR(100)   
            ,doccat_cons VARCHAR(100)   
            ,doccurr_cons VARCHAR(100)   
            ,ord_req_dl_dte VARCHAR(100)   
            ,ord_dlv_blck_ssk VARCHAR(100)   
            ,shipto_cons VARCHAR(100)   
            ,ord_ship_cond VARCHAR(100)   
            ,ord_ship_point VARCHAR(100)   
            ,changeoni_cons VARCHAR(100)   
            ,createdoni_cons VARCHAR(100)   
            ,createdtimei_cons VARCHAR(100)   
            ,createdby_cons VARCHAR(100)   
            ,billingblock_cons VARCHAR(100)   
            ,orderqty_cons NUMERIC(38,10)   
            ,salesdeal_cons VARCHAR(100)   
            ,reject_reason_cd VARCHAR(100)   
            ,rejectqty_cons NUMERIC(38,10)   
            ,promotion_cons VARCHAR(100)   
            ,ordercost_cons NUMERIC(38,10)   
            ,ordervalue_cons NUMERIC(38,10)   
            ,material_cons VARCHAR(100)   
            ,salesunit_cons VARCHAR(100)   
            ,netprice_cons NUMERIC(38,10)   
            ,netvalue_cons NUMERIC(38,10)   
            ,profitcenter_cons VARCHAR(100)   
            ,itemcat_cons VARCHAR(100)   
            ,plant_cons VARCHAR(100)   
            ,salesrep_cons VARCHAR(100)   
            ,ord_stat_dlv VARCHAR(100)   
            ,ord_stat_crdt VARCHAR(100)   
            ,svclvldate_cons VARCHAR(100)   
            ,ord_orgtranplandt VARCHAR(100)   
            ,ord_svc_lvl_fiscy VARCHAR(100)   
            ,ord_svc_lvl_fiscp VARCHAR(100)   
            ,ord_svc_lvl_fiscw VARCHAR(100)   
            ,ord_creat_fiscy VARCHAR(100)   
            ,ord_creat_fiscp VARCHAR(100)   
            ,ord_creat_fiscw VARCHAR(100)   
            ,doctype_cons_txt VARCHAR(100)   
            ,reject_reason_cd_txt VARCHAR(100)   
            ,loaddts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_crte_user VARCHAR(100)   
            ,etl_crte_ts TIMESTAMP WITHOUT TIME ZONE   
            ,etl_updt_user VARCHAR(100)   
            ,etl_updt_ts TIMESTAMP WITHOUT TIME ZONE   
            ,PRIMARY KEY (source_sys, salesordnum_cons, salesorditem_cons)
        )
        CLUSTER BY (material_cons)
    "
}%}

{{ config(materialized = "ephermeral") }}
{% do run_query(table_metadata.table_definition) %}