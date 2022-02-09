-- Imports

-- Add reference
WITH zs AS (

    SELECT * FROM temp.z_slo004
),

-- Add reference
lip AS (

    SELECT * FROM sapc11.likp_current
),

-- Add reference
lis AS (

    SELECT * FROM sapc11.lips_current
),

-- Add reference
edw_shipments AS (

    SELECT 
      order_number
    FROM otif.edw_shipments
)

final AS (

    SELECT      
    'WMS_BW' AS source_schema,
    256||trim( leading '0' FROM DELIV_NUMB)  AS order_number,
    ZI_USBOL AS bol_number,
    ZI_TRANMD AS scac,
    ZI_CONFDT||':'||ZI_CONFTM AS completion_dt,
    ZI_ESDLDT AS early_delivery_dt, 
    Z_PLANDT||':'||Z_PLANTM AS planned_dt,
    Z_LASTGDT||':'||z_lastgtm AS staged_dt,
    Z_LOADEDT||':'||Z_LOADETM AS loaded_dt,
    Z_DOCUMDT||':'||Z_DOCUMTM AS documented_dt,
    Z_LASTGDT||':'||z_lastgtm AS plt_last_staged_dt,
    Z_1STPICD||':'||Z_1STPICT AS rpk_first_picked_dt,
    Z_LASTGDT||':'||Z_LASTGTM  AS rpk_last_sealed_dt,
    ZI_CARPNT AS carrier_name,
    lip.vbeln AS sap_delivery,
    lip.lfart AS sap_delivery_type,
    lip.werks  AS sap_plant,
    lip.route  AS sap_route,
    lip.erdat  AS sap_createdondate,
    lip.erzet AS sap_createdontime,
    lip.wadat AS sap_plandate,
    lip.kodat AS sap_pickingdate,
    lip.lddat AS sap_loadingdate,
    lip.lfdat AS sap_deliverydate,
    lip.tddat AS sap_transpdate,
    lip.wadat_ist AS sap_actgoodsissue,
    lis.posnr AS line_number,		
    'ETL_USER' AS etl_crte_user,
    current_timestamp etl_crte_ts      
    FROM temp.z_slo004 zs 
    LEFT JOIN sapc11.likp_current lip
    ON zs.DELIV_NUMB = lip.vbeln
    LEFT JOIN sapc11.lips_current lis
    ON zs.DELIV_NUMB = lis.vbeln
    WHERE order_number NOT IN (edw_shipments)
)

SELECT * FROM final