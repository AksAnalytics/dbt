{{config(
    materialized = 'table'
    schema = 'global_pl'
)}}

WITH a AS (

    SELECT * FROM global_pl.bar_acct_attr
),

b AS (

    SELECT * FROM global_pl.bar_entity_attr
),

hfm AS (

    SELECT * FROM bods.hfm_vw_hfm_actual_trans_current
),

e03_3fi_sl_h1_si_current AS (

    SELECT
      t.BAR_ACCT AS col1,
      t.BAR_AMT AS col2,
      t.BAR_BRAND AS col3,
      t.BAR_BU AS col4,
      t.BAR_CURRTYPE AS col5,
      t.BAR_CUSTNO AS col6,
      t.BAR_ENTITY AS col7,
      t.BAR_FUNCTION AS col8,
      t.BAR_PERIOD AS col9,
      t.BAR_PRODUCT AS col10,
      t.BAR_SCENARIO AS col11,
      t.BAR_SHIPTO AS col12,
      t.BAR_YEAR AS col13, 
      cast(t.PERIOD AS text) AS col14,
      t.ACCT AS col15,
      t.INT_BRANDGRP AS col16,
      NULL AS col17,
      t.COCODE AS col18,
      t.COSTCTR AS col19,
      t.DOCTYPE AS col20,
      t.DOCLINE AS col21,
      t.DOCNO AS col22,
      t.SGTXT AS col23,
      NULL AS col24,
      t.PRODUCT AS col25,
      t.PAYER AS col26,
      t.CPUDT AS col27,
      t.QUANTITY AS col28,
      t.QUANUNIT AS col29,
      t.REFDOCCAT AS col30,
      t.REFITM AS col31,
      t.REFDOC AS col32,
      t.PROFCTR AS col33,
      t.SALESDIV AS col34,
      t.SALESOFF AS col35,
      t.SHIPTOCUST AS col36,
      t.SOLDTOCUST AS col37,
      t.PLANT AS col38,
      t.CHARTACCTS AS col39,
      t.LOADDATETIME AS col40,
      t.ID AS col41,
      'E03' AS col42,
      b.bar_entity_currency AS col43,
      hfm.bar_amt AS col44,
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45,
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.e03_3fi_sl_h1_si_current t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency 
    WHERE t.id IS NOT NULL
),

shp_tb_litm_current AS (

    SELECT 
      t.BAR_ACCT AS col1,
      t.BAR_AMT AS col2,
      t.BAR_BRAND AS col3,
      t.BAR_BU AS col4,
      t.BAR_CURRTYPE AS col5,
      t.BAR_CUSTNO AS col6,
      t.BAR_ENTITY AS col7,
      t.BAR_FUNCTION AS col8,
      t.BAR_PERIOD AS col9,
      t.BAR_PRODUCT AS col10,
      t.BAR_SCENARIO AS col11,
      t.BAR_SHIPTO AS col12,
      t.BAR_YEAR AS col13,
      t.FISCPER AS col14,
      t.GLACCOUNT AS col15,
      t.MATERIALBRAND AS col16,
      NULL AS col17,
      t.COMPANYCODE AS col18,
      t.COSTCENTER AS col19,
      t.DOCTYPE AS col20,
      cast(t.LINEITMNUM AS text) AS col21,
      t.DOCNUM  AS col22,
      NULL  AS col23,
      NULL  AS col24,
      t.MATERIALNUM AS col25,
      t.PAYER AS col26,
      t.POSTINGDATE AS col27,
      NULL AS col28,
      NULL AS col29,
      NULL AS col30,
      NULL AS col31,
      t.REFDOC AS col32,
      t.PROFITCENTER AS col33,
      t.SALESORG AS col34,
      NULL AS col35,
      t.SHIPTOPARTY AS col36,
      t.SOLDTOPARTY AS col37,
      t.PLANT AS col38,
      NULL AS col39,
      t.LOADDATETIME AS col40,
      t.ID AS col41,
      'SHP' AS col42,
      b.bar_entity_currency AS col43,
      hfm.bar_amt AS col44,
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45,
      to_date(GETDATE(),'yyyyMMdd') AS col46,
      'etl_user' AS col47,
    FROM bods.shp_tb_litm_current t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity 
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency

),

p10_0ec_pca_3_trans_current AS (

    SELECT 
      t.BAR_ACCT AS col1, 
      t.BAR_AMT AS col2, 
      t.BAR_BRAND AS col3, 
      t.BAR_BU AS col4, 
      t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, 
      t.BAR_ENTITY AS col7, 
      t.BAR_FUNCTION AS col8,
      t.BAR_PERIOD AS col9,
      t.BAR_PRODUCT AS col10,
      t.BAR_SCENARIO AS col11,
      t.BAR_SHIPTO AS col12,
      t.BAR_YEAR AS col13,
      t.FISCPER::text AS col14,
      t.ACCT AS col15,
      t.BRAND_CD AS col16,
      t.BUS_AREA AS col17,
      t.CO_CD AS col18,
      t.COST_CNTR AS col19,
      t.DOCCT AS col20,
      t.DOCLN AS col21,
      t.DOCNR AS col22,
      t.SGTXT AS col23,
      t.LIFNR AS col24,
      t.PROD_CD AS col25,
      t.HIGHER_LVL_CUST AS col26,
      t.CPUDT AS col27,
      t.QUANTITY AS col28,
      t.QUANUNIT AS col29,
      t.REFDOCCT AS col30,
      t.REFDOCLN AS col31,
      t.REFDOCNR AS col32,
      t.PROFIT_CNTR AS col33,
      t.CUST_ACT_GRP AS col34,
      NULL AS col35,
      t.SHIPTO_CUST_NBR AS col36,
      t.CUST_NO AS col37,
      t.WERKS AS col38,
      t.CHARTACCTS AS col39,
      t.LOADDATETIME AS col40,
      t.ID AS col41,
      'P10' AS col42,
      b.bar_entity_currency AS col43,
      hfm.bar_amt AS col44,
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45,
      to_date(GETDATE(),'yyyyMMdd') AS col46,
      'etl_user' AS col47
    FROM bods.p10_0ec_pca_3_trans_current t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity 
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency

),

lawson_mac_pl_trans_current AS (

    SELECT 
      t.BAR_ACCT AS col1,
      t.BAR_AMT AS col2,
      t.BAR_BRAND AS col3,
      t.BAR_BU AS col4,
      t.BAR_CURRTYPE AS col5,
      t.BAR_CUSTNO AS col6,
      t.BAR_ENTITY AS col7,
      t.BAR_FUNCTION AS col8,
      t.BAR_PERIOD AS col9,
      t.BAR_PRODUCT AS col10,
      t.BAR_SCENARIO AS col11,
      t.BAR_SHIPTO AS col12,
      t.BAR_YEAR AS col13,
      t.FISCPER AS col14,
      t.ACCT AS col15,
      t.BRAND_CD AS col16,
      t.ACCT_UNIT AS col17,
      t.CO_CD AS col18,
      t.SUB_ACCT AS col19,
      t.SYS_CD AS col20,
      t.POST_DOC_REF_LN_NBR AS col21,
      t.POST_DOC_REF_NBR AS col22,
      t.SYS_NAME AS col23,
      NULL AS col24,
      t.PROD_CD AS col25,
      NULL AS col26,
      t.POST_DTE AS col27,
      t.QUANTITY::numeric(38,10) AS col28,
      NULL AS col29,
      t.SRC_DOC_TYP AS col30,
      NULL AS col31,
      t.SRC_DOC_NBR AS col32,
      NULL AS col33,
      NULL AS col34,
      NULL AS col35,
      NULL AS col36,
      CUST_NBR AS col37,
      NULL AS col38,
      NULL AS col39,
      t.LOADDATETIME AS col40,
      t.ID::bigint AS col41, 
      'LAWSON' AS col42,
      b.bar_entity_currency AS col43,
      hfm.bar_amt AS col44,
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45,
      to_date(GETDATE(),'yyyyMMdd') AS col46,
      'etl_user' AS col47
    FROM bods.lawson_mac_pl_trans_current t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity 
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency

),

union_table AS (

    SELECT * FROM e03_3fi_sl_h1_si_current
    UNION ALL  
    SELECT * FROM shp_tb_litm_current
    UNION ALL
    SELECT * FROM p10_0ec_pca_3_trans_current
    UNION ALL
    SELECT * FROM lawson_mac_pl_trans_current
    
),


final AS (

    SELECT
      col1 AS bar_account,
      col2 AS bar_amt_lc,
      col3 AS bar_brand,
      col4 AS bar_bu,
      col5 AS bar_currtype,
      col6 AS bar_customer,
      col7 AS bar_entity,
      col8 AS bar_function,
      col9 AS bar_period,
      col10 AS bar_product,
      col11 AS bar_scenario,
      col12 AS bar_shipto,
      col13 AS bar_year,
      col14 AS bar_fiscal_period,
      col15 AS erp_account,
      col16 AS erp_brand_code,
      col17 AS erp_business_area,
      col18 AS erp_company_code,
      col19 AS erp_cost_center,
      col20 AS erp_doc_type,
      col21 AS erp_doc_line_num,
      col22 AS erp_doc_num,
      col23 AS erp_document_text,
      col24 AS erp_vendor,
      col25 AS erp_material,
      col26 AS erp_customer_parent,
      col27 AS erp_posting_date,
      col28 AS erp_quantity,
      col29 AS erp_quantity_uom,
      col30 AS erp_ref_doc_type,
      col31 AS erp_ref_doc_line_num,
      col32 AS erp_ref_doc_num,
      col33 AS erp_profit_center,
      col34 AS erp_sales_group,
      col35 AS erp_sales_office,
      col36 AS erp_customer_ship_to,
      col37 AS erp_customer_sold_to,
      col38 AS erp_plant,
      col39 AS erp_chartaccts,
      col40 AS bar_bods_loaddatetime,
      col41 AS bar_bods_record_id,
      col42 AS erp_source,
      col43 AS bar_s_entity_currency,
      col44 AS bar_s_curr_rate_actual,
      col45 AS bar_amt_usd,
      col46 AS etl_crte_ts,
      col47 AS etl_crte_user
    FROM union_table

)

SELECT * FROM final