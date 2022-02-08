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

byd_pl_trans_archive_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.GL_ACCOUNT AS col15, 
      t.INT_BRANDGRP AS col16, t.SEGMENT AS col17, t.COMPANY_ID AS col18, t.COST_CENTER AS col19, NULL AS col20, 
      t.JOURNAL_ENTRY_ITEM AS col21, t.JOURNAL_ENTRY AS col22, NULL AS col23, NULL AS col24, t.PRODUCT_ID AS col25, 
      t.PAYER AS col26, t.POSTING_DATE AS col27, t.INVOICED_QUANTITY AS col28, NULL AS col29, t.DOCUMENT_TYPE AS col30, 
      NULL AS col31, t.SOURCE_DOCUMENT_ID AS col32, t.PROFIT_CENTER AS col33, t.CUSTOMER_CHANNEL_CODE AS col34, NULL AS col35, 
      t.SHIP_TO_CUSTOMER AS col36, t.BILL_TO_CUSTOMER AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'BYD'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47   
    FROM bods.byd_pl_trans_archive_current t 
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

hfm_vw_hfm_actual_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, NULL AS col14, t.ACCT AS col15, 
      NULL AS col16, t.CUSTOM1 AS col17, t.ENTITY AS col18, t.CUSTOM2 AS col19, NULL AS col20, 
      NULL AS col21, NULL AS col22, NULL AS col23, NULL AS col24, NULL AS col25, 
      NULL AS col26, NULL AS col27, NULL AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, NULL AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'HFM'AS col42, b.bar_entity_currency AS col43, t.bar_amt AS col44, t.bar_amt AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47 
    FROM bods.hfm_vw_hfm_actual_trans_current t  
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
),

nav_storage_pl_trans_current AS ( 
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCT AS col15, 
      NULL AS col16, t.DEPARTMENT AS col17, t.COCODE AS col18, NULL AS col19, t.DOCUMENT_TYPE AS col20, 
      NULL AS col21, t.DOCUMENT_NO AS col22, NULL AS col23, NULL AS col24, NULL AS col25, 
      NULL AS col26, NULL AS col27, NULL AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, t.TRANSACTION_NO AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, NULL AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'NAVSTORAGE'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47 
    FROM bods.nav_storage_pl_trans_current t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),
   
nav_eur_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCT AS col15, 
      NULL AS col16, NULL AS col17, t.CO_CD AS col18, NULL AS col19, NULL AS col20, 
      NULL AS col21, t.TXN_ID AS col22, NULL AS col23, NULL AS col24, NULL AS col25, 
      NULL AS col26, NULL AS col27, NULL AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, t.BAR_CUSTNO AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'NAVEUR'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.nav_eur_pl_trans_current  t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),

ufida_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.GLACCOUNT_CODE AS col15, 
      NULL AS col16, t.DEPARTMENT_CODE AS col17, t.CDCCODE AS col18, NULL AS col19, NULL AS col20, 
      NULL AS col21, t.VOUCHNO AS col22, NULL AS col23, NULL AS col24, t.PRODUCT_CODE AS col25, 
      NULL AS col26, t.POSTING_DATE AS col27, t.PRODUCT_QTY AS col28, NULL AS col29, 
      NULL AS col30, NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, t.CUSTOMER_CODE AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'UFIDA'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.ufida_pl_trans_current  t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),

orch_bgi_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCT AS col15, 
      NULL AS col16, t.ICP_CD AS col17, t.CO_CD AS col18, NULL AS col19, 
      NULL AS col20, NULL AS col21, t.TXN_ID AS col22, NULL AS col23, NULL AS col24, NULL AS col25, 
      NULL AS col26, t.POST_DTE AS col27, NULL AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, NULL AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'ORCHBGI'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47 
    FROM bods.orch_bgi_pl_trans_current t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),

cont_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCT AS col15, 
      NULL AS col16, NULL AS col17, t.CO_CD AS col18, NULL AS col19, NULL AS col20, 
      NULL AS col21, t.TXN_ID AS col22, NULL AS col23, NULL AS col24, NULL AS col25, 
      NULL AS col26, t.POST_DTE AS col27, NULL AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, NULL AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'CONT'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47  
    FROM bods.cont_pl_trans_current t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),

movex_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCT AS col15, 
      NULL AS col16, t.DEPT AS col17, t.CO_CD AS col18, NULL AS col19, NULL AS col20, 
      NULL AS col21, t.TXN_ID AS col22, NULL AS col23, NULL AS col24, NULL AS col25, 
      NULL AS col26, t.TXN_DTE AS col27, NULL AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, t.CUST_NBR AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'MOVEX'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.movex_pl_trans_current t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),		

ifs_pl_trans_current AS (  
    SELECT 
      t.ACCOUNT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCOUNT AS col15, 
      NULL AS col16, t.CODE_B AS col17, t.COMPANY AS col18, t.ACCOUNT_GROUP AS col19, t.VOUCHER_TYPE AS col20, 
      NULL AS col21, t.VOUCHER_NO AS col22, 
      REGEXP_REPLACE(TEXT, '[^a-zA-Z0-9\u00E0-\u00FC ]+','') AS col23, 
      NULL AS col24, NULL AS col25, 
      NULL AS col26, NULL AS col27, NULL AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, NULL AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'IFS'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.ifs_pl_trans_current t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),		

nelson_asmp_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, 
      t.GL_ACCOUNT AS col15, NULL AS col16, NULL AS col17, t.COMPANY AS col18, NULL AS col19, t.JOURNAL_CODE AS col20, 
      t.JOURNAL_LINE AS col21, t.JOURNAL_NUM AS col22, 
      REGEXP_REPLACE(DESCRIPTION,'[^a-zA-Z0-9\u00E0-\u00FC ]+','') AS col23, 
      NULL AS col24, NULL AS col25, 
      NULL AS col26, t.POSTED_DATE AS col27, NULL AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, NULL AS col33, t.GROUPID AS col34, NULL AS col35, 
      NULL AS col36, NULL AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'NELSON'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.nelson_asmp_pl_trans_current  t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),		

agresso_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, NULL AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCOUNT AS col15, 
      NULL AS col16, t.BUSINESS_AREA AS col17, NULL AS col18, t.COST_CENTER AS col19, NULL AS col20, 
      NULL AS col21, NULL AS col22, NULL AS col23, NULL AS col24, t.PRODUCT_CODE AS col25, 
      NULL AS col26, NULL AS col27, NULL AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, t.PROFIT_CENTER AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, t.CUST_NUM AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'AGRESSO'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.agresso_pl_trans_current t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),		

nav_assm_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCOUNT AS col15, 
      NULL AS col16, t.DEPARTMENT_EXP AS col17, NULL AS col18, t.EXPENSES_TYPE AS col19, t.DOCUMENT_TYPE AS col20, 
      NULL AS col21, t.DOCUMENT_ID AS col22, NULL AS col23, NULL AS col24, t.PRODUCT AS col25, 
      t.BILL_TO_CUSTOMER AS col26, t.TRANSACTION_DATE AS col27, t.QTY AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      t.SHIP_TO_CUSTOMER AS col36, t.SOLD_TO_CUSTOMER AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'NAV'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user'  AS col47
    FROM bods.nav_assm_pl_trans_current  t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),		

qad_brazil_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCT AS col15, 
      NULL AS col16, NULL AS col17, t.QAD_ENTITY AS col18, t.COSTCTR AS col19, t.DOCUMENT_TYPE AS col20, 
      NULL AS col21, t.DOCUMENT_ID AS col22, NULL AS col23, NULL AS col24, NULL AS col25, 
      t.BILL_TO_CUSTOMER AS col26, t.TRANSACTION_DATE AS col27, t.QUANTITY AS col28, NULL AS col29, 
      NULL AS col30, NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, t.SOLD_TO_CUSTOMER AS col37, t.SITE AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'BRAZIL'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.qad_brazil_pl_trans_current  t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),		

qad_dech_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, NULL AS col15, 
      NULL AS col16, NULL AS col17, NULL AS col18, NULL AS col19, t.DOCUMENT_TYPE AS col20, 
      NULL AS col21, t.DOCUMENT_ID AS col22, NULL AS col23, NULL AS col24, t.PRODUCT AS col25, 
      t.BILL_TO_CUSTOMER AS col26, t.POSTING_DATE AS col27, t.QUANTITY AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      t.SHIP_TO AS col36, t.SOLD_TO_CUSTOMER AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'DECH'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.qad_dech_pl_trans_current  t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),		

qad_chile_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCT AS col15, 
      NULL AS col16, NULL AS col17, t.QAD_ENTITY AS col18, t.COSTCTR AS col19, t.DOCUMENT_TYPE AS col20, 
      NULL AS col21, t.DOCUMENT_ID AS col22, NULL AS col23, NULL AS col24, NULL AS col25, 
      t.BILL_TO_CUSTOMER AS col26, t.TRANSACTION_DATE AS col27, t.QUANTITY AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, t.SOLD_TO_CUSTOMER AS col37, t.SITE AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'QAD'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.qad_chile_pl_trans_current  t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),		

qad_argentina_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCT AS col15, 
      NULL AS col16, NULL AS col17, t.QAD_ENTITY AS col18, t.COSTCTR AS col19, t.DOCUMENT_TYPE AS col20, 
      NULL AS col21, t.DOCUMENT_ID AS col22, NULL AS col23, NULL AS col24, NULL AS col25, 
      t.BILL_TO_CUSTOMER AS col26, t.TRANSACTION_DATE AS col27, t.QUANTITY AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, t.SOLD_TO_CUSTOMER AS col37, t.SITE AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'QAD'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47 
    FROM bods.qad_argentina_pl_trans_current  t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),		

qad_peru_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCT AS col15, 
      NULL AS col16, NULL AS col17, t.QAD_ENTITY AS col18, t.COSTCTR AS col19, t.DOCUMENT_TYPE AS col20, 
      NULL AS col21, t.DOCUMENT_ID AS col22, NULL AS col23, NULL AS col24, NULL AS col25, 
      t.BILL_TO_CUSTOMER AS col26, t.TRANSACTION_DATE AS col27, t.QUANTITY AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, t.SOLD_TO_CUSTOMER AS col37, t.SITE AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'QAD'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.qad_peru_pl_trans_current  t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),		

p02_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, NULL AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCT AS col15, 
      NULL AS col16, t.BUS_AREA AS col17, t.CO_CD AS col18, t.COST_CNTR AS col19, t.DOCCT AS col20, 
      t.DOCLN AS col21, t.DOCNR AS col22, 
      REGEXP_REPLACE(SGTXT,'[^a-zA-Z0-9\u00E0-\u00FC ]+','') AS col23, 
      t.VENDOR_ID AS col24, t.MATERIAL AS col25, 
      NULL AS col26, t.CPUDT AS col27, t.QUANTITY AS col28, t.QUANUNIT AS col29, t.REFDOCCT AS col30, 
      t.REFDOCLN AS col31, t.REFDOCNR AS col32, t.PROFIT_CNTR AS col33, NULL AS col34, NULL AS col35, 
      t.SHIPTO_CUST_NBR AS col36, NULL AS col37, t.PLANT AS col38, t.CHARTACCTS AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'P02'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.p02_pl_trans_current  t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),		

baan_besco_tw_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCT AS col15, 
      NULL AS col16, NULL AS col17, t.CO_CD AS col18, t.COST_CNTR AS col19, NULL AS col20, 
      t.DOC_LN_NBR AS col21, t.DOC_NBR AS col22, NULL AS col23, NULL AS col24, NULL AS col25, 
      NULL AS col26, t.POST_DTE AS col27, NULL AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, NULL AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'BAANBESCOTW'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.baan_besco_tw_pl_trans_current  t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),		

navision_actuals_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRANDAS col3, t.BAR_BUAS col4, t.BAR_CURRTYPEAS col5, 
      t.BAR_CUSTNOAS col6, t.BAR_ENTITYAS col7, t.BAR_FUNCTIONAS col8, t.BAR_PERIODAS col9, t.BAR_PRODUCTAS col10, 
      t.BAR_SCENARIOAS col11, t.BAR_SHIPTOAS col12, t.BAR_YEARAS col13, NULLAS col14, t.ACCTAS col15, 
      NULL AS col16, t.FUNCAS col17, t.ENTITYAS col18, NULL AS col19, NULL AS col20, 
      NULL AS col21, NULL AS col22, NULL AS col23, NULL AS col24, NULL AS col25, 
      NULL AS col26, NULL AS col27, NULL AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, NULL AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIMEAS col40, 
      t.ID AS col41, 'NAVISION'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.navision_actuals_trans_current  t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),		

jde_na_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCOUNT AS col15, 
      t.BRAND AS col16, NULL AS col17, t.ENTITY AS col18, NULL AS col19, t.DOCUMENT_TYPE AS col20, 
      NULL AS col21, t.DOCUMENT_ID AS col22, NULL AS col23, NULL AS col24, t.PRODUCT AS col25, 
      t.CUSTOMER AS col26, t.POSTING_DATE AS col27, t.QUANTITY AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, t.COMPANY AS col33, NULL AS col34, NULL AS col35, 
      t.END_CUSTOMER AS col36, t.SOLD_TO_CUSTOMER AS col37, t.PLANT AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'JDE'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.jde_na_pl_trans_current  t 
    LEFT OUTER JOIN a 
      ON t.BAR_ACCT = a.bar_account 
    LEFT OUTER JOIN b 
      ON t.BAR_ENTITY = b.bar_entity
    LEFT OUTER JOIN hfm 
      ON t.bar_year = hfm.bar_year 
     AND t.bar_period = hfm.bar_period 
     AND hfm.bar_function = b.bar_entity_currency
),		

orch_ppe_pl_trans_current AS (  
    SELECT 
      t.BAR_ACCT AS col1, t.BAR_AMT AS col2, t.BAR_BRAND AS col3, t.BAR_BU AS col4, t.BAR_CURRTYPE AS col5, 
      t.BAR_CUSTNO AS col6, t.BAR_ENTITY AS col7, t.BAR_FUNCTION AS col8, t.BAR_PERIOD AS col9, t.BAR_PRODUCT AS col10, 
      t.BAR_SCENARIO AS col11, t.BAR_SHIPTO AS col12, t.BAR_YEAR AS col13, t.FISCPER AS col14, t.ACCT AS col15, 
      NULL AS col16, t.ICP_CD AS col17, t.CO_CD AS col18, NULL AS col19, NULL AS col20, 
      NULL AS col21, t.TXN_ID AS col22, NULL AS col23, NULL AS col24, NULL AS col25, 
      NULL AS col26, t.POST_DTE AS col27, NULL AS col28, NULL AS col29, NULL AS col30, 
      NULL AS col31, NULL AS col32, NULL AS col33, NULL AS col34, NULL AS col35, 
      NULL AS col36, NULL AS col37, NULL AS col38, NULL AS col39, t.LOADDATETIME AS col40, 
      t.ID AS col41, 'ORCHPPE'AS col42, b.bar_entity_currency AS col43, hfm.bar_amt AS col44, 
      (t.BAR_AMT * hfm.bar_amt * COALESCE (a.flipsign::INTEGER,1)) AS col45, 
      to_date(GETDATE(),'yyyyMMdd') AS col46, 
      'etl_user' AS col47
    FROM bods.orch_ppe_pl_trans_current  t 
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

    SELECT * FROM byd_pl_trans_archive_current
    UNION ALL
    SELECT * FROM hfm_vw_hfm_actual_trans_current
    UNION ALL
    SELECT * FROM nav_storage_pl_trans_current
    UNION ALL
    SELECT * FROM nav_eur_pl_trans_current
    UNION ALL
    SELECT * FROM ufida_pl_trans_current
    UNION ALL
    SELECT * FROM orch_bgi_pl_trans_current
    UNION ALL
    SELECT * FROM cont_pl_trans_current
    UNION ALL
    SELECT * FROM movex_pl_trans_current
    UNION ALL
    SELECT * FROM ifs_pl_trans_current
    UNION ALL
    SELECT * FROM nelson_asmp_pl_trans_current
    UNION ALL
    SELECT * FROM agresso_pl_trans_current
    UNION ALL
    SELECT * FROM nav_assm_pl_trans_current
    UNION ALL
    SELECT * FROM qad_brazil_pl_trans_current
    UNION ALL
    SELECT * FROM qad_dech_pl_trans_current
    UNION ALL
    SELECT * FROM qad_chile_pl_trans_current
    UNION ALL
    SELECT * FROM qad_argentina_pl_trans_current
    UNION ALL
    SELECT * FROM qad_peru_pl_trans_current
    UNION ALL
    SELECT * FROM p02_pl_trans_current
    UNION ALL
    SELECT * FROM baan_besco_tw_pl_trans_current
    UNION ALL
    SELECT * FROM navision_actuals_trans_current
    UNION ALL
    SELECT * FROM jde_na_pl_trans_current
    UNION ALL
    SELECT * FROM orch_ppe_pl_trans_current
),

final AS (
    SELECT 
    
    
    FROM union_table

)

SELECT * FROM final

