{{config(
    materialized = 'table'
    schema = 'global_pl'
)}}

WITH c11_customer AS (

    SELECT 
      KUNNR, ADRNR, BRAN1, BRAN2, BRAN3,
      BRAN4, BRAN5, BRSCH, CITYC, COUNC,
      LAND1, NAME1, ORT01, ORT02, PFACH,
      PSTL2, PSTLZ, REGIO, RPMKR, 
      'C11' AS ERP_SOURCE,
      STRAS, 
      'etl_user' AS ETL_CRTE_USER,
      to_date(GETDATE(),'yyyyMMdd') AS ETL_CRTE_TS
    FROM bods.c11_0customer_attr_current
),

e03_customer AS (

    SELECT
      KUNNR, ADRNR, BRAN1, BRAN2, BRAN3,
      BRAN4, BRAN5, BRSCH, CITYC, COUNC,
      LAND1, NAME1, ORT01, ORT02, PFACH,
      PSTL2, PSTLZ, REGIO, RPMKR, 
      'E03' AS ERP_SOURCE,
      STRAS, 
      'etl_user' AS ETL_CRTE_USER,
      to_date(GETDATE(),'yyyyMMdd') AS ETL_CRTE_TS
    FROM bods.e03_0customer_attr_current
),

extr_p10_customer AS (

    SELECT
      KUNNR, ADRNR, BRAN1, BRAN2, BRAN3,
      BRAN4, BRAN5, BRSCH, CITYC, COUNC,
      LAND1, NAME1, ORT01, ORT02, PFACH,
      PSTL2, PSTLZ, REGIO, RPMKR, 
      'P10' AS ERP_SOURCE,
      STRAS, 
      'etl_user' AS ETL_CRTE_USER,
      to_date(GETDATE(),'yyyyMMdd') AS ETL_CRTE_TS
    FROM bods.extr_p10_0customer_attr_current
),

extr_shp_customer AS (

    SELECT
      KUNNR, ADRNR, BRAN1, BRAN2, BRAN3,
      BRAN4, BRAN5, BRSCH, CITYC, COUNC,
      LAND1, NAME1, ORT01, ORT02, PFACH,
      PSTL2, PSTLZ, REGIO, RPMKR, 
      'SHP' AS ERP_SOURCE,
      STRAS, 
      'etl_user' AS ETL_CRTE_USER,
      to_date(GETDATE(),'yyyyMMdd') AS ETL_CRTE_TS
    FROM bods.extr_shp_customer_attr_current
),

union_table AS (

    SELECT * FROM c11_customer
    UNION ALL
    SELECT * FROM e03_customer
    UNION ALL
    SELECT * FROM extr_p10_customer
    UNION ALL 
    SELECT * FROM extr_shp_customer
),

final AS (

    SELECT
      KUNNR AS ERP_CUSTOMER_NUMBER,
      ADRNR AS ERP_CUSTOMER_ADDRESS_CODE,
      BRAN1 AS ERP_CUSTOMER_INDUSTRY_CODE_1,
      BRAN2 AS ERP_CUSTOMER_INDUSTRY_CODE_2,
      BRAN3 AS ERP_CUSTOMER_INDUSTRY_CODE_3,
      BRAN4 AS ERP_CUSTOMER_INDUSTRY_CODE_4,
      BRAN5 AS ERP_CUSTOMER_INDUSTRY_CODE_5,
      BRSCH AS ERP_CUSTOMER_INDUSTRY_KEY,
      CITYC AS ERP_CUSTOMER_CITY_CODE,
      COUNC AS ERP_CUSTOMER_COUNTY_CODE,
      LAND1 AS ERP_CUSTOMER_COUNTRY,
      NAME1 AS ERP_CUSTOMER_NAME,
      ORT01 AS ERP_CUSTOMER_CITY,
      ORT02 AS ERP_CUSTOMER_DISTRICT,
      PFACH AS ERP_CUSTOMER_PO_BOX,
      PSTL2 AS ERP_CUSTOMER_PO_BOX_POSTAL_CODE,
      PSTLZ AS ERP_CUSTOMER_POSTAL_CODE,
      REGIO AS ERP_CUSTOMER_REGION,
      RPMKR AS ERP_CUSTOMER_REGIONAL_MARKET,
      ERP_SOURCE,
      STRAS AS ERP_CUSTOMER_ADDRESS,
      ETL_CRTE_USER,
      ETL_CRTE_TS
    FROM union_table
)

SELECT * FROM final 


