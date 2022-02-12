-- Imports

WITH base_knvv AS (

    SELECT * FROM sape03.knvv_current
),

base_kna1 AS (

    SELECT * FROM sape03.kna1_current
),

txt_cust_chann AS (
	
    SELECT DISTINCT 
      * 
    FROM edw_stage.texts 
    WHERE field = 'cust_chann' 
      AND source_sys='E03'

),

final AS (
					   
	SELECT 
	  'E03' AS SOURCE_SYS,
	  KNVV.KUNNR  AS  CUSTOMER_CONS,
	  KNVV.SPART  AS  SALESDIV_CONS,
	  KNVV.VKORG  AS  SALESORG_CONS,
	  KNVV.VTWEG  AS  SALESDIST_CONS,
	  KNVV.VKGRP  AS  SALES_GROUP_CONS,
	  KNVV.VKBUR  AS  SALES_OFFICE_CONS,
	  KNA1.BRSCH  AS  INDUSTRY_KEY_CONS,
	  KNA1.KTOKD  AS  ACCGROUP_CONS,
	  KNA1.KUKLA  AS  CUSTCLASS_CONS,
	  KNA1.LAND1  AS  COUNTRY_CONS,
	  KNA1.NAME1  AS  NAME_CONS,
	  KNA1.ORT01  AS  CITY_CONS,
	  KNA1.REGIO  AS  REGION_CONS,
	  KNVV.KONDA  AS  CUST_CHAN_CODE,
	  NULL  AS  CUST_SUB_CHAN_CODE,
	  txt_cust_chann.code_txt  AS  CUST_CHAN_TXT,
	  NULL  AS  CUST_SUB_CHAN_TXT,
  	  NULL  AS  CUST_FIELD_ZONE,
	  NULL  AS  CUST_FIELD_AREA,
	  NULL  AS  CUST_FIELD_TERR,
	  KNA1.KATR1  AS  CUST_TYPE,
	  NULL AS  C11DEMAND_GROUP,
	  KNVV.LOADDTS AS LOADDTS,
	  v_job_name AS  ETL_CRTE_USER,
	  CURRENT_TIMESTAMP AS  ETL_CRTE_TS ,
	  NULL AS  ETL_UPDT_USER,
	  CAST(NULL AS timestamp) AS  ETL_UPDT_TS
	FROM sape03.knvv_current KNVV
	JOIN sape03.kna1_current KNA1
	  ON (KNVV.KUNNR = KNA1.KUNNR)
    LEFT JOIN txt_cust_chann 
      ON KNVV.KONDA = txt_cust_chann.code
	WHERE KNVV.loaddts > v_last_extract_timestamp
)

SELECT * FROM final