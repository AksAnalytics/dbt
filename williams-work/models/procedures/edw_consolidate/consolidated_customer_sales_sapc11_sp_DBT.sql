-- Imports

WITH base_knvv AS (

    SELECT * FROM sapc11.knvv_current
),

base_kna1 AS (

    SELECT * FROM sapc11.kna
),

base_ZCHCLKP_C AS (

    SELECT * FROM sapc11.zchclkp_current
),

base_transcust AS (

    SELECT * FROM jda.udtdfutranscust_current
),

final AS (
    
    SELECT 
	  'C11' AS SOURCE_SYS,
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
	  ZCHCLKP_C.zchcid AS CUST_CHAN_CODE,
	  ZCHCLKP_S.zchcid AS CUST_SUB_CHAN_CODE,
	  ZCHCLKP_C.TXTMD AS CUST_CHAN_TXT,
	  ZCHCLKP_S.TXTMD AS CUST_SUB_CHAN_TXT,
	  NULL  AS  CUST_FIELD_ZONE,
	  NULL  AS  CUST_FIELD_AREA,
	  NULL  AS  CUST_FIELD_TERR,
	  KNA1.KATR1  AS  CUST_TYPE,
	  U_CUST_DMDGROUP AS  C11DEMAND_GROUP,
	  KNVV.LOADDTS AS LOADDTS,
	  v_job_name AS  ETL_CRTE_USER,
	  current_timestamp AS  ETL_CRTE_TS,
	  NULL AS  ETL_UPDT_USER,
	  CAST(NULL AS timestamp) AS ETL_UPDT_TS
	FROM base_knvv KNVV
	JOIN base_kna1 KNA1
	  ON (KNVV.KUNNR = KNA1.KUNNR)
	LEFT JOIN base_ZCHCLKP_C AS ZCHCLKP_C
	  ON (KNVV.VKGRP = ZCHCLKP_C.ZSLGPFRM
      AND KNVV.VKBUR = ZCHCLKP_C.ZSLOFRM
      AND ZCHCLKP_C.ZCHCIND = 'C')
	LEFT JOIN base_ZCHCLKP_C AS ZCHCLKP_S
	  ON (KNVV.VKGRP = ZCHCLKP_S.ZSLGPFRM
     AND KNVV.VKBUR = ZCHCLKP_S.ZSLOFRM
     AND ZCHCLKP_S.ZCHCIND = 'S')
	LEFT JOIN base_transcust AS TRANSCUST
	  ON (TRANSCUST.U_CUST_CODE = KNVV.KUNNR 
	AND TRANSCUST.U_CUST_SALESORG = KNVV.VKORG)

),

SELECT * FROM final
	