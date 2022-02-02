-- Import

WITH base_kna1 AS (

    SELECT * FROM sape03.kna1_current
),

base_texts AS (

    SELECT * FROM edw_stage.texts
),

txt_cust_clASs AS (
    
    SELECT DISTINCT 
      * 
    FROM base_texts
    WHERE field = 'cust_class' 
      AND source_sys='E03'
),

final AS (

	SELECT 
	  'E03' AS  SOURCE_SYS,
	  KNA1.KUNNR AS CUSTOMER_CONS,
	  KNA1.BRSCH AS INDUSTKEY_CONS,
	  KNA1.KTOKD AS ACCGROUP_CONS,
	  KNA1.KUKLA AS CUSTCLASS_CONS,
	  KNA1.LAND1 AS COUNTRY_CONS,
	  KNA1.NAME1 AS NAME_CONS,
	  KNA1.ORT01 AS CITY_CONS,
	  KNA1.REGIO AS REGION_CONS,
	  txt_cust_class.CODE_TXT AS CUSTCLASS_CONS_TXT,
	  KNA1.LOADDTS AS LOADDTS,
	  v_job_name AS  ETL_CRTE_USER,
	  CURRENT_TIMESTAMP AS  ETL_CRTE_TS ,	
	  NULL AS  ETL_UPDT_USER,	
	  CAST(NULL AS timestamp) AS  ETL_UPDT_TS	
	FROM base_kna1 KNA1 
	LEFT JOIN txt_cust_class 
      ON txt_cust_class.code = KNA1.KUKLA
    WHERE KNA1.loaddts > v_lASt_extract_timestamp
)

SELECT * FROM final