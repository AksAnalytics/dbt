WITH base_mara AS (

    SELECT * FROM edw.sapc11.mara_current
),

base_makt AS (

    SELECT * FROM edw.sapc11.makt_current
),

base_zgpp_sbut AS (

    SELECT * FROM edw.sapc11.zgppsbut_current
),

base_zgpp_divt AS (

    SELECT * FROM edw.sapc11.zgppdivt_current
),

base_zgpp_catt AS (

    SELECT * FROM edw.sapc11.zgppcatt_current
),

base_zgpp_portt AS (

    SELECT * FROM edw.sapc11.zgppport_current
),

txt_mat_grp AS (
	
    SELECT DISTINCT * 
    FROM edw_stage.texts 
    WHERE field = 'mat_grp' 
      AND source_sys = 'C11'
),

final AS (
    
	SELECT 
	  'C11' AS  SOURCE_SYS,
	  MARA.MATNR AS  MATERIAL_CONS,
	  MAKT.MAKTG AS  MATERIALTEXT_CONS,
	  MARA.ATTYP AS  MATERIALCATEG_CONS,
	  MARA.MATKL AS  MATERIALGRP_CONS,
	  MARA.MTART AS  MATERIALTP_CONS,
	  MARA.BISMT AS  OLDMATERIAL_CONS,
	  MARA.BWSCL AS  SOURCESUPPLY_CONS,
	  MARA.EAN11 AS  EAN_CONS,
	  MARA.ERSDA AS  CREATEDON_CONS,
	  MARA.MEINS AS  BASEUNIT_CONS,
	  MARA.PRDHA AS  PRODHIER_CONS,
	  NULL AS PRODHL1_E03,
	  NULL AS  PRODHL1TEXT_E03,
	  NULL AS PRODHL2_E03,
	  NULL  AS  PRODHL2TEXT_E03,
	  NULL AS PRODHL3_E03,
	  NULL AS  PRODHL3TEXT_E03,
	  NULL AS PRODHL4_E03,
	  NULL  AS  PRODHL4TEXT_E03,
	  NULL AS PRODHL5_E03,
	  NULL AS  PRODHL5TEXT_E03,
	  NULL AS PRODHL6_E03,
	  NULL  AS  PRODHL6TEXT_E03,
	  NULL AS  PRODHL1_C11,
	  NULL AS  PRODHL1TEXT_C11,
	  NULL AS  PRODHL2_C11,
	  NULL AS  PRODHL2TEXT_C11,
	  NULL AS  PRODHL3_C11,
	  NULL AS  PRODHL3TEXT_C11,
	  MARA.SPART AS  BRAND_CONS,
	  MARA.WRKST AS  BASICMATERIAL_CONS,
	  SUBSTRING(MARA.WRKST,1,3) AS  GPPSBU_CONS,
	  ZGPP_SBUT.ZSBUDESC AS  GPPSBUTEXT_CONS,
	  SUBSTRING(MARA.WRKST,5,2) AS  GPPDIV_CONS,
	  ZGPP_DIVT.ZSPARTDESC AS  GPPDIVTEXT_CONS,
	  SUBSTRING(MARA.WRKST,8,3) AS  GPPCAT_CONS,
	  ZGPP_CATT.zcatdesc AS  GPPCATTEXT_CONS,
	  SUBSTRING(MARA.WRKST,12,5) AS  GPPPOR_CONS,
	  ZGPP_PORTT.zportdesc AS  GPPPORTEXT_CONS,
	  txt_mat_grp.code_txt AS MATERIALGRP_CONS_TXT,
	  MARA.LOADDTS AS LOADDTS,
	  v_job_name AS  ETL_CRTE_USER,
	  CURRENT_TIMESTAMP AS  ETL_CRTE_TS ,
	  NULL AS  ETL_UPDT_USER,
	  CAST(NULL AS TIMESTAMP) AS  ETL_UPDT_TS
	FROM base_mara AS MARA
	JOIN base_makt AS MAKT
	  ON MARA.MATNR = MAKT.MATNR 
      AND MAKT.SPRAS = 'E' 
	LEFT JOIN base_zgpp_sbut AS ZGPP_SBUT
	  ON ZGPP_SBUT.SPRAS = 'E' 
     AND ZGPP_SBUT.zsbu = SUBSTRING(MARA.WRKST,1,3)
	LEFT JOIN base_zgpp_divt AS ZGPP_DIVT
	  ON ZGPP_DIVT.SPRAS = 'E' 
     AND ZGPP_DIVT.zspart = SUBSTRING(MARA.WRKST,5,2)
	LEFT JOIN base_zgpp_catt AS ZGPP_CATT
	  ON ZGPP_CATT.SPRAS = 'E' 
     AND ZGPP_CATT.zcategory = SUBSTRING(MARA.WRKST,8,3)
	LEFT JOIN base_zgpp_portt AS ZGPP_PORTT
	  ON ZGPP_PORTT.SPRAS = 'E' 
     AND ZGPP_PORTT.zport = SUBSTRING(MARA.WRKST,12,5)
	LEFT JOIN txt_mat_grp 
      ON txt_mat_grp.code = MARA.MATKL
    WHERE MARA.loaddts > v_last_extract_timestamp
)

SELECT * FROM final