WITH udtsrcsupply_current AS (

    SELECT * FROM jda.udtsrcsupply_current
),

base_marc AS (

    SELECT * FROM sape03.marc_current
),

base_makt AS (

    SELECT * FROM sape03.makt_current
),

base_pl AS (

    SELECT * FROM edw.edw_consolidated.consolidated_plants
),

base_uc AS (
    
    SELECT * FROM (
      SELECT 
        u_plant_vendor, 
        u_replen_loc,
        u_plant_vendor_descr,
        u_item,u_loc,
        u_updated,
        ROW_NUMBER() OVER (PARTITION BY u_item, u_loc ORDER BY u_updated DESC) AS row_num
      FROM udtsrcsupply_current
    )
    WHERE row_num = 1
),

base_matpl AS (

    SELECT 
      MARC.MATNR  AS MATERIAL_CONS,
      MARC.WERKS  AS MATERIALPLANT_CONS, 
      MAKT.MAKTG  AS MATERIALTEXT_CONS,
      MARC.MMSTA  AS PLNTMATSTAT_CONS,
      MARC.MAABC  AS ABCIND_CONS,
      MARC.EKGRP  AS PURCHGRP_CONS,
      MARC.DISPO  AS MRPCTRL_CONS,
      MARC.PRCTR  AS PROFITCTR_CONS,
      MARC.LOADDTS AS LOADDTS,
      uc.u_replen_loc AS PROD_PLNT_RPL_LOC,
      uc.u_plant_vendor	AS SOURCESUP_CONS,
      uc.u_plant_vendor_descr AS SOURCESUP_TEXT_CONS
    FROM  base_marc AS MARC
    JOIN base_makt AS MAKT
      ON MARC.MATNR = MAKT.MATNR 
     AND MAKT.SPRAS='E'
    LEFT JOIN base_uc AS uc 
      ON MARC.MATNR = uc.u_item 
     AND MARC.WERKS = uc.u_loc
),

final AS (
	
    SELECT 
      'E03' AS  SOURCE_SYS,
      matpl.MATERIAL_CONS,
      matpl.MATERIALPLANT_CONS, 
      matpl.MATERIALTEXT_CONS,
      matpl.PLNTMATSTAT_CONS,
      matpl.ABCIND_CONS,
      matpl.PURCHGRP_CONS,
      matpl.MRPCTRL_CONS,
      matpl.SOURCESUP_CONS,
      matpl.SOURCESUP_TEXT_CONS,
      matpl.PROFITCTR_CONS,
      matpl.PROD_PLNT_RPL_LOC,

      CASE 
        WHEN pl.plant_txt IS NULL OR pl.plant_txt = '' 
        THEN matpl.SOURCESUP_TEXT_CONS 
        ELSE pl.plant_txt 
      END AS PROD_PLNT_RPL_LOC_DESC,
    
      matpl.LOADDTS,
      v_job_name AS ETL_CRTE_USER,
      CURRENT_TIMESTAMP AS  ETL_CRTE_TS ,
      NULL AS ETL_UPDT_USER,
      CAST(NULL AS TIMESTAMP) AS  ETL_UPDT_TS
    FROM base_matpl AS matpl 
    LEFT JOIN base_pl
      ON matpl.PROD_PLNT_RPL_LOC = pl.plant 
)

SELECT * FROM final 