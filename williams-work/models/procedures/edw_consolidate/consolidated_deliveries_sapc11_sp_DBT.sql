  -- Imports

WITH base_LIKP AS (

    SELECT * FROM sapc11.likp_current
),

base_LIPS AS (

    SELECT * FROM sapc11.lips_current
),

base_SHIP AS (

    SELECT * FROM otif.edw_shipments
),

LIKP AS ( 
    
    SELECT
	  LIKP.VBELN,
	  LIKP.LFART,	
	  LIKP.WERKS,	
	  LIKP.ROUTE,	
	  
      CASE 
        WHEN REPLACE(LIKP.ERDAT , '-', '') ~ '^[0-9\.]+$' THEN REPLACE(LIKP.ERDAT, '-', '')::bigint
        ELSE 00000000::bigint
	  END ERDAT,

	  LIKP.ERNAM,		
	
      CASE 
        WHEN REPLACE(LIKP.AEDAT , '-', '') ~ '^[0-9\.]+$' THEN REPLACE(LIKP.AEDAT, '-', '')::bigint
        ELSE 00000000::bigint
	  END AEDAT,
	
      LIKP.AENAM,	
	
      CASE 
        WHEN REPLACE(LIKP.KODAT , '-', '') ~ '^[0-9\.]+$' THEN REPLACE(LIKP.KODAT, '-', '')::bigint
        ELSE 00000000::bigint
	  END KODAT,	
	
      CASE 
        WHEN REPLACE(LIKP.LDDAT , '-', '') ~ '^[0-9\.]+$' THEN REPLACE(LIKP.LDDAT, '-', '')::bigint
        ELSE 00000000::bigint
	  END LDDAT,	
	
      CASE 
        WHEN REPLACE(LIKP.tddat , '-', '') ~ '^[0-9\.]+$' THEN REPLACE(LIKP.tddat, '-', '')::bigint
        ELSE 00000000::bigint
	  END TDDAT,	
	
      CASE 
        WHEN REPLACE(LIKP.LFDAT , '-', '') ~ '^[0-9\.]+$' THEN REPLACE(LIKP.LFDAT, '-', '')::bigint
        ELSE 00000000::bigint
	  END LFDAT,
	
	  LIKP.LIFSK,	
	
      CASE 
        WHEN REPLACE(LIKP.WADAT , '-', '') ~ '^[0-9\.]+$' THEN REPLACE(LIKP.WADAT, '-', '')::bigint
        ELSE 00000000::bigint
	  END WADAT,	
	
	  CASE 
        WHEN REPLACE(LIKP.WADAT_IST , '-', '') ~ '^[0-9\.]+$' THEN REPLACE(LIKP.WADAT_IST, '-', '')::bigint
        ELSE 00000000::bigint
	  END WADAT_IST,
	
	  LIKP.INCO1,
	  LIKP.INCO2 
    FROM base_LIKP AS LIKP 
    WHERE LIKP.LFART <> '7'

),

final AS (
	
    SELECT DISTINCT 
      'C11' AS  SOURCE_SYS,	
	  LIPS.VBELN AS  DELIVNUM_CONS,	
	  LIPS.POSNR AS  DELIVITEM_CONS,	
	  LIKP.LFART AS  DELIVTYPE_CONS,	
	  LIKP.WERKS AS  PLANT_CONS,	
	  LIKP.ROUTE AS  ROUTE_CONS,	
	  LIKP.ERDAT AS  CREATEDONH_CONS,	
	  LIKP.ERNAM AS  CREATEDBYH_CONS,	
	  LIKP.AEDAT AS  CHANGEDONH_CONS,	
	  LIKP.AENAM AS  CHANGEBY_CONS,	
	  LIKP.KODAT AS  PICKDATE_CONS,	
	  LIKP.LDDAT AS  LOADDATE_CONS,	
      LIKP.tddat AS  transpdate_cons,
	  LIKP.LFDAT AS  DELIVDATE_CONS,	
	  LIKP.LIFSK AS  DELIVBLOCK_CONS,	
	  LIKP.WADAT AS  GIDATE_CONS,
	  LIKP.WADAT_IST AS  ACTGIDATE_CONS,
	  NULL AS OVDL_EST_ACT_DLV_DTE,
	  SUBSTRING(SHIP.completion_dt,1,8) AS WMSCONFDATE_CONS,
	  LIPS.ZZFSTLFIMG AS ODLV_ORIG_QTY,
	
      CASE 
        WHEN SHIP.completion_dt IS NOT NULL THEN SUBSTRING(SHIP.completion_dt,1,8) 
	    ELSE LIKP.WADAT_IST::text 
      END AS  ODLV_OTIF_DTE,

	  LIPS.LFIMG AS  DELIVQTY_CONS,	
      LIPS.MATNR AS  MATERIAL_CONS,	
	  LIPS.MEINS AS  BASEUNIT_CONS,
	  LIKP.INCO1 AS INCOTERMS1_CONS,
	  LIKP.INCO2 AS INCOTERMS2_CONS,
	  LIPS.VGBEL AS  REFDOC_CONS,	
	  LIPS.VGPOS AS  REFITEM_CONS,
	  LIPS.LOADDTS AS LOADDTS,
	  v_job_name AS  ETL_CRTE_USER,
	  CURRENT_TIMESTAMP AS  ETL_CRTE_TS,	
	  NULL AS  ETL_UPDT_USER,	
	  CAST(NULL AS timestamp) AS  ETL_UPDT_TS 	
	
    FROM base_LIPS AS LIPS
	JOIN LIKP   
	  ON LIPS.VBELN = LIKP.VBELN
	 AND LIKP.ERDAT >= 20190101
	 AND LIPS.posnr IS NOT NULL
    LEFT JOIN base_SHIP AS SHIP 
      ON LIPS.vbeln = SHIP.sap_delivery
     AND LIPS.POSNR = SHIP.line_number 
   --WHERE LIPS.loaddts>v_lASt_extract_timestamp
)

SELECT * FROM FINAL
               
               
