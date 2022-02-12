-- Imports

WITH base_LIKP AS (

    SELECT * FROM sape03.likp_current
),

base_LIPS AS (

    SELECT * FROM sape03.lips_current
),

LIKP AS ( 
    
    SELECT
	  LIKP.VBELN,
	  LIKP.LFART,	
	  LIKP.WERKS,	
	  LIKP.ROUTE,	
	
      CASE 
        WHEN replace(LIKP.ERDAT , '-', '') ~ '^[0-9\.]+$' THEN replace(LIKP.ERDAT, '-', '')::bigint
        ELSE 00000000::bigint
	  END ERDAT,
	
      LIKP.ERNAM,		
	
      CASE 
        WHEN replace(LIKP.AEDAT , '-', '') ~ '^[0-9\.]+$' THEN replace(LIKP.AEDAT, '-', '')::bigint
        ELSE 00000000::bigint
	  END AEDAT,
	  
      LIKP.AENAM,	
	
      CASE 
        WHEN replace(LIKP.KODAT , '-', '') ~ '^[0-9\.]+$' THEN replace(LIKP.KODAT, '-', '')::bigint
        ELSE 00000000::bigint
	  END KODAT,	
	
      CASE 
        WHEN replace(LIKP.LDDAT , '-', '') ~ '^[0-9\.]+$' THEN replace(LIKP.LDDAT, '-', '')::bigint
        ELSE 00000000::bigint
	  END LDDAT,	
	
      CASE 
        WHEN replace(LIKP.tddat , '-', '') ~ '^[0-9\.]+$' THEN replace(LIKP.tddat, '-', '')::bigint
        ELSE 00000000::bigint
	  END TDDAT,	
	
      CASE 
        WHEN replace(LIKP.LFDAT , '-', '') ~ '^[0-9\.]+$' THEN replace(LIKP.LFDAT, '-', '')::bigint
        ELSE 00000000::bigint
	  END LFDAT,
	
	  LIKP.LIFSK,

	  CASE 
        WHEN replace(LIKP.WADAT , '-', '') ~ '^[0-9\.]+$' THEN replace(LIKP.WADAT, '-', '')::bigint
        ELSE 00000000::bigint
	  END WADAT,	
	
	  CASE 
        WHEN replace(LIKP.WADAT_IST , '-', '') ~ '^[0-9\.]+$' THEN replace(LIKP.WADAT_IST, '-', '')::bigint
        ELSE 00000000::bigint
	  END WADAT_IST,
		
	  CASE 
        WHEN replace(LIKP.ZZADLYEDAT , '-', '') ~ '^[0-9\.]+$' THEN replace(LIKP.ZZADLYEDAT, '-', '')::bigint
        ELSE 00000000::bigint
	  END ZZADLYEDAT,
	
	  LIKP.INCO1,
	  LIKP.INCO2 
    FROM base_LIKP AS LIKP 
    WHERE LIKP.LFART <> '7'

),

final AS (
	
    SELECT 
      'E03' AS  SOURCE_SYS,
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
      likp.ZZADLYEDAT AS OVDL_EST_ACT_DLV_DTE,
      NULL AS    WMSCONFDATE_CONS,
      LIPS.ZZFSTLFIMG AS ODLV_ORIG_QTY,
      LIKP.WADAT_IST AS ODLV_OTIF_DTE,
      LIPS.LFIMG AS  DELIVQTY_CONS,
      LIPS.MATNR AS  MATERIAL_CONS,
      LIPS.MEINS AS  BASEUNIT_CONS,
      LIKP.INCO1 AS INCOTERMS1_CONS,
      LIKP.INCO2 AS INCOTERMS2_CONS,
      LIPS.VGBEL AS  REFDOC_CONS,
      LIPS.VGPOS AS  REFITEM_CONS,
      LIPS.LOADDTS AS LOADDTS,
      v_job_name AS  ETL_CRTE_USER,
      CURRENT_TIMESTAMP AS  ETL_CRTE_TS ,
      NULL AS  ETL_UPDT_USER,
      CAST(NULL AS TIMESTAMP) AS  ETL_UPDT_TS
    FROM base_LIPS AS LIPS
    JOIN LIKP 
      ON LIPS.VBELN = LIKP.VBELN
     AND LIKP.ERDAT >= 20190101
     AND LIPS.posnr IS NOT NULL
    --WHERE LIPS.loaddts>v_lASt_extract_timestamp;
)

SELECT * FROM final