WITH txt_doc_type as (

    SELECT DISTINCT * 
    FROM edw_stage.texts 
    WHERE field = 'doc_type'
),

txt_reject_code as (

    SELECT DISTINCT * 
    FROM edw_stage.texts
    WHERE field = 'reject_cde'
),

txt_mat_grp as (

    SELECT DISTINCT * 
    FROM edw_stage.texts 
    WHERE field = 'mat_grp'
),

txt_cust_class as (

    SELECT DISTINCT * 
    FROM edw_stage.texts 
    WHERE field = 'cust_class'
),	

calENDar1 as (
    
    SELECT DISTINCT * 
    FROM edw_stage.dim_calENDer 
),

calENDar2 as (
    
    SELECT DISTINCT * 
    FROM edw_stage.dim_calENDer 
),

base_vbap AS (

    SELECT * FROM sape03.vbap_current
),

VBAP as (

    SELECT 
      VBAP.VBELN, 
      VBAP.POSNR,

      CASE 
        WHEN REPLACE(VBAP.AEDAT, '-', '') ~ '^[0-9\.]+$' 
        THEN REPLACE(VBAP.AEDAT, '-', '')
      ELSE '00000000'
      END AEDAT,

      CASE 
        WHEN REPLACE(VBAP.ERDAT, '-', '') ~ '^[0-9\.]+$' 
        THEN REPLACE(VBAP.ERDAT, '-', '')
        ELSE '00000000'
      END ERDAT,   
    
      VBAP.ERNAM,
      VBAP.FAKSP,
      VBAP.KWMENG,
      VBAP.KNUMA_AG,
      VBAP.ABGRU,
      VBAP.ZZREJECTQT,
      VBAP.KNUMA_PI,
      VBAP.WAVWR, 
      VBAP.KZWI6,
      VBAP.MATNR,
      VBAP.VRKME,
      VBAP.NETPR,
      VBAP.NETWR,
      VBAP.PRCTR,
      VBAP.PSTYV,
      VBAP.ZZSALESREP,
      VBAP.WERKS,
      VBAP.erzet,
    
      CASE 
        WHEN REPLACE(VBAP.ZZSLDAT, '-', '') ~ '^[0-9\.]+$' 
        THEN REPLACE(VBAP.ZZSLDAT, '-', '')
        ELSE '0000000'
      END ZZSLDAT,
      
      vbap.vstel, 
      VBAP.LOADDTS 

    FROM base_vbap AS VBAP
),

base_vpba AS (

    SELECT * 
    FROM sape03.vbpa_current VBPA 
    WHERE VBPA.PARVW = 'WE' 
      AND VBPA.POSNR='00000'
)

final AS (

    SELECT 
      'E03' as  SOURCE_SYS,
      VBAP.VBELN as  SALESORDNUM_CONS,
      VBAP.POSNR as  SALESORDITEM_CONS,
      VBAK.VKORG as  SALESORG_CONS,
      VBAK.VTWEG as  SALESDIST_CONS,
      VBAK.SPART as  SALESDIV_CONS,
      VBAK.VKBUR as  SALESOFF_CONS,
      VBAK.AUART as  DOCTYPE_CONS,
      VBAK.AEDAT as  CHANGEONH_CONS,
      VBAK.AUGRU as  ORDERREASON_CONS,

      CASE 
        WHEN REPLACE(VBAK.ERDAT , '-', '') ~ '^[0-9\.]+$' 
        THEN REPLACE(VBAK.ERDAT, '-', '')::bigint
        ELSE 00000000::bigint
      END CREATEDONH_CONS,
    
      VBAK.ERZET as CREATEDTIMEH_CONS,
      VBAK.KUNNR as  SOLDTO_CONS,
      VBAK.VBTYP as  DOCCAT_CONS,
      VBAK.WAERK as  DOCCURR_CONS,
    
      CASE 
        WHEN REPLACE(VBAK.VDATU , '-', '') ~ '^[0-9\.]+$' 
        THEN REPLACE(VBAK.VDATU , '-', '')::bigint
        ELSE 00000000::bigint
      END ORD_REQ_DL_DTE,
    
      VBAK.LIFSK as ORD_DLV_BLCK_SSK,
      VBPA.KUNNR as  SHIPTO_CONS,
      VBAK.VSBED as ORD_SHIP_COND,
      VBAP.VSTEL as ORD_SHIP_POINT,
      VBAP.AEDAT as  CHANGEONI_CONS,
      VBAP.ERDAT as  CREATEDONI_CONS,
      REPLACE(VBAP.ERZET, ':','') as CREATEDTIMEI_CONS,
      VBAP.ERNAM as  CREATEDBY_CONS,
      VBAP.FAKSP as  BILLINGBLOCK_CONS,
      VBAP.KWMENG as  ORDERQTY_CONS,
      VBAP.KNUMA_AG as  SALESDEAL_CONS,
      VBAP.ABGRU AS REJECT_REASON_CD,
      VBAP.ZZREJECTQT AS REJECTQTY_CONS,
      VBAP.KNUMA_PI as  PROMOTION_CONS,
      VBAP.WAVWR as ORDERCOST_CONS,
     
      CASE 
        WHEN VBAK.WAERK = TCURX.currkey
        THEN VBAP.KZWI6 * (10 ^(2-cast(TCURX.currdec as int)))
        ELSE VBAP.KZWI6
      END as  ORDERVALUE_CONS,

      VBAP.MATNR as  MATERIAL_CONS,
      VBAP.VRKME as  SALESUNIT_CONS,
      VBAP.NETPR as  NETPRICE_CONS,
      VBAP.NETWR as  NETVALUE_CONS,
      VBAP.PRCTR as  PROFITCENTER_CONS,
      VBAP.PSTYV as  ITEMCAT_CONS,
      VBAP.WERKS as  PLANT_CONS,
      VBAP.ZZSALESREP as  SALESREP_CONS,
      VBUK.LFSTK as ORD_STAT_DLV,
      VBUK.CMGST as ORD_STAT_CRDT,
      VBAP.ZZSLDAT as  SVCLVLDATE_CONS,
      '0000000' as ORD_OrgTranPlanDt,
      calendar1.fyr_id  as ORD_SVC_LVL_FISCY,
      calendar1.fmth_id as ORD_SVC_LVL_FISCP,
      calendar1.fwk_id  as ORD_SVC_LVL_FISCW,
      calendar2.fyr_id  as ORD_CREAT_FISCY,
      calendar2.fmth_id as ORD_CREAT_FISCP,
      calendar2.fwk_id  as ORD_CREAT_FISCW,
      txt_doc_type.code_txt as DOCTYPE_CONS_TXT,
      txt_reject_code.code_txt as REJECT_REASON_CD_TXT,
      VBAP.LOADDTS as LOADDTS,
      v_job_name as  ETL_CRTE_USER,
      CURRENT_TIMESTAMP  as  ETL_CRTE_TS,
      NULL as  ETL_UPDT_USER,
      CAST(NULL as timestamp) as  ETL_UPDT_TS 
    FROM VBAP
    JOIN sape03.vbak_current VBAK 
      ON VBAP.VBELN = VBAK.VBELN
     AND VBAP.ERDAT >= 20190101
   LEFT JOIN base_vpba AS VBPA
     ON VBAP.VBELN   = VBPA.VBELN
   LEFT JOIN sape03.tcurx_current TCURX
     ON VBAK.WAERK = TCURX.currkey
   LEFT JOIN calendDar1 
     ON calENDar1.dy_id::bigint =  VBAP.ZZSLDAT 
   LEFT JOIN calendar2 
     ON calendar2.dy_id::bigint =  VBAP.ERDAT
   LEFT JOIN sape03.vbuk_current VBUK 
     ON VBAK.vbeln=VBUK.vbeln
   LEFT JOIN txt_doc_type 
     ON txt_doc_type.source_sys = 'E03' 
    AND txt_doc_type.code= VBAK.AUART
   LEFT JOIN txt_reject_code 
     ON txt_reject_code.source_sys = 'E03' 
    AND txt_reject_code.code= VBAP.ABGRU
    WHERE VBAP.loaddts > v_last_extract_timestamp

),

SELECT * FROM final