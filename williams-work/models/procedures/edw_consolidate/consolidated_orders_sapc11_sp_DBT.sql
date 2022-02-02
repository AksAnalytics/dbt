WITH txt_doc_type as (

    SELECT DISTINCT * 
    FROM edw_stage.texts 
    WHERE field = 'doc_type' 
      AND source_sys = 'C11'
),

txt_reject_code as (
    
    SELECT DISTINCT * 
    FROM edw_stage.texts 
    WHERE field = 'reject_cde' 
      AND source_sys = 'C11'
),

txt_mat_grp as (

    SELECT DISTINCT * 
    FROM edw_stage.texts 
    WHERE field = 'mat_grp' 
      AND source_sys = 'C11'
),

txt_cust_class as (

    SELECT DISTINCT * 
    FROM edw_stage.texts 
    WHERE field = 'cust_class' 
      AND source_sys = 'C11'
),

 calendar1 as (

    SELECT DISTINCT * 
    FROM edw_stage.dim_calENDer 
),

calENDar2 as (

    SELECT DISTINCT * 
    FROM edw_stage.dim_calENDer 
),

base_vbap AS (

    SELECT * FROM sapc11.vbap_current
),

base_vbpa AS (

    SELECT  *
    FROM sapc11.vbpa_current AS VBPA
    WHERE VBPA.PARVW = 'WE'
      AND VBPA.POSNR = '00000'
),

vbap as  (

    SELECT 
      VBAP.VBELN, 
      VBAP.POSNR,
        
      CASE 
        WHEN REPLACE(VBAP.AEDAT, '-', '') ~ '^[0-9\.]+$' 
          THEN REPLACE(VBAP.AEDAT, '-', '')::bigint
          ELSE 00000000::bigint
      END AEDAT,

      CASE 
        WHEN REPLACE(VBAP.ERDAT, '-', '') ~ '^[0-9\.]+$' 
        THEN REPLACE(VBAP.ERDAT, '-', '')::bigint
        ELSE 00000000::bigint
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
      VBAP.WERKS,
      VBAP.erzet,
      
      CASE 
        WHEN REPLACE(VBAP.SLDAT, '-', '') ~ '^[0-9\.]+$' 
        THEN REPLACE(VBAP.SLDAT, '-', '')::bigint
        ELSE 00000000::bigint
      END SLDAT,
   
      CASE 
        WHEN REPLACE(VBAP.ZZFSTTDDAT, '-', '') ~ '^[0-9\.]+$' 
        THEN REPLACE(VBAP.ZZFSTTDDAT, '-', '')::bigint
        ELSE 00000000::bigint
      END ZZFSTTDDAT,
    
      vbap.vstel, 
      VBAP.LOADDTS 
    FROM base_vbap AS VBAP 
),

final AS (
    
    SELECT
      'C11' as SOURCE_SYS,
      VBAP.VBELN as SALESORDNUM_CONS,
      VBAP.POSNR as SALESORDITEM_CONS,
      VBAK.VKORG as SALESORG_CONS,
      VBAK.VTWEG as SALESDIST_CONS,
      VBAK.SPART as SALESDIV_CONS,
      VBAK.VKBUR as SALESOFF_CONS,
      VBAK.AUART as DOCTYPE_CONS,
      VBAK.AEDAT as CHANGEONH_CONS,
      VBAK.AUGRU as ORDERREASON_CONS,
      
      CASE
        WHEN REPLACE(VBAK.ERDAT , '-', '') ~ '^[0-9\.]+$' 
        THEN REPLACE(VBAK.ERDAT, '-', '')::bigint
        ELSE 00000000::bigint
      END CREATEDONH_CONS,
    
      VBAK.ERZET as CREATEDTIMEH_CONS,
      VBAK.KUNNR as SOLDTO_CONS,
      VBAK.VBTYP as DOCCAT_CONS,
      VBAK.WAERK as DOCCURR_CONS,
    
      CASE
        WHEN REPLACE(VBAK.VDATU , '-', '') ~ '^[0-9\.]+$' 
        THEN REPLACE(VBAK.VDATU , '-', '')::bigint
        ELSE 00000000::bigint
      END ORD_REQ_DL_DTE,

      VBAK.LIFSK as ORD_DLV_BLCK_SSK,
      VBPA.KUNNR as SHIPTO_CONS,
      VBAK.VSBED as ORD_SHIP_COND,
      VBAP.VSTEL as ORD_SHIP_POINT,
      VBAP.AEDAT as CHANGEONI_CONS,
      VBAP.ERDAT as CREATEDONI_CONS,
      REPLACE(VBAP.ERZET, ':', '') as CREATEDTIMEI_CONS,
      VBAP.ERNAM as CREATEDBY_CONS,
      VBAP.FAKSP as BILLINGBLOCK_CONS,
      VBAP.KWMENG as ORDERQTY_CONS,
      VBAP.KNUMA_AG as SALESDEAL_CONS,
      VBAP.ABGRU as REJECT_REASON_CD,
      VBAP.ZZREJECTQT as REJECTQTY_CONS,
      VBAP.KNUMA_PI as PROMOTION_CONS,
      VBAP.WAVWR as ORDERCOST_CONS,
      VBAP.KZWI6 as ORDERVALUE_CONS,
      VBAP.MATNR as MATERIAL_CONS,
      VBAP.VRKME as SALESUNIT_CONS,
      VBAP.NETPR as NETPRICE_CONS,
      VBAP.NETWR as NETVALUE_CONS,
      VBAP.PRCTR as PROFITCENTER_CONS,
      VBAP.PSTYV as ITEMCAT_CONS,
      VBAP.WERKS as PLANT_CONS,
      NULL as SALESREP_CONS,
      VBUK.LFSTK as ORD_STAT_DLV,
      VBUK.CMGST as ORD_STAT_CRDT,
      VBAP.SLDAT as SVCLVLDATE_CONS,
      VBAP.ZZFSTTDDAT as ORD_OrgTranPlANDt,
      calendar1.fyr_id as ORD_SVC_LVL_FISCY,
      calendar1.fmth_id as ORD_SVC_LVL_FISCP,
      calendar1.fwk_id as ORD_SVC_LVL_FISCW,
      calendar2.fyr_id as ORD_CREAT_FISCY,
      calendar2.fmth_id as ORD_CREAT_FISCP,
      calendar2.fwk_id as ORD_CREAT_FISCW,
      txt_doc_type.code_txt as DOCTYPE_CONS_TXT,
      txt_reject_code.code_txt as REJECT_REASON_CD_TXT,
      VBAP.LOADDTS as LOADDTS,
      v_job_name as ETL_CRTE_USER,
      CURRENT_TIMESTAMP as ETL_CRTE_TS,
      NULL as ETL_UPDT_USER,
      CAST(NULL AS TIMESTAMP) as ETL_UPDT_TS
    FROM VBAP
    JOIN sapc11.vbak_current VBAK 
      ON VBAP.VBELN = VBAK.VBELN
     AND VBAP.ERDAT >= 20190101
    LEFT JOIN base_vbpa AS VBPA
      ON VBAP.VBELN = vbpa.VBELN 
    LEFT JOIN sapc11.vbuk_current VBUK 
      ON VBAK.vbeln = VBUK.vbeln
    LEFT JOIN calendar1 
      ON calendar1.dy_id::bigint = VBAP.SLDAT ::bigint
    LEFT JOIN calendar2 
      ON calendar2.dy_id::bigint = VBAP.ERDAT ::bigint
    LEFT JOIN txt_doc_type 
      ON txt_doc_type.code = VBAK.AUART
    LEFT JOIN txt_reject_code 
      ON txt_reject_code.code = VBAP.ABGRU
    WHERE VBAP.loaddts > v_last_extract_TIMESTAMP

)

SELECT * FROM final