CREATE OR REPLACE PROCEDURE edw_consolidated.consolidated_orders_sape03_sp()
	LANGUAGE plpgsql
AS $$
	
	
	
	
	
	
	
	
	
	
	
	
	
declare
	v_job_name varchar(100);
    v_table_name varchar(100);
    v_source_system varchar(50);
	v_job_id int;
	v_exit_code int;
    v_runid int;
	v_start_timestamp timestamp;
	v_endtimestamp timestamp;
    v_insert_count bigint;
    v_delete_count bigint;
    audit_rec RECORD;
	v_last_extract_timestamp timestamp;
	v_run_seq int;
	
begin
	v_job_id=7;
	SELECT INTO audit_rec * FROM EDW.EDW_STAGE.job_master WHERE job_id = v_job_id;
	v_start_timestamp:=(select current_timestamp);
	v_source_system=audit_rec.source_sys;
	v_job_name=audit_rec.job_name;
    v_table_name=audit_rec.table_name;
    v_runid=(select pg_backend_pid());
    v_last_extract_timestamp=nvl(audit_rec.last_extract_timestamp,'1900-01-01 00:00:00');
    v_exit_code=-1;
	v_run_seq:=(select coalesce((max(run_seq)+1),1) from EDW_STAGE.job_history where job_id=v_job_id and run_id=v_runid);
    
    ---Entry Into Job History Table----
   
    insert into EDW.EDW_STAGE.job_history
	 select  
	 v_runid as run_id
	,v_job_id as job_id
	,v_job_name
	,v_table_name
	,to_date(GETDATE(), 'yyyy-MM-DD HH24:MI:SS') run_date
	,v_start_timestamp
	,null as end_timestamp
	,v_run_seq run_seq
	,'STARTED' as job_status
	,'ETL_USER' as etl_crte_user
	,current_timestamp etl_crte_ts
	,'ETL_USER' as etl_updt_user
	,null etl_updt_ts;
	
    COMMIT;
    RAISE NOTICE 'RECORD INSERTED INTO JOB_HISTORY';
   
    drop table if exists CONSOLIDATED_ORDERS_STG_SAPE03;
   
   
	CREATE TEMPORARY TABLE CONSOLIDATED_ORDERS_STG_SAPE03 AS
	with txt_doc_type as (
	select distinct * from edw_stage.texts where field = 'doc_type'
					  )

	,txt_reject_code as (
	select distinct * from edw_stage.texts where field = 'reject_cde'
	)

	,txt_mat_grp as (
	select distinct * from edw_stage.texts where field = 'mat_grp'
	)

	,txt_cust_class as (
	select distinct * from edw_stage.texts where field = 'cust_class'
	)	
	,calendar1 as (
	select distinct * from edw_stage.dim_calender 
	)
	,calendar2 as (
	select distinct * from edw_stage.dim_calender 
	)
	, VBAP as (
select VBAP.VBELN , VBAP.POSNR ,
    case when replace(VBAP.AEDAT, '-', '') ~ '^[0-9\.]+$' then replace(VBAP.AEDAT, '-', '')
        else '00000000'
    end AEDAT,
    case when replace(VBAP.ERDAT, '-', '') ~ '^[0-9\.]+$' then replace(VBAP.ERDAT, '-', '')
        else '00000000'
    end ERDAT,   
    VBAP.ERNAM,VBAP.FAKSP,VBAP.KWMENG,VBAP.KNUMA_AG ,VBAP.ABGRU ,VBAP.ZZREJECTQT ,VBAP.KNUMA_PI ,
    VBAP.WAVWR ,VBAP.KZWI6 ,VBAP.MATNR ,VBAP.VRKME ,VBAP.NETPR ,VBAP.NETWR ,VBAP.PRCTR ,VBAP.PSTYV ,VBAP.ZZSALESREP ,
    VBAP.WERKS ,VBAP.erzet,
    case when replace(VBAP.ZZSLDAT, '-', '') ~ '^[0-9\.]+$' then replace(VBAP.ZZSLDAT, '-', '')
        else '0000000'
    end ZZSLDAT
    ,vbap.vstel, VBAP.LOADDTS from sape03.vbap_current  VBAP)
	
    select 
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
	case when replace(VBAK.ERDAT , '-', '') ~ '^[0-9\.]+$' then replace(VBAK.ERDAT, '-', '')::bigint
    else 00000000::bigint
	end  CREATEDONH_CONS,
	VBAK.ERZET as CREATEDTIMEH_CONS,
	VBAK.KUNNR as  SOLDTO_CONS,
	VBAK.VBTYP as  DOCCAT_CONS,
	VBAK.WAERK as  DOCCURR_CONS,
	case when replace(VBAK.VDATU , '-', '') ~ '^[0-9\.]+$' then replace(VBAK.VDATU , '-', '')::bigint
    else 00000000::bigint
	end ORD_REQ_DL_DTE,
	VBAK.LIFSK as ORD_DLV_BLCK_SSK,
	VBPA.KUNNR as  SHIPTO_CONS,
	VBAK.VSBED as ORD_SHIP_COND,
	VBAP.VSTEL as ORD_SHIP_POINT,
	VBAP.AEDAT as  CHANGEONI_CONS,
	VBAP.ERDAT as  CREATEDONI_CONS,
	replace(VBAP.ERZET, ':','') as CREATEDTIMEI_CONS,
	VBAP.ERNAM as  CREATEDBY_CONS,
	VBAP.FAKSP as  BILLINGBLOCK_CONS,
	VBAP.KWMENG as  ORDERQTY_CONS,
	VBAP.KNUMA_AG as  SALESDEAL_CONS,
	VBAP.ABGRU AS REJECT_REASON_CD,
	VBAP.ZZREJECTQT AS REJECTQTY_CONS,
	VBAP.KNUMA_PI as  PROMOTION_CONS,
	VBAP.WAVWR as ORDERCOST_CONS,
	Case when VBAK.WAERK = TCURX.currkey
	  then VBAP.KZWI6 * (10 ^(2-cast(TCURX.currdec as int)))
	  else VBAP.KZWI6
	end as  ORDERVALUE_CONS,
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
	-- 27/10
	'0000000' as ORD_OrgTranPlanDt,
	-- 27/10
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
	current_timestamp  as  ETL_CRTE_TS,
	null as  ETL_UPDT_USER,
	CAST(null as timestamp) as  ETL_UPDT_TS 
	from VBAP
	join sape03.vbak_current VBAK 
     on (
          VBAP.VBELN = VBAK.VBELN
          AND VBAP.ERDAT >= 20190101
     )
	left join (select * from sape03.vbpa_current VBPA where VBPA.PARVW = 'WE' and VBPA.POSNR='00000') VBPA
        on ( VBAP.VBELN   = VBPA.VBELN )
	left join sape03.tcurx_current TCURX
      on(VBAK.WAERK = TCURX.currkey)
    left join calendar1 ON calendar1.dy_id::bigint =  VBAP.ZZSLDAT 
    left join calendar2 ON calendar2.dy_id::bigint =  VBAP.ERDAT
    left join sape03.vbuk_current VBUK ON VBAK.vbeln=VBUK.vbeln
    LEFT JOIN txt_doc_type ON txt_doc_type.source_sys = 'E03' AND txt_doc_type.code= VBAK.AUART
	LEFT JOIN txt_reject_code ON txt_reject_code.source_sys = 'E03' AND txt_reject_code.code= VBAP.ABGRU
	where VBAP.loaddts>v_last_extract_timestamp;
    RAISE NOTICE 'TEMPORARY TABLE CREATED CONSOLIDATED_ORDERS_STG'; 

    
    
    ----INSERTS & UPDATES----
	delete from EDW.EDW_CONSOLIDATED.CONSOLIDATED_ORDERS
	using CONSOLIDATED_ORDERS_STG_SAPE03 orders_stg
	where orders_stg.salesordnum_cons=CONSOLIDATED_ORDERS.salesordnum_cons
	and orders_stg.salesorditem_cons=CONSOLIDATED_ORDERS.salesorditem_cons
	and orders_stg.source_sys=CONSOLIDATED_ORDERS.source_sys;
	GET DIAGNOSTICS v_delete_count:= ROW_COUNT;
    RAISE NOTICE 'DELETED RECORDS FROM % % ', v_source_system, v_table_name;

    insert into EDW.EDW_CONSOLIDATED.CONSOLIDATED_ORDERS
	select * from CONSOLIDATED_ORDERS_STG_SAPE03;
	GET DIAGNOSTICS v_insert_count:= ROW_COUNT;
    v_endtimestamp:=(select current_timestamp);
    RAISE NOTICE 'INSERTED RECORDS INTO % %; RECORD COUNT:%', v_source_system, v_table_name, v_insert_count;
	v_exit_code=0;		

	update edw.edw_stage.job_master
	set job_state=case when v_exit_code=0 then 'SUCCESS' ELSE 'FAILED' end,
	last_extract_timestamp=(select max(loaddts) from EDW.EDW_CONSOLIDATED.CONSOLIDATED_ORDERS where source_sys=v_source_system),
	etl_updt_ts=current_timestamp
	where job_id=v_job_id;
	RAISE NOTICE 'RECORD UPDATED IN JOB_MASTER';

    update EDW.EDW_STAGE.job_history
	set job_status=case when v_exit_code=0 then 'SUCCESS' ELSE 'FAILED' end,
	end_timestamp=v_endtimestamp,
	etl_updt_ts=current_timestamp,
	insert_count=v_insert_count,
	delete_count=v_delete_count
	where run_id=v_runid and job_id=v_job_id and run_seq=v_run_seq;
    RAISE NOTICE 'RECORD UPDATED IN JOB_HISTORY';
    RAISE NOTICE 'DELTA LOAD FOR % % COMPLETED SUCCESSFULLY', v_source_system, v_table_name;
	
    GRANT SELECT  ON ALL TABLES IN SCHEMA OTIF TO GROUP "G-ADA-OTIFOpt-RO";
   
	-----EXCEPTION HANDLING------
	EXCEPTION
       WHEN others THEN
       	  RAISE EXCEPTION 'GOT EXCEPTION:SQLSTATE: % SQLERRM: % FOR JOB: % AND JOB ID:%', SQLSTATE, SQLERRM,v_job_name,v_job_id;          
end;












$$
;