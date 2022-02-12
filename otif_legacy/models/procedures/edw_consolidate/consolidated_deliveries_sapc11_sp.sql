CREATE OR REPLACE PROCEDURE edw_consolidated.consolidated_deliveries_sapc11_sp()
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
    v_totalcount bigint;
    v_delete_count bigint;
    audit_rec RECORD;
	v_last_extract_timestamp timestamp;
    v_run_seq int;
	
begin
	v_job_id=2;
	SELECT INTO audit_rec * FROM EDW.EDW_STAGE.job_master WHERE job_id = v_job_id;
	v_start_timestamp:=(select current_timestamp);
	v_source_system=audit_rec.source_sys;
	v_job_name=audit_rec.job_name;
    v_table_name=audit_rec.table_name;
    v_runid:=(select pg_backend_pid());
	v_last_extract_timestamp=audit_rec.last_extract_timestamp;
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
  
    drop table if exists CONSOLIDATED_DELIVERIES_STG_SAPC11;
	
	

   
	CREATE TEMPORARY table CONSOLIDATED_DELIVERIES_STG_SAPC11 AS
			WITH LIKP AS ( select
	LIKP.VBELN ,
	LIKP.LFART ,	
	LIKP.WERKS ,	
	LIKP.ROUTE ,	
	case when replace(LIKP.ERDAT , '-', '') ~ '^[0-9\.]+$' then replace(LIKP.ERDAT, '-', '')::bigint
    else 00000000::bigint
	end ERDAT,
	LIKP.ERNAM ,		
	case when replace(LIKP.AEDAT , '-', '') ~ '^[0-9\.]+$' then replace(LIKP.AEDAT, '-', '')::bigint
    else 00000000::bigint
	end AEDAT,
	LIKP.AENAM,	
	case when replace(LIKP.KODAT , '-', '') ~ '^[0-9\.]+$' then replace(LIKP.KODAT, '-', '')::bigint
    else 00000000::bigint
	end KODAT,	
	case when replace(LIKP.LDDAT , '-', '') ~ '^[0-9\.]+$' then replace(LIKP.LDDAT, '-', '')::bigint
    else 00000000::bigint
	end LDDAT,	
	case when replace(LIKP.tddat , '-', '') ~ '^[0-9\.]+$' then replace(LIKP.tddat, '-', '')::bigint
    else 00000000::bigint
	end TDDAT,	
	case when replace(LIKP.LFDAT , '-', '') ~ '^[0-9\.]+$' then replace(LIKP.LFDAT, '-', '')::bigint
    else 00000000::bigint
	end LFDAT,
	
	LIKP.LIFSK,	
	case when replace(LIKP.WADAT , '-', '') ~ '^[0-9\.]+$' then replace(LIKP.WADAT, '-', '')::bigint
    else 00000000::bigint
	end WADAT,	
	
	case when replace(LIKP.WADAT_IST , '-', '') ~ '^[0-9\.]+$' then replace(LIKP.WADAT_IST, '-', '')::bigint
    else 00000000::bigint
	end WADAT_IST,
	
	LIKP.INCO1 ,
	LIKP.INCO2 from sapc11.likp_current LIKP where LIKP.LFART <>  '7'

	)
	
    select 
    DISTINCT 
    'C11' as  SOURCE_SYS,	
	LIPS.VBELN as  DELIVNUM_CONS,	
	LIPS.POSNR as  DELIVITEM_CONS,	
	LIKP.LFART as  DELIVTYPE_CONS,	
	LIKP.WERKS as  PLANT_CONS,	
	LIKP.ROUTE as  ROUTE_CONS,	
	LIKP.ERDAT as  CREATEDONH_CONS,	
	LIKP.ERNAM as  CREATEDBYH_CONS,	
	LIKP.AEDAT as  CHANGEDONH_CONS,	
	LIKP.AENAM as  CHANGEBY_CONS,	
	LIKP.KODAT as  PICKDATE_CONS,	
	LIKP.LDDAT as  LOADDATE_CONS,	
    LIKP.tddat as  transpdate_cons,
	LIKP.LFDAT as  DELIVDATE_CONS,	
	LIKP.LIFSK as  DELIVBLOCK_CONS,	
	LIKP.WADAT as  GIDATE_CONS,
	LIKP.WADAT_IST as  ACTGIDATE_CONS,
	null as OVDL_EST_ACT_DLV_DTE,
	substring(SHIP.completion_dt,1,8) as WMSCONFDATE_CONS,
	LIPS.ZZFSTLFIMG as ODLV_ORIG_QTY,
	CASE WHEN SHIP.completion_dt IS NOT NULL THEN substring(SHIP.completion_dt,1,8) 
	ELSE LIKP.WADAT_IST::text END as  ODLV_OTIF_DTE,
	LIPS.LFIMG as  DELIVQTY_CONS,	
	LIPS.MATNR as  MATERIAL_CONS,	
	LIPS.MEINS as  BASEUNIT_CONS,
	LIKP.INCO1 as INCOTERMS1_CONS,
	LIKP.INCO2 as INCOTERMS2_CONS,
	LIPS.VGBEL as  REFDOC_CONS,	
	LIPS.VGPOS as  REFITEM_CONS,
	LIPS.LOADDTS as LOADDTS,
	v_job_name as  ETL_CRTE_USER,
	current_timestamp as  ETL_CRTE_TS ,	
	null as  ETL_UPDT_USER,	
	cast(null as timestamp) as  ETL_UPDT_TS 	
	from sapc11.lips_current LIPS
	join  LIKP   
	on (    LIPS.VBELN = LIKP.VBELN
			AND LIKP.ERDAT >= 20190101
			and LIPS.posnr is not null
       )	
    left join otif.edw_shipments SHIP on ( 
                    LIPS.vbeln = SHIP.sap_delivery
                and LIPS.POSNR = SHIP.line_number );
   --where LIPS.loaddts>v_last_extract_timestamp
    
               
               
               RAISE NOTICE 'TEMPORARY TABLE CREATED CONSOLIDATED_DELIVERIES_STG';
    --GET DIAGNOSTICS v_totalcount:= ROW_COUNT;
    
    ----INSERTS & UPDATES----
	delete from EDW.EDW_CONSOLIDATED.CONSOLIDATED_DELIVERIES
	using CONSOLIDATED_DELIVERIES_STG_SAPC11 deliveries_stg
	where deliveries_stg.DELIVNUM_CONS=CONSOLIDATED_DELIVERIES.DELIVNUM_CONS
	and deliveries_stg.DELIVITEM_CONS=CONSOLIDATED_DELIVERIES.DELIVITEM_CONS
	and deliveries_stg.source_sys=CONSOLIDATED_DELIVERIES.source_sys;
    RAISE NOTICE 'DELETED RECORDS FROM % % ', v_source_system, v_table_name;
    GET DIAGNOSTICS v_delete_count:= ROW_COUNT;

    insert into EDW.EDW_CONSOLIDATED.CONSOLIDATED_DELIVERIES
	select * from CONSOLIDATED_DELIVERIES_STG_SAPC11;
	GET DIAGNOSTICS v_insert_count:= ROW_COUNT;
    v_endtimestamp:=(select current_timestamp);
    RAISE NOTICE 'INSERTED RECORDS INTO % %; RECORD COUNT:%', v_source_system, v_table_name, v_insert_count;
	v_exit_code=0;		

	update edw.edw_stage.job_master
	set job_state=case when v_exit_code=0 then 'SUCCESS' ELSE 'FAILED' end,
	last_extract_timestamp=(select max(loaddts) from EDW.EDW_CONSOLIDATED.CONSOLIDATED_DELIVERIES where source_sys=v_source_system),
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