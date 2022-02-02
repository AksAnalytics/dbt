CREATE OR REPLACE PROCEDURE edw_consolidated.consolidated_customer_sales_sapc11_sp()
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
	v_job_id=6;
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
   
    drop table if exists CONSOLIDATED_CUSTOMER_SALES_STG_SAPC11;
   
	CREATE TEMPORARY table CONSOLIDATED_CUSTOMER_SALES_STG_SAPC11 AS
	select 
	'C11' as SOURCE_SYS,
	KNVV.KUNNR  as  CUSTOMER_CONS,
	KNVV.SPART  as  SALESDIV_CONS,
	KNVV.VKORG  as  SALESORG_CONS,
	KNVV.VTWEG  as  SALESDIST_CONS,
	KNVV.VKGRP  as  SALES_GROUP_CONS,
	KNVV.VKBUR  as  SALES_OFFICE_CONS,
	KNA1.BRSCH  as  INDUSTRY_KEY_CONS,
	KNA1.KTOKD  as  ACCGROUP_CONS,
	KNA1.KUKLA  as  CUSTCLASS_CONS,
	KNA1.LAND1  as  COUNTRY_CONS,
	KNA1.NAME1  as  NAME_CONS,
	KNA1.ORT01  as  CITY_CONS,
	KNA1.REGIO  as  REGION_CONS,
	ZCHCLKP_C.zchcid as CUST_CHAN_CODE,
	ZCHCLKP_S.zchcid as CUST_SUB_CHAN_CODE,
	ZCHCLKP_C.TXTMD as CUST_CHAN_TXT,
	ZCHCLKP_S.TXTMD as CUST_SUB_CHAN_TXT,
	NULL  as  CUST_FIELD_ZONE,
	NULL  as  CUST_FIELD_AREA,
	NULL  as  CUST_FIELD_TERR,
	KNA1.KATR1  as  CUST_TYPE,
	U_CUST_DMDGROUP as  C11DEMAND_GROUP,
	KNVV.LOADDTS as LOADDTS,
	v_job_name as  ETL_CRTE_USER,
	current_timestamp as  ETL_CRTE_TS,
	null as  ETL_UPDT_USER,
	cast(null as timestamp) as  ETL_UPDT_TS
	from sapc11.knvv_current KNVV
	join sapc11.kna1_current KNA1
	on (KNA1.KUNNR = KNVV.KUNNR)
	--join sapc11.knvh_current KNVH 
	--on (KNVH.KUNNR =KNA1.KUNNR)
	left join sapc11.zchclkp_current ZCHCLKP_C
	on(KNVV.VKGRP = ZCHCLKP_C.ZSLGPFRM
        AND KNVV.VKBUR = ZCHCLKP_C.ZSLOFRM
        AND ZCHCLKP_C.ZCHCIND = 'C'
	  )
	left join sapc11.zchclkp_current ZCHCLKP_S
	on(KNVV.VKGRP = ZCHCLKP_S.ZSLGPFRM
        AND KNVV.VKBUR = ZCHCLKP_S.ZSLOFRM
        AND ZCHCLKP_S.ZCHCIND = 'S'
	  )
	left join jda.udtdfutranscust_current  TRANSCUST
	on (TRANSCUST.U_CUST_CODE = KNVV.KUNNR 
	and TRANSCUST.U_CUST_SALESORG = KNVV.VKORG);

    --where KNVV.loaddts>v_last_extract_timestamp;
    RAISE NOTICE 'TEMPORARY TABLE CREATED CONSOLIDATED_CUSTOMER_SALES_STG';
    
    ----INSERTS & UPDATES----
	delete from EDW.EDW_CONSOLIDATED.CONSOLIDATED_CUSTOMER_SALES
	using CONSOLIDATED_CUSTOMER_SALES_STG_SAPC11 customer_sales_stg
	where customer_sales_stg.CUSTOMER_CONS=CONSOLIDATED_CUSTOMER_SALES.CUSTOMER_CONS
	and customer_sales_stg.SALESDIV_CONS=CONSOLIDATED_CUSTOMER_SALES.SALESDIV_CONS
	and customer_sales_stg.SALESORG_CONS=CONSOLIDATED_CUSTOMER_SALES.SALESORG_CONS
	and customer_sales_stg.SALESDIST_CONS=CONSOLIDATED_CUSTOMER_SALES.SALESDIST_CONS
	and customer_sales_stg.source_sys=CONSOLIDATED_CUSTOMER_SALES.source_sys;
	GET DIAGNOSTICS v_delete_count:= ROW_COUNT;
    RAISE NOTICE 'DELETED RECORDS FROM % % ', v_source_system, v_table_name;

    insert into EDW.EDW_CONSOLIDATED.CONSOLIDATED_CUSTOMER_SALES
	select * from CONSOLIDATED_CUSTOMER_SALES_STG_SAPC11;
	GET DIAGNOSTICS v_insert_count:= ROW_COUNT;
    v_endtimestamp:=(select current_timestamp);
    RAISE NOTICE 'INSERTED RECORDS INTO % %; RECORD COUNT:%', v_source_system, v_table_name, v_insert_count;
	v_exit_code=0;		

    --lock edw.edw_stage.job_master;
	update edw.edw_stage.job_master
	set job_state=case when v_exit_code=0 then 'SUCCESS' ELSE 'FAILED' end,
	last_extract_timestamp=(select max(loaddts) from EDW.EDW_CONSOLIDATED.CONSOLIDATED_CUSTOMER_SALES where source_sys=v_source_system),
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