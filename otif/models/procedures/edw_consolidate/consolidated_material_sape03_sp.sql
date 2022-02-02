CREATE OR REPLACE PROCEDURE edw_consolidated.consolidated_material_sape03_sp()
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
	v_job_id=9;
	SELECT INTO audit_rec * FROM EDW.EDW_STAGE.job_master WHERE job_id = v_job_id;
	v_start_timestamp:=(select current_timestamp);
	v_source_system=audit_rec.source_sys;
	v_job_name=audit_rec.job_name;
    v_table_name=audit_rec.table_name;
    v_runid=(select pg_backend_pid()) ;
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
   
    drop table if exists CONSOLIDATED_MATERIAL_STG_SAPE03;
   
	CREATE TEMPORARY TABLE CONSOLIDATED_MATERIAL_STG_SAPE03 AS
	with txt_mat_grp as (
	select distinct * from edw_stage.texts where field = 'mat_grp' and source_sys = 'E03'
	)
	select 
	'E03' as  SOURCE_SYS,
	MARA.MATNR as  MATERIAL_CONS,
	MAKT.MAKTG as  MATERIALTEXT_CONS,
	MARA.ATTYP as  MATERIALCATEG_CONS,
	MARA.MATKL as  MATERIALGRP_CONS,
	MARA.MTART as  MATERIALTP_CONS,
	MARA.BISMT as  OLDMATERIAL_CONS,
	MARA.BWSCL as  SOURCESUPPLY_CONS,
	MARA.EAN11 as  EAN_CONS,
	MARA.ERSDA as  CREATEDON_CONS,
	MARA.MEINS as  BASEUNIT_CONS,
	MARA.PRDHA as  PRODHIER_CONS,
	SUBSTRING(MARA.PRDHA,1,1) as PRODHL1_E03,
	T179T_1.VTEXT as  PRODHL1TEXT_E03,
	SUBSTRING(MARA.PRDHA,1,3) as PRODHL2_E03,
	T179T_2.VTEXT as  PRODHL2TEXT_E03,
	SUBSTRING(MARA.PRDHA,1,7) as PRODHL3_E03,
	T179T_3.VTEXT as  PRODHL3TEXT_E03,
	SUBSTRING(MARA.PRDHA,1,11) as PRODHL4_E03,
	T179T_4.VTEXT as  PRODHL4TEXT_E03,
	SUBSTRING(MARA.PRDHA,1,15) as PRODHL5_E03,
	T179T_5.VTEXT as  PRODHL5TEXT_E03,
	SUBSTRING(MARA.PRDHA,1,18) as PRODHL6_E03,
	T179T_6.VTEXT as  PRODHL6TEXT_E03,
	NULL as  PRODHL1_C11,
	NULL as  PRODHL1TEXT_C11,
	NULL as  PRODHL2_C11,
	NULL as  PRODHL2TEXT_C11,
	NULL as  PRODHL3_C11,
	NULL as  PRODHL3TEXT_C11,
	MARA.SPART as  BRAND_CONS,
	MARA.WRKST as  BASICMATERIAL_CONS,
	SUBSTRING(MARA.WRKST,1,3) as  GPPSBU_CONS,
	ZGPP_SBUT.ZSBUDESC as  GPPSBUTEXT_CONS,
	SUBSTRING(MARA.WRKST,5,2) as  GPPDIV_CONS,
	ZGPP_DIVT.ZSPARTDESC as  GPPDIVTEXT_CONS,
	SUBSTRING(MARA.WRKST,8,3) as  GPPCAT_CONS,
	ZGPP_CATT.zcatdesc as  GPPCATTEXT_CONS,
	SUBSTRING(MARA.WRKST,12,5) as  GPPPOR_CONS,
	ZGPP_PORTT.zportdesc as  GPPPORTEXT_CONS,
	txt_mat_grp.code_txt as MATERIALGRP_CONS_TXT,
	MARA.LOADDTS as LOADDTS,
	v_job_name as  ETL_CRTE_USER,
	current_timestamp as  ETL_CRTE_TS ,
	null as  ETL_UPDT_USER,
	cast(null as timestamp) as  ETL_UPDT_TS
	from edw.sape03.mara_current MARA
	join edw.sape03.makt_current MAKT
	on (MARA.MATNR = MAKT.MATNR AND MAKT.SPRAS='E' )
	left join edw.sape03.T179T_current T179T_1
	on (   T179T_1.SPRAS = 'E' and T179T_1.prodh = SUBSTRING(MARA.PRDHA,1,1))
	left join edw.sape03.T179T_current T179T_2
	on ( T179T_2.SPRAS = 'E' and T179T_2.prodh = SUBSTRING(MARA.PRDHA,1,3))
	left join edw.sape03.T179T_current T179T_3
	on ( T179T_3.SPRAS = 'E' and T179T_3.prodh = SUBSTRING(MARA.PRDHA,1,7))
	left join edw.sape03.T179T_current T179T_4
	on ( T179T_4.SPRAS = 'E' and T179T_4.prodh = SUBSTRING(MARA.PRDHA,1,11))
	left join edw.sape03.T179T_current T179T_5
	on ( T179T_5.SPRAS = 'E' and T179T_5.prodh = SUBSTRING(MARA.PRDHA,1,15))
	left join edw.sape03.T179T_current T179T_6
	on ( T179T_6.SPRAS = 'E' and T179T_6.prodh = SUBSTRING(MARA.PRDHA,1,18))
	left join sape03.zgppsbut_current ZGPP_SBUT
	on ( ZGPP_SBUT.SPRAS = 'E' and ZGPP_SBUT.zsbu = SUBSTRING(MARA.WRKST,1,3))
	left join sape03.zgppdivt_current ZGPP_DIVT
	on ( ZGPP_DIVT.SPRAS = 'E' and ZGPP_DIVT.zspart = SUBSTRING(MARA.WRKST,5,2))
	left join sape03.zgppcatt_current ZGPP_CATT
	on ( ZGPP_CATT.SPRAS = 'E' and ZGPP_CATT.zcategory = SUBSTRING(MARA.WRKST,8,3))
	left join sape03.zgppportt_current ZGPP_PORTT
	on ( ZGPP_PORTT.SPRAS = 'E' and ZGPP_PORTT.zport = SUBSTRING(MARA.WRKST,12,5))
    LEFT JOIN txt_mat_grp ON txt_mat_grp.code= MARA.MATKL
    where MARA.loaddts>v_last_extract_timestamp;
    RAISE NOTICE 'TEMPORARY TABLE CREATED CONSOLIDATED_MATERIAL_STG';
    
    ----INSERTS & UPDATES----
	delete from EDW.EDW_CONSOLIDATED.CONSOLIDATED_MATERIAL
	using CONSOLIDATED_MATERIAL_STG_SAPE03 material_stg
	where material_stg.MATERIAL_CONS=CONSOLIDATED_MATERIAL.MATERIAL_CONS
	and material_stg.source_sys=CONSOLIDATED_MATERIAL.source_sys;
	GET DIAGNOSTICS v_delete_count:= ROW_COUNT;
    RAISE NOTICE 'DELETED RECORDS FROM % % ', v_source_system, v_table_name;

    insert into EDW.EDW_CONSOLIDATED.CONSOLIDATED_MATERIAL
	select * from CONSOLIDATED_MATERIAL_STG_SAPE03;
	GET DIAGNOSTICS v_insert_count:= ROW_COUNT;
    v_endtimestamp:=(select current_timestamp);
    RAISE NOTICE 'INSERTED RECORDS INTO % %; RECORD COUNT:%', v_source_system, v_table_name, v_insert_count;
	v_exit_code=0;		

	update edw.edw_stage.job_master
	set job_state=case when v_exit_code=0 then 'SUCCESS' ELSE 'FAILED' end,
	last_extract_timestamp=(select max(loaddts) from EDW.EDW_CONSOLIDATED.CONSOLIDATED_MATERIAL where source_sys=v_source_system),
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