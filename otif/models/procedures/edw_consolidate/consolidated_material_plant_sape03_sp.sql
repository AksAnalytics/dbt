CREATE OR REPLACE PROCEDURE edw_consolidated.consolidated_material_plant_sape03_sp()
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
	v_job_id=10;
	SELECT INTO audit_rec * FROM EDW.EDW_STAGE.job_master WHERE job_id = v_job_id;
	v_start_timestamp:=(select current_timestamp);
	v_source_system=audit_rec.source_sys;
	v_job_name=audit_rec.job_name;
    v_table_name=audit_rec.table_name;
    v_runid=(select pg_backend_pid());
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
   
    drop table if exists CONSOLIDATED_MATERIAL_PLANT_STG_SAPE03;
   
	CREATE TEMPORARY TABLE CONSOLIDATED_MATERIAL_PLANT_STG_SAPE03 as
	
	select 'E03' as  SOURCE_SYS,
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
		CASE WHEN   pl.plant_txt is null or  pl.plant_txt  = '' THEN matpl.SOURCESUP_TEXT_CONS ELSE pl.plant_txt END as PROD_PLNT_RPL_LOC_DESC,
		matpl.LOADDTS,
		v_job_name as ETL_CRTE_USER,
		current_timestamp as  ETL_CRTE_TS ,
		null as ETL_UPDT_USER,
		cast(null as timestamp) as  ETL_UPDT_TS
		from ( 
			select 
			MARC.MATNR  as MATERIAL_CONS,
			MARC.WERKS  as MATERIALPLANT_CONS, 
			MAKT.MAKTG  as MATERIALTEXT_CONS,
			MARC.MMSTA  as PLNTMATSTAT_CONS,
			MARC.MAABC  as ABCIND_CONS,
			MARC.EKGRP  as PURCHGRP_CONS,
			MARC.DISPO  as MRPCTRL_CONS,
			MARC.PRCTR  as PROFITCTR_CONS,
			MARC.LOADDTS as LOADDTS,
			uc.u_replen_loc as PROD_PLNT_RPL_LOC,
			uc.u_plant_vendor	as SOURCESUP_CONS,
		    uc.u_plant_vendor_descr as SOURCESUP_TEXT_CONS
			From  sape03.marc_current MARC
			join sape03.makt_current MAKT
			on (MARC.MATNR = MAKT.MATNR and MAKT.SPRAS='E')
			LEFT join ( 
				select * from (
				select u_plant_vendor, u_replen_loc,u_plant_vendor_descr,u_item,u_loc ,u_updated,ROW_NUMBER() OVER (partition by u_item,u_loc order by u_updated desc ) as row_num
				from jda.udtsrcsupply_current 				  )
				where row_num=1
				) uc 
			on(MARC.MATNR = uc.u_item AND MARC.WERKS = uc.u_loc) --where MARC.loaddts>v_last_extract_timestamp
	
		) as matpl left join edw.edw_consolidated.consolidated_plants pl on matpl.PROD_PLNT_RPL_LOC = pl.plant ;
    


    RAISE NOTICE 'TEMPORARY TABLE CREATED CONSOLIDATED_MATERIAL_PLANT_STG';
    
    ----INSERTS & UPDATES----
	delete from EDW.EDW_CONSOLIDATED.CONSOLIDATED_MATERIAL_PLANT
	using CONSOLIDATED_MATERIAL_PLANT_STG_SAPE03 material_plant_stg
	where material_plant_stg.MATERIAL_CONS=CONSOLIDATED_MATERIAL_PLANT.MATERIAL_CONS
	and material_plant_stg.MATERIALPLANT_CONS=CONSOLIDATED_MATERIAL_PLANT.MATERIALPLANT_CONS
	and material_plant_stg.source_sys=CONSOLIDATED_MATERIAL_PLANT.source_sys;
	GET DIAGNOSTICS v_delete_count:= ROW_COUNT;
    RAISE NOTICE 'DELETED RECORDS FROM % % ', v_source_system, v_table_name;

    insert into EDW.EDW_CONSOLIDATED.CONSOLIDATED_MATERIAL_PLANT
	select * from CONSOLIDATED_MATERIAL_PLANT_STG_SAPE03;
	GET DIAGNOSTICS v_insert_count:= ROW_COUNT;
    v_endtimestamp:=(select current_timestamp);
    RAISE NOTICE 'INSERTED RECORDS INTO % %; RECORD COUNT:%', v_source_system, v_table_name, v_insert_count;
	v_exit_code=0;		

	update edw.edw_stage.job_master
	set job_state=case when v_exit_code=0 then 'SUCCESS' ELSE 'FAILED' end,
	last_extract_timestamp=(select max(loaddts) from EDW.EDW_CONSOLIDATED.CONSOLIDATED_MATERIAL_PLANT where source_sys=v_source_system),
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