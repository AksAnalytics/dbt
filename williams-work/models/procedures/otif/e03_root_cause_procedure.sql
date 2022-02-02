CREATE OR REPLACE PROCEDURE otif.root_cause_e03_sp(p_process_id int4)
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
    v_insertcount bigint;
    v_integer_var bigint;
    v_totalcount bigint;
    v_deletecount bigint;
    audit_rec RECORD;
	v_last_extract_timestamp timestamp;
	
begin
	v_job_id=18;
	SELECT INTO audit_rec * FROM EDW.EDW_STAGE.job_master WHERE job_id = v_job_id;
	v_start_timestamp:=(select current_timestamp);
	v_source_system=audit_rec.source_sys;
	v_job_name=audit_rec.job_name;
    v_table_name=audit_rec.table_name;
    v_runid=p_process_id;
	v_last_extract_timestamp=audit_rec.last_extract_timestamp;
	v_exit_code=-1;
    
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
	,1 run_seq
	,'STARTED' as job_status
	--, null as message
	,'ETL_USER' as etl_crte_user
	,current_timestamp etl_crte_ts
	,'ETL_USER' as etl_updt_user
	,null::timestamp etl_updt_ts;
	
    --COMMIT;
    RAISE NOTICE 'RECORD INSERTED INTO JOB_HISTORY';
   
	TRUNCATE TABLE otif.e03_root_cause; 
    Insert into otif.e03_root_cause
	
    select *
	from
        (
        select
        RL2.source_sys,
        RL2.salesordnum_cons,
        RL2.salesorditem_cons,
        RL2.delivnum_cons,
        RL2.delivitem_cons,
        case when RL2.root_code in ('Delivery Block','RDD Unrealistic') then RL2.root_code::varchar
        else NULL	
        end root_code_l2,
        case when RL2.root_code in ('OTIF','DC Delay','Credit Issue','Other Issue','Product Availability') then RL2.root_code::varchar
        else 'SOM'::varchar
        end root_code_l1,
        'Y' as atcv_flag,
        current_timestamp etl_crte_ts,
        null::timestamp etl_updt_ts
        from
            (select 
            source_sys,
            salesordnum_cons,
            salesorditem_cons,
            delivnum_cons,
            delivitem_cons,
            --MODIFY 
            ord_stat_crdt,
            ORD_DLV_BLCK_SSK,
            ord_req_dl_dte,
            ovdl_est_act_dlv_dte,
            --additional
            gidate_cons,
            actgidate_cons,
            otif_qty,
            case 
            when otif_pct=100 then 'OTIF'::varchar
            when actgidate_cons::int > gidate_cons::int  then 'DC Delay'::varchar
            when ord_stat_crdt in ('B','C') then 'Credit Issue'::varchar	
            when ord_stat_crdt not in ('B','C') and ORD_DLV_BLCK_SSK <> '' 
            and ord_req_dl_dte::int <= 
            ovdl_est_act_dlv_dte::int then 'Delivery Block'::varchar	
            when ord_stat_crdt not in ('B','C') and ORD_DLV_BLCK_SSK='' 
            and ord_req_dl_dte::int 
            <= ovdl_est_act_dlv_dte::int then 'RDD Unrealistic'::varchar
            when delivqty_cons < otif_qty then 'Other Issue'::varchar
            when delivqty_cons <> odlv_orig_qty and ( actgidate_cons is NULL or actgidate_cons = '' ) then 'Product Availability'::varchar
            else 'Product Availability':: varchar
            end as root_code 
            from 
                (	
                SELECT 
                co.source_sys,
                co.salesordnum_cons,
                co.salesorditem_cons,
                cd.delivnum_cons,
                cd.delivitem_cons,
                co.ord_stat_crdt,
                co.ord_req_dl_dte,
                co.ORD_DLV_BLCK_SSK,
                cd.delivqty_cons,
                --MODIFY 
                cd.ovdl_est_act_dlv_dte,
                cd.actgidate_cons,
                cd.odlv_orig_qty,
                co.orderqty_cons,
                --additional
                cd.gidate_cons,
                --cd.actgidate_cons,
                CASE
                        WHEN cd.odlv_otif_dte::text > co.svclvldate_cons::text OR co.svclvldate_cons::text < to_char(to_date('now'::character varying::date::character varying::text, 'YYYY-MM-DD HH24:MI:SS'::character varying::text)::timestamp without time zone, 'YYYYMMDD'::character varying::text) AND (cd.delivnum_cons IS NULL OR cd.odlv_otif_dte IS NULL) THEN 0::numeric::numeric(18,0)
                        WHEN co.svclvldate_cons::text > to_char(to_date('now'::character varying::date::character varying::text, 'YYYY-MM-DD HH24:MI:SS'::character varying::text)::timestamp without time zone, 'YYYYMMDD'::character varying::text) AND (cd.delivnum_cons IS NULL OR cd.odlv_otif_dte IS NULL) THEN NULL::numeric::numeric(18,0)
                        ELSE cd.delivqty_cons
                    END AS otif_qty,
                CASE
                        WHEN (co.reject_reason_cd IN ( SELECT "of".value
                        FROM edw_stage.otif_filter "of"
                        WHERE ("of".source_sys IN ( SELECT DISTINCT consolidated_orders.source_sys
                                FROM edw_consolidated.consolidated_orders)) AND "of".field::text = 'rej_code'::character varying::text)) THEN 
                        CASE
                            WHEN (co.orderqty_cons - co.rejectqty_cons) = 0::numeric::numeric(18,0) THEN 0::numeric::numeric(18,0)::numeric(38,10)
                            ELSE 
                            CASE
                                WHEN cd.odlv_otif_dte::text > co.svclvldate_cons::text OR co.svclvldate_cons::text < to_char(to_date('now'::character varying::date::character varying::text, 'YYYY-MM-DD HH24:MI:SS'::character varying::text)::timestamp without time zone, 'YYYYMMDD'::character varying::text) AND (cd.delivnum_cons IS NULL OR cd.odlv_otif_dte IS NULL) THEN 0::numeric::numeric(18,0)
                                WHEN co.svclvldate_cons::text > to_char(to_date('now'::character varying::date::character varying::text, 'YYYY-MM-DD HH24:MI:SS'::character varying::text)::timestamp without time zone, 'YYYYMMDD'::character varying::text) AND (cd.delivnum_cons IS NULL OR cd.odlv_otif_dte IS NULL) THEN NULL::numeric::numeric(18,0)
                                ELSE cd.delivqty_cons
                            END::numeric(38,10) / (co.orderqty_cons - co.rejectqty_cons) * 100::numeric::numeric(18,0)
                        END
                        ELSE 
                        CASE
                            WHEN co.orderqty_cons = 0::numeric::numeric(18,0)::numeric(38,10) THEN 0::numeric::numeric(18,0)::numeric(38,10)
                            ELSE 
                            CASE
                                WHEN cd .odlv_otif_dte::text > co.svclvldate_cons::text OR co.svclvldate_cons::text < to_char(to_date('now'::character varying::date::character varying::text, 'YYYY-MM-DD HH24:MI:SS'::character varying::text)::timestamp without time zone, 'YYYYMMDD'::character varying::text) AND (cd.delivnum_cons IS NULL OR cd.odlv_otif_dte IS NULL) THEN 0::numeric::numeric(18,0)
                                WHEN co.svclvldate_cons::text > to_char(to_date('now'::character varying::date::character varying::text, 'YYYY-MM-DD HH24:MI:SS'::character varying::text)::timestamp without time zone, 'YYYYMMDD'::character varying::text) AND (cd.delivnum_cons IS NULL OR cd.odlv_otif_dte IS NULL) THEN NULL::numeric::numeric(18,0)
                                ELSE cd.delivqty_cons
                            END::numeric(38,10) / co.orderqty_cons * 100::numeric::numeric(18,0)
                        END
                    END AS otif_pct		
                    from edw.edw_consolidated.consolidated_orders co
                left outer join edw.edw_consolidated.consolidated_deliveries cd
                on co.source_sys = cd.source_sys and 
                co.salesordnum_cons = cd.refdoc_cons and 
                co.salesorditem_cons = cd.refitem_cons
                where co.source_sys='E03'
                )
        ) RL2
    );
   


   
	v_endtimestamp:=(select current_timestamp);
    GET DIAGNOSTICS v_integer_var:= ROW_COUNT;
	RAISE NOTICE 'INSERTED RECORDS INTO % %; RECORD COUNT:%', v_source_system, v_table_name, v_integer_var;
	v_exit_code=0;	

	update edw.edw_stage.job_master
	set job_state=case when v_exit_code=0 then 'SUCCESS' ELSE 'FAILED' end,
	last_extract_timestamp=current_timestamp,
	etl_updt_ts=current_timestamp
	where job_id=v_job_id;
	RAISE NOTICE 'RECORD UPDATED IN JOB_MASTER';

    update EDW.EDW_STAGE.job_history
	set job_status=case when v_exit_code=0 then 'SUCCESS' ELSE 'FAILED' end,
	end_timestamp=v_endtimestamp,
	etl_updt_ts=current_timestamp,
	insert_count=v_integer_var,
	update_count=0,
	delete_count=0
	--total_count=v_integer_var
	where run_id=v_runid;
    RAISE NOTICE 'RECORD UPDATED IN JOB_HISTORY';
    RAISE NOTICE 'INCREMENTAL LOAD FOR % % COMPLETED SUCCESSFULLY', v_source_system, v_table_name;
	
	-----EXCEPTION HANDLING------
	EXCEPTION
       WHEN others THEN
       	  RAISE EXCEPTION 'GOT EXCEPTION:SQLSTATE: % SQLERRM: %', SQLSTATE, SQLERRM;          
end;



$$
;