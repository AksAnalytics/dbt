
CREATE OR REPLACE PROCEDURE stage.p_build_source_core_tran_delta_hfm(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN  
raise info 'delete hfm core bods transactions for fiscal month : % data from table',fmthid ;	
delete from stage.core_tran_delta where fiscal_month_id = fmthid AND audit_rec_src='hfm';

    
    /* used to dedup source tran table */
    drop table if exists stg_max_loaddts
    ;
    create temporary table stg_max_loaddts
    diststyle all
    as 
        select  s.bar_year, s.bar_period, max(s.loaddatetime) as max_loaddatetime
        from    {{ source('bods', 'hfm_vw_hfm_actual_trans') }} as s
        group by s.bar_year, s.bar_period 
    ;
raise info 'Insert one month of hfm core bods transactions : % data into stage.core_tran_delta',fmthid;
insert into stage.core_tran_delta 
( audit_rec_src,
  document_no,
  document_line,
  bar_year,
  bar_period,
  bar_entity,
  bar_acct,
  shiptocust,
  soldtocust,
  material,
  bar_custno,
  bar_product,
  bar_brand,
  bar_currtype,
  postdate,
  posting_week_enddate,
  fiscal_month_id,
  bar_amt,
  quantity,
  quanunit,
  dataprocessing_hash,
  audit_loadts)
Select 'hfm' AS audit_rec_src,
	  '-1' AS document_no,
	  '-1' AS document_line,
	  s.bar_year,
	  cast(fiscal_month_id as varchar(10)) as bar_period,
	  bar_entity,
	  s.bar_acct,
	  null as shiptocust,
	  null as soldtocust,
	  null as material,
	  s.bar_custno, 
	  s.bar_product,
	  s.bar_brand,
	  s.bar_currtype,
	  fiscal_month_end_date as postdate, 
	  fiscal_month_end_date as posting_week_enddate,
	  fiscal_month_id,
	  cast(bar_amt as numeric(19,8)) as bar_amt,
	  -1 as quantity,
	  'unknown' as quanunit,
	  md5(cast('hfm' as varchar(10))) as dataprocessing_hash,
	  cast(getdate() as timestamp) as audit_loadts
from {{ source('bods', 'hfm_vw_hfm_actual_trans') }} s 
                inner join stg_max_loaddts as cur 
                    on  cur.bar_year = s.bar_year and 
                        cur.bar_period = s.bar_period and 
                        cur.max_loaddatetime = s.loaddatetime
inner join (select distinct bar_acct from ref_data.pnl_acct) acct on lower(s.bar_acct) = lower(acct.bar_acct) 
inner join ref_data.entity rbh on s.bar_entity = rbh.name
inner join (select fyr_id, lower(SUBSTRING(fmth_name,1,3)) as bar_period,
			   fmth_id as fiscal_month_id,
			   min(cast(fmth_begin_dte as date)) as fiscal_month_begin_date,
			   min(cast(fmth_end_dte as date)) as fiscal_month_end_date
		  from ref_data.calendar 
		  group by fyr_id, lower(SUBSTRING(fmth_name,1,3)), fmth_id
		  ) c on cast(s."year" as integer) = c.fyr_id 
		and lower(s."period") = c.bar_period
where rectype = 'Actual'
and s.bar_bu = 'GTS'
and rbh.level4 = 'GTS_NA'
and fiscal_month_id=fmthid;

exception
when others then raise exception 'exception occur while ingesting data for fiscal month : % in stage.core_tran_delta for hfm', fmthid;
end;
$$
;