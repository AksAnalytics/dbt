
CREATE OR REPLACE PROCEDURE stage.p_build_source_core_tran_delta_c11_stdcost(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN  
raise info 'delete c11 core bods transactions for fiscal month : % data from table',fmthid ;	
delete from stage.core_tran_delta where fiscal_month_id = fmthid and audit_rec_src='ext_c11std';

raise info 'Insert one month of c11 core bods transactions : % data into stage.core_tran_delta',fmthid;
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
with dates as (	
	Select distinct fmth_begin_dte, fmth_end_dte,fmth_cd,fmth_id
	from ref_data.calendar c 
	where fmth_id = fmthid
)	
Select audit_rec_src,
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
	  case when SoldToFlag in  (1) and barcustflag = 0 and materialflag in (1,0) and barproductFlag in (1,0) then '2-' || md5(concat(SoldToFlag,barcustflag)) 
			when materialflag =1 and barproductflag = 0 and soldtoflag in (1,0) and barcustflag in (1,0) then '3-' || md5(concat(materialflag,barproductflag))
			when materialflag = 1 and barbrandflag = 0 and soldtoflag in (1,0) and barcustflag in (1,0)  then '4-' || md5(concat(materialflag,barbrandflag))
		else md5(concat(concat(concat(SoldToFlag,barcustflag),materialflag),barproductFlag)) end as dataprocessing_hash
      ,cast(getdate() as timestamp) as audit_loadts
From (
select	    DISTINCT
            s.refdocline,
            s.refdocnr,
            
            cast('ext_c11std' as varchar(10)) as audit_rec_src ,
			cast(docno as varchar(10)) as document_no, 
			cast(docline as varchar(3)) as document_line,
			cast(s.year as varchar(4)) as bar_year ,
			cast(s.period as varchar(10)) as bar_period ,
			cast(s.bar_entity as varchar(5)) as bar_entity,
			cast(s.bar_acct as varchar(6)) as bar_acct,
			case when cast(shiptocust as varchar(10)) = ''  then null else cast(shiptocust as varchar(10)) end as shiptocust, 
			case when cast(soldtocust as varchar(10)) = '' then null else cast(soldtocust as varchar(10)) end  as soldtocust,
			case when cast(material as varchar(30)) = ''  then null else cast(material as varchar(30)) end as material,
			case when cast(s.bar_custno as varchar(20)) = '' then null else cast(s.bar_custno as varchar(20)) end as bar_custno,
			case when cast(s.bar_product  as varchar(22)) = '' then null else cast(s.bar_product  as varchar(22)) end as bar_product,
			case when cast(bar_brand as varchar(14))  = ''  then null else cast(bar_brand as varchar(14)) end as bar_brand,
			case when (cast(shiptocust as varchar(10)) = '' or  cast(shiptocust as varchar(10)) is null) then '0' else '1' end as shiptocustflag, 
			case when (cast(soldtocust as varchar(10)) = '' or cast(soldtocust as varchar(10)) is null)  then'0' else '1' end  as soldtoflag,
			case when (cast(material as varchar(30)) = '' or cast(material as varchar(30)) is null) then '0' else '1' end as materialflag,
			case when (cast(s.bar_custno as varchar(20)) = '' or cast(s.bar_custno as varchar(20)) is null)  then '0' else '1' end as barcustflag,
			case when (cast(s.bar_product  as varchar(22)) = '' or cast(s.bar_product  as varchar(22)) is null) then '0' else '1' end as barproductflag,
			case when (cast(bar_brand as varchar(14))  = '' or cast(bar_brand as varchar(14)) is null) then '0' else '1' end as barbrandflag,
			bar_currtype,
			cast(s.postdate as date) as postdate,
			cast(dd.wk_end_dte as date) as posting_week_enddate,
			cast(dd.fmth_id as integer) as fiscal_month_id,
			cast(bar_amt as numeric(38, 8))bar_amt,
			cast(quantity as decimal(38, 8)) as quantity,
			case when cast(s.quanunit as varchar(10)) = '' then null else cast(s.quanunit as varchar(10)) end as quanunit
	from {{ source('bods', 'c11_0ec_pca3') }} s
		left join ref_data.calendar dd on
			cast((case when s.postdate = '' then null else postdate end) as date) = cast(dd.dy_dte as date)
		inner join (select distinct bar_acct from ref_data.pnl_acct) acct on lower(s.bar_acct) = lower(acct.bar_acct) 
		left join {{ source('sapc11', 'kna1') }} kc 
				on lower(case when cast(shiptocust as varchar(10)) = ''  then null else cast(shiptocust as varchar(10)) end) = lower(kc.kunnr) 
		cross join dates d
		WHERE		
			s.bar_acct = 'A60111'
			and cast((case when s.postdate = '' then null else postdate end) as date)  
			--between cast('2020-06-28' as date) and cast('2020-08-01' as date)
			between d.fmth_begin_dte and d.fmth_end_dte 
			and (bar_bu='GTS' or bar_bu is null)
			--and kc.land1 in ('CA','US')
) t;

exception
when others then raise exception 'exception occur while ingesting data for fiscal month : % in stage.core_tran_delta for acct : 60111 - acct exception', fmthid;
end;
$$
;