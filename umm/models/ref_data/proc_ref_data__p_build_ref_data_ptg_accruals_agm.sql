
CREATE OR REPLACE PROCEDURE ref_data.p_build_ref_data_ptg_accruals_agm(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 
	
	
delete from ref_data.ptg_accruals where fiscal_month_id = fmthid;
insert into ref_data.ptg_accruals (gl_acct ,amt,amt_usd,currkey,fiscal_month_id,posting_week_enddate,audit_loadts)
Select CAST(acct AS varchar(50)) AS gl_acct, 
	  sum(CAST(amt AS NUMERIC(19,8))) AS amt, 
	  sum(case when hc.from_currtype is null then CAST(amt AS NUMERIC(19,8))
	  		 else cast(fxrate as numeric(19,8))*CAST(amt AS NUMERIC(19,8)) end) as amt_usd, 
	  cast(currkey as varchar(10)) as currkey,
	  fmthid as fiscal_month_id,
	  dd.wk_end_dte as posting_week_enddate,
	  getdate() as audit_loadts
from {{ source('bods', 'c11_0ec_pca3') }} s
left join ref_data.calendar dd on cast((case when s.postdate = '' then null else postdate end) as date) = cast(dd.dy_dte as date)
left join {{ source('ref_data', 'hfmfxrates') }} hc on dd.fmth_id = hc.fiscal_month_id and lower(s.currkey) = lower(hc.from_currtype)
where 1=1
and dd.fmth_id = fmthid
and s.costctr in ('1005000000')
and acct in ('0005757004','0005555531')
group by CAST(acct AS varchar(50)) ,cast(currkey as varchar(10)), dd.wk_end_dte;
end
$$
;