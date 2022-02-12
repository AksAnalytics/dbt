
CREATE OR REPLACE PROCEDURE stage.p_build_source_core_tran_delta_lawson_agm(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN  
	
	delete from stage.core_tran_delta_agm where fiscal_month_id = fmthid and audit_rec_src = 'sap_lawson';
	
	
	insert into stage.core_tran_delta_agm (audit_rec_src, document_no, document_line, bar_year, bar_period, bar_entity, bar_acct, shiptocust, soldtocust, material, 
	bar_custno, bar_product, bar_brand, bar_currtype, postdate, posting_week_enddate, fiscal_month_id, 
	bar_amt, quantity, quanunit, dataprocessing_hash, audit_loadts)
	Select
	  audit_rec_src,
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
	from 
	(
	Select
	 		cast('sap_lawson' as varchar(10)) as audit_rec_src ,
			cast(a.post_doc_ref_nbr as varchar(10)) as document_no, 
			cast(a.post_doc_ref_ln_nbr as varchar(3)) as document_line,
			cast(a.bar_year as varchar(4)) as bar_year ,
			cast(a.bar_period as varchar(10)) as bar_period ,
			cast(a.bar_entity as varchar(5)) as bar_entity,
			cast(a.bar_acct as varchar(10)) as bar_acct,
			null as shiptocust,
			case when cast(a.cust_nbr as varchar(10)) = '' then null else cast(a.cust_nbr as varchar(10)) end  as soldtocust,
			case when cast(prod_cd as varchar(30)) = '' then null else cast(prod_cd as varchar(30)) end as material,
			case when cast(a.bar_custno as varchar(20)) = '' then null else cast(a.bar_custno as varchar(20)) end as bar_custno,
			case when cast(a.bar_product  as varchar(22)) = '' then null else cast(a.bar_product  as varchar(22)) end as bar_product,
			case when cast(bar_brand as varchar(14))  = '' then null else cast(bar_brand as varchar(14)) end as bar_brand,
			'0' as shiptocustflag, 
			case when (cast(cust_nbr as varchar(10)) = '' or cast(cust_nbr as varchar(10)) is null)  then'0' else '1' end  as soldtoflag,
			case when (cast(prod_cd as varchar(30)) = '' or cast(prod_cd as varchar(30)) is null) then '0' else '1' end as materialflag,
			case when (cast(a.bar_custno as varchar(20)) = '' or cast(a.bar_custno as varchar(20)) is null)  then '0' else '1' end as barcustflag,
			case when (cast(a.bar_product  as varchar(22)) = '' or cast(a.bar_product  as varchar(22)) is null) then '0' else '1' end as barproductflag,
			case when (cast(a.bar_brand as varchar(14))  = '' or cast(a.bar_brand as varchar(14)) is null) then '0' else '1' end as barbrandflag,
			bar_currtype,
			cast(a.post_dte as date) as postdate,
			cast(dd.wk_end_dte as date) as posting_week_enddate,
			cast(dd.fmth_id as integer) as fiscal_month_id,
			cast(bar_amt as numeric(38, 8)) as  bar_amt,
			cast(quantity as decimal(38, 8)) as quantity,
			null as quanunit
		from
			{{ source('bods', 'lawson_mac_pl_trans') }} a
			left join ref_data.calendar dd on cast((case when a.post_dte = '' then null else a.post_dte end) as date) = cast(dd.dy_dte as date)
			inner join ref_data.entity rbh on a.bar_entity = rbh.name
			---only accounts thats contributes to agm pnl structure
			inner join  ref_data.pnl_acct_agm acct on lower(a.bar_acct) = lower(acct.bar_acct) 
			
			cross join (Select distinct fmth_begin_dte, fmth_end_dte,fmth_cd,fmth_id
						from ref_data.calendar c 
						where fmth_id = fmthid) dates
	
		WHERE 
			a.bar_acct is not null 
			and a.bar_entity is not null 
			and rbh.level4 = 'GTS_NA'
			and cast((case when a.post_dte = '' then null else a.post_dte end) as date) between dates.fmth_begin_dte and dates.fmth_end_dte 
	) r;
	
END
$$
;