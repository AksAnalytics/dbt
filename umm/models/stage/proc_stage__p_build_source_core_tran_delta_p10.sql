
CREATE OR REPLACE PROCEDURE stage.p_build_source_core_tran_delta_p10(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN  
	
	
	delete from stage.core_tran_delta where fiscal_month_id = fmthid and audit_rec_src = 'sap_p10';
	
	insert into stage.core_tran_delta (audit_rec_src, document_no, document_line, bar_year, bar_period, bar_entity, bar_acct, shiptocust, soldtocust, material, 
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
	 		cast('sap_p10' as varchar(10)) as audit_rec_src ,
			cast(docnr as varchar(10)) as document_no, 
			cast(docln as varchar(3)) as document_line,
			cast(a.bar_year as varchar(4)) as bar_year ,
			cast(a.bar_period as varchar(10)) as bar_period ,
			cast(a.bar_entity as varchar(5)) as bar_entity,
			cast(a.bar_acct as varchar(6)) as bar_acct,
			case when cast(shipto_cust_nbr as varchar(10)) = '' then null else cast(shipto_cust_nbr as varchar(10)) end as shiptocust,
			---for 2019, Storage_other custno : p10 has data issue --> map all shipto to soldto for this pattern
			case when dd.fyr_id = 2019 
				and lower(case when cast(a.bar_custno as varchar(20)) = '' then null else cast(a.bar_custno as varchar(20)) end) = 'storage_oth' 
				and case when cast(cust_no as varchar(10)) = '' then null else cast(cust_no as varchar(10)) end is null 
				then case when cast(shipto_cust_nbr as varchar(10)) = '' then null else cast(shipto_cust_nbr as varchar(10)) end
				else case when cast(cust_no as varchar(10)) = '' then null else cast(cust_no as varchar(10)) end
			end as soldtocust,
			---case when cast(cust_no as varchar(10)) = '' then null else cast(cust_no as varchar(10)) end  as soldtocust,
			case when cast(prod_cd as varchar(30)) = '' then null else cast(prod_cd as varchar(30)) end as material,
			case when cast(a.bar_custno as varchar(20)) = '' then null else cast(a.bar_custno as varchar(20)) end as bar_custno,
			case when cast(a.bar_product  as varchar(22)) = '' then null else cast(a.bar_product  as varchar(22)) end as bar_product,
			case when cast(bar_brand as varchar(14))  = '' then null else cast(bar_brand as varchar(14)) end as bar_brand,
			case when (cast(shipto_cust_nbr as varchar(10)) = '' or  cast(shipto_cust_nbr as varchar(10)) is null) then '0' else '1' end as shiptocustflag, 
			case when (cast(cust_no as varchar(10)) = '' or cast(cust_no as varchar(10)) is null)  then'0' else '1' end  as soldtoflag,
			case when (cast(prod_cd as varchar(30)) = '' or cast(prod_cd as varchar(30)) is null) then '0' else '1' end as materialflag,
			case when (cast(a.bar_custno as varchar(20)) = '' or cast(a.bar_custno as varchar(20)) is null)  then '0' else '1' end as barcustflag,
			case when (cast(a.bar_product  as varchar(22)) = '' or cast(a.bar_product  as varchar(22)) is null) then '0' else '1' end as barproductflag,
			case when (cast(a.bar_brand as varchar(14))  = '' or cast(a.bar_brand as varchar(14)) is null) then '0' else '1' end as barbrandflag,
			bar_currtype,
			cast(a.cpudt as date) as postdate,
			cast(dd.wk_end_dte as date) as posting_week_enddate,
			cast(dd.fmth_id as integer) as fiscal_month_id,
			cast(bar_amt as numeric(38, 8))bar_amt,
			cast(quantity as decimal(38, 8)) as quantity,
			case when cast(a.quanunit as varchar(10)) = '' then null else cast(a.quanunit as varchar(10)) end as quanunit
		from {{ source('bods', 'p10_0ec_pca_3_trans') }} a
			left join ref_data.calendar dd on cast((case when a.cpudt = '' then null else a.cpudt end) as date) = cast(dd.dy_dte as date)
			inner join ref_data.entity rbh on a.bar_entity = rbh.name
			---only accounts thats contributes to sgm pnl structure
			inner join (select distinct bar_acct from ref_data.pnl_acct) acct on lower(a.bar_acct) = lower(acct.bar_acct) 
			
			cross join (Select distinct fmth_begin_dte, fmth_end_dte,fmth_cd,fyr_id
						from ref_data.calendar c 
						where fmth_id = fmthid) dates
	
		WHERE 
			a.bar_acct is not null 
			and a.bar_entity is not null 
			and rbh.level4 = 'GTS_NA'
			--and a.bar_currtype in ('USD' ,'CAD')
			and cast((case when a.cpudt = '' then null else a.cpudt end) as date) between dates.fmth_begin_dte and dates.fmth_end_dte 
			--and a.cpudt <> '2021'  --this was temporary band aid to fix 1 row of bad data
	) r;
	
END
$$
;