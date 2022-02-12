
CREATE OR REPLACE PROCEDURE stage.p_build_source_core_tran_delta_cleansed(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN  
raise info 'delete fiscal month % data from table if exists', fmthid;	
delete from stage.core_tran_delta_cleansed where fiscal_month_id = fmthid; 
---and audit_rec_src='sap_c11';

raise info 'Insert one month : % data into core_tran_delta', fmthid ;
INSERT INTO stage.core_tran_delta_cleansed
(
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
  org_bar_custno,
  mapped_bar_custno,
  org_bar_product,
  mapped_bar_product,
  org_bar_brand,
  mapped_bar_brand,
  bar_currtype,
  postdate,
  posting_week_enddate,
  fiscal_month_id,
  bar_amt,
  quantity,
  quanunit,
  org_dataprocessing_hash,
  mapped_dataprocessing_hash,
  audit_loadts
)
Select t.audit_rec_src,
	  t.document_no,
	  t.document_line,
	  t.bar_year,
	  t.bar_period,
	  t.bar_entity,
	  t.bar_acct,
	  t.shiptocust,
	  t.soldtocust,
	  t.material,
	  t.org_bar_custno,
	  t.mapped_bar_custno,
	  t.org_bar_product,
	  t.mapped_bar_product,
	  t.org_bar_brand,
	  t.mapped_bar_brand,
	  t.bar_currtype,
	  t.postdate,
	  t.posting_week_enddate,
	  t.fiscal_month_id,
	  t.bar_amt,
	  t.quantity,
	  t.quanunit,
	  t.org_dataprocessing_hash,
	  md5(concat(concat(concat(t.SoldToFlag,mapped_barcustflag),materialflag),mapped_barproductFlag)) as dataprocessing_hash,
	  cast(getdate() as timestamp) as audit_loadts
from (
	SELECT audit_rec_src,
	       document_no,
	       document_line,
	       bar_year,
	       bar_period,
	       bar_entity,
	       ctd.bar_acct,
	       shiptocust,
	       ctd.soldtocust,
	       ctd.material,
	       ctd.bar_custno as org_bar_custno,
	       case when dprp.dataprocessing_group = 'cleansed - data : bar_custno' and ctd.audit_rec_src='sap_c11' then sbm.bar_custno else ctd.bar_custno end as  mapped_bar_custno,
	       ctd.bar_product as org_bar_product,
	       case when dprp.dataprocessing_group = 'cleansed - data : bar_product' and ctd.audit_rec_src='sap_c11' then sbmp.bar_product else ctd.bar_product end as  mapped_bar_product,
	       ctd.bar_brand as org_bar_brand,
	       case when dprp.dataprocessing_group = 'cleansed - data : bar_brand' and ctd.audit_rec_src='sap_c11' then  sbbp.bar_brand
	       	  when lower(ctd.bar_brand) = 'brand_none' and ctd.audit_rec_src in ('sap_lawson', 'sap_p10','sap_c11') 
	       	  then sbbp.bar_brand
	       	  else ctd.bar_brand end as mapped_bar_brand,
	       bar_currtype,
	       ctd.postdate,
	       posting_week_enddate,
	       fiscal_month_id,
	       ---for hfm - negate bar_amt for cost accounts and keep it same for sales accounts
	       case when dprp.dataprocessing_group ='cleansed - data : hfm sales & cost acct' 
	       		  and ctd.audit_rec_src= 'hfm'
	       		  and pnl_acct.acct_type ='cost'
	       	  then bar_amt*-1  
	       	  else bar_amt 
	       end as bar_amt,
	       quantity,
	       quanunit,
	       ctd.dataprocessing_hash as org_dataprocessing_hash,
	       case when ctd.soldtocust is not null then '1' else '0' end as soldtoflag,
	       case when  ctd.material is not null then '1' else '0' end as materialflag,
		  case when mapped_bar_custno is not null then '1' else '0' end as mapped_barcustflag,
		  case when mapped_bar_product is not null then '1' else '0' end as mapped_barproductFlag
FROM stage.core_tran_delta ctd
	left join ref_data.data_processing_rule dprp on ctd.dataprocessing_hash = dprp.dataprocessing_hash --and dprp.dataprocessing_group like 'cleansed%'
	LEFT join ref_data.soldto_barcust_mapping sbm on ctd.soldtocust = sbm.soldtocust and sbm.current_flag =1 
	LEFT join ref_data.sku_barproduct_mapping sbmp on ctd.material = sbmp.material and sbmp.current_flag =1 
	left join ref_data.sku_barbrand_mapping sbbp on ctd.material = sbbp.material and sbbp.current_flag =1 
	left join ref_data.pnl_acct pnl_acct on ctd.bar_acct = pnl_acct.bar_acct  
where fiscal_month_id = fmthid
) t;

exception
when others then raise exception 'exception occur while ingesting : % data in core_tran_delta_cleansed', fmthid;
end;
$$
;