
CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_21_c11(fmthid integer)
 LANGUAGE plpgsql
AS $_$
--DECALRE Variables here
declare 
current_posting_week date;
calendar_posting_week date;
BEGIN 
	
---step 1 : 1. Keep all current transactions pulled for A40111 (transactions booked to a GTS-NA Entity). Do not apply allocation rule to the account. Sum up $ each month for each customer. Do not use these transactions in UMM
--- : acct exception - fob_invoicesale
	/* create temp table for select ected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				fmth_id as fiscal_month_id
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid 
		group by fmth_id
	;

select dd.wk_begin_dte-1 
into calendar_posting_week
from dw.dim_date dd
where dy_dte = cast(getdate() as date);

--fob invoice sales : only exists for c11 data. 
drop table if exists stage_c11_amount_to_allocate_rule_21;
create temporary table stage_c11_amount_to_allocate_rule_21
as
 Select 	  audit_rec_src as source_system,
 		  bar_entity,
		  mapped_bar_custno,
		  sum(bar_amt) as bar_amt,
		  sum(sales_volume) as sales_volume,
		  sum(tran_volume) as tran_volume,
		  bcta.bar_currtype,
		  bcta.fiscal_month_id 
from stage.bods_core_transaction_agg bcta
LEFT JOIN ref_data.data_processing_rule dpr  on bcta.mapped_dataprocessing_ruleid = dpr.data_processing_ruleid 
where dpr.data_processing_ruleid =21
	and bcta.fiscal_month_id = fmthid--fmthid
	and bcta.audit_rec_src in  ('sap_c11') 
	and bcta.bar_acct = 'A40111'
	and bcta.posting_week_enddate <= calendar_posting_week
group by mapped_bar_custno,bar_currtype,audit_rec_src,bar_entity,fiscal_month_id
having sum(bar_amt)<>0;


-----Step 2 : 1. Pull transactions in A40111 BODS filtering on bar_bu = 'GTS', shipto to US/CA (kunnr -> land1 mapping in sapc11.kna1) (Total 40111). 
----Use these transactions in UMM
---perform volume conversion : for fob_invoice sales
drop table if exists stage_base_allocation_rate_by_entity; 
create temporary table stage_base_allocation_rate_by_entity
as 
Select bar_entity,
 	   bar_currtype,
 	   mapped_bar_custno, 
 	   toat_amt,
 	   total_amt / total_amt_per_cust as wt_avg 
 from (
 Select bar_entity,
 	   bar_currtype,
 	   mapped_bar_custno, 
 	   bar_amt as toat_amt,
 	   sum(bar_amt) over (partition by mapped_bar_custno, bar_currtype) as total_amt_per_cust,
 	   cast(bar_amt as numeric(19,6)) as total_amt
 from stage_c11_amount_to_allocate_rule_21
)a WHERE total_amt_per_cust <> 0;
 
drop table if exists manuf_fob_invoice_sales;
create temporary table manuf_fob_invoice_sales
as 
Select  s.audit_rec_src as  source_system,
	   s.org_tranagg_id,
	   posting_week_enddate,
	   fiscal_month_id,
 	   s.bar_acct,
 	   org_bar_brand,
 	   org_bar_custno,
 	   org_bar_product,
 	   mapped_bar_brand,
 	   mapped_bar_custno,
 	   mapped_bar_product,
	   'unknown' as org_shiptocust,
	   'unknown' as org_soldtocust,
	   'unknown' as org_material,
    	   s.shiptocust,
 	   s.soldtocust,
 	   s.material,
 	   isnull(sgm.gpp_portfolio,'ADJ_FOB_NO_CUST')  as alloc_bar_product,
 	   isnull(fsbm.bar_custno,'ADJ_FOB_NO_CUST') as alloc_bar_custno, -->mapped_bar_custno 
 	   isnull(sbm.bar_brand,'N/A') as alloc_bar_brand,
 	   s.mapped_dataprocessing_ruleid, 
 	   s.bar_currtype,
 	   bar_amt as bar_amt,
 	   sales_volume as sales_volume,
  	   tran_volume as tran_volume,
        uom 
from stage.bods_core_transaction_agg s
 left join ref_data.sku_gpp_mapping sgm on lower(s.material) = lower(sgm.material)  and sgm.current_flag =1
 left join ref_data.fob_soldto_barcust_mapping fsbm on lower(fsbm.soldtocust) = lower(s.soldtocust)
 left join ref_data.sku_barbrand_mapping sbm on lower(s.material) = lower(sbm.material)  and sbm.current_flag =1
 left join {{ source('sapc11', 'kna1') }} kc on lower(shiptocust) = lower(kc.kunnr) 
where s.audit_rec_src = 'ext_c11fob'
and bar_acct = 'A40111'
and kc.land1 in ('CA','US')
and s.posting_week_enddate <= calendar_posting_week
and fiscal_month_id = fmthid;
--
--Select sum(bar_amt),  alloc_bar_custno
--from manuf_fob_invoice_sales
--group by alloc_bar_custno;
----

select max(posting_week_enddate)  
into current_posting_week
from manuf_fob_invoice_sales;

		
		
drop table if exists allocated_fob_invoice_sales; 
create temporary table allocated_fob_invoice_sales
as
Select *
from (
---union the gap between allocated data from GTS and unallocated from GTS_NA
with allocated_fob_sales as (
Select isnull(r.bar_entity,'E2035') as bar_entity,s.fiscal_month_id,alloc_bar_custno, s.bar_currtype,
	  sum(s.bar_amt*isnull(r.wt_avg,1))  as alloc_bar_amt, 
	  0 as alloc_sales_volume,
	  0 as alloc_tran_volume 
--	  sum(s.sales_volume*isnull(r.wt_avg,1)) as alloc_sales_volume,
--	  sum(s.tran_volume*isnull(r.wt_avg,1)) as alloc_tran_volume
from manuf_fob_invoice_sales s 
left join  stage_base_allocation_rate_by_entity r on s.bar_currtype = r.bar_currtype
		and s.alloc_bar_custno = r.mapped_bar_custno
group by isnull(r.bar_entity,'E2035'),s.fiscal_month_id,alloc_bar_custno,s.bar_currtype
), tobe_allocated_fob_sales as (
	Select bar_entity,mapped_bar_custno,bar_currtype,fiscal_month_id,
		sum(bar_amt) as bar_amt, 
		sum(sales_volume) as sales_volume, 
		sum(tran_volume) as tran_volume
	from stage_c11_amount_to_allocate_rule_21
		--where mapped_bar_custno <> 'Customer_None'
	group by bar_entity,mapped_bar_custno,bar_currtype,fiscal_month_id
)
Select cast('sap_c11' as varchar(10)) as source_system,
	  -1 as org_tranagg_id, 
	  coalesce(current_posting_week,d.range_end_date)  as posting_week_enddate,
	  al.fiscal_month_id,
	  al.bar_entity,
	  'A40111' as bar_acct,
	  case when al.alloc_bar_custno= 'ADJ_FOB_NO_CUST' then 'N/A'
	  	  else  'N/A' 
	  	 end as org_bar_brand,
	  case when al.alloc_bar_custno= 'ADJ_FOB_NO_CUST' then 'ADJ_FOB_NO_CUST'
	  	  else  'ADJ_FOB' 
	  	 end as org_bar_custno,
	  case when al.alloc_bar_custno= 'ADJ_FOB_NO_CUST' then 'ADJ_FOB_NO_CUST'
	  	  else  'ADJ_FOB' 
	  	 end as org_bar_product,
	  case when al.alloc_bar_custno= 'ADJ_FOB_NO_CUST' then 'N/A'
	  	  else  'N/A' 
	  	 end as mapped_bar_brand,
	   al.alloc_bar_custno as mapped_bar_custno,
       case when al.alloc_bar_custno= 'ADJ_FOB_NO_CUST' then 'ADJ_FOB_NO_CUST'
	  	  else  'ADJ_FOB' 
	  	 end as mapped_bar_product,	  
	  case when al.alloc_bar_custno= 'ADJ_FOB_NO_CUST' then 'ADJ_FOB_NO_CUST'
	  	  else  'ADJ_FOB' 
	  	 end as org_shiptocust,
	  case when al.alloc_bar_custno= 'ADJ_FOB_NO_CUST' then 'ADJ_FOB_NO_CUST'
	  	  else  'ADJ_FOB' 
	  	 end as org_soldtocust,
	 case when al.alloc_bar_custno= 'ADJ_FOB_NO_CUST' then 'ADJ_FOB_NO_CUST'
	  	  else  'ADJ_FOB' 
	  	 end as org_material,
	  case when al.alloc_bar_custno= 'ADJ_FOB_NO_CUST' then 'ADJ_FOB_NO_CUST'
	  	  else  'ADJ_FOB' 
	  	 end as alloc_shiptocust,
	 case when al.alloc_bar_custno= 'ADJ_FOB_NO_CUST' then 'ADJ_FOB_NO_CUST'
	  	  else  'ADJ_FOB' 
	  	 end as alloc_soldtocust,
	  case when al.alloc_bar_custno= 'ADJ_FOB_NO_CUST' then 'ADJ_FOB_NO_CUST'
	  	  else  'ADJ_FOB' 
	  	 end as alloc_material,
	  case when al.alloc_bar_custno= 'ADJ_FOB_NO_CUST' then 'ADJ_FOB_NO_CUST'
	  	  else  'ADJ_FOB' 
	  	 end as alloc_bar_product,
	  al.bar_currtype,
	  21 as org_dataprocessing_ruleid,
	  21 as mapped_dataprocessing_ruleid,  --will always be 21
	  2 as dataprocessing_outcome_id, --2 as not allocated
	  case when al.alloc_bar_custno= 'ADJ_FOB_NO_CUST' then 'phase 104' else  'phase 103' end as dataprocessing_phase,
	  (alloc_bar_amt - isnull(bar_amt,0))*-1 as alloc_bar_amt,
	  (alloc_sales_volume - isnull(sales_volume,0)) as alloc_sales_volume,
	  (alloc_tran_volume - isnull(tran_volume,0)) as alloc_tran_volume,
	  cast('ea' as varchar(10)) as uom,
	  cast(getdate() as timestamp) as audit_loadts
from allocated_fob_sales al
left join tobe_allocated_fob_sales tal on  alloc_bar_custno = mapped_bar_custno
and al.bar_currtype = tal.bar_currtype
and al.bar_entity = tal.bar_entity
left join vtbl_date_range d on al.fiscal_month_id = d.fiscal_month_id
union all
-----customer none goes here
Select cast('sap_c11' as varchar(10)) as source_system,
	  -1 as org_tranagg_id, 
	  coalesce(current_posting_week,d.range_end_date)  as posting_week_enddate,
	  tal.fiscal_month_id,
	  tal.bar_entity,
	  'A40111' as bar_acct,
	  'N/A' as org_bar_brand,
	  'ADJ_FOB' as org_bar_custno,
	  'ADJ_FOB' as org_bar_product,
	  'N/A' as mapped_bar_brand,
	   tal.mapped_bar_custno,
       'ADJ_FOB' as mapped_bar_product,	  
	  'ADJ_FOB' as org_shiptocust,
	  'ADJ_FOB' as org_soldtocust,
	  'ADJ_FOB' as org_material,
	  'ADJ_FOB' as alloc_shiptocust,
	  'ADJ_FOB' as alloc_soldtocust,
	  'ADJ_FOB' as alloc_material,
	  'ADJ_FOB' as alloc_bar_product,
	  tal.bar_currtype,
	  21 as org_dataprocessing_ruleid,
	  21 as mapped_dataprocessing_ruleid,  --will always be 21
	  2 as dataprocessing_outcome_id, --2 as not allocated
	  'phase 103' as dataprocessing_phase,
	  isnull(bar_amt,0) as alloc_bar_amt,
	  isnull(sales_volume,0) as alloc_sales_volume,
	  isnull(tran_volume,0) as alloc_tran_volume,
	  cast('ea' as varchar(10)) as uom,
	  cast(getdate() as timestamp) as audit_loadts
from tobe_allocated_fob_sales tal 
left join allocated_fob_sales al on  alloc_bar_custno = mapped_bar_custno
and al.bar_currtype = tal.bar_currtype
and al.bar_entity = tal.bar_entity
left join vtbl_date_range d on tal.fiscal_month_id = d.fiscal_month_id
where al.alloc_bar_custno is null 
union all 
Select cast('sap_c11' as varchar(10)) as source_system,
	  s.org_tranagg_id,  
	  s.posting_week_enddate,
	  s.fiscal_month_id,
	  isnull(r.bar_entity,'E2035'),
	  s.bar_acct,
	  org_bar_brand,
	  org_bar_custno,
	  org_bar_product,
	  s.alloc_bar_brand as mapped_bar_brand,
	  s.alloc_bar_custno as mapped_bar_custno,   ---allocated customers are mapped here as cust is never traversed  
	  s.mapped_bar_product,
	  org_shiptocust,
	  org_soldtocust,
	  org_material, 
	  s.shiptocust as alloc_shiptocust,
	  s.soldtocust as alloc_soldtocust,
	  s.material as alloc_material,
	  s.alloc_bar_product,
	  s.bar_currtype,
	  s.mapped_dataprocessing_ruleid as org_dataprocessing_ruleid,
	  s.mapped_dataprocessing_ruleid,  --will always be 21
	  1 as dataprocessing_outcome_id, --1 as allocated
	  'phase 3' as dataprocessing_phase,
	  s.bar_amt*isnull(r.wt_avg,1)  as alloc_bar_amt, 
	  s.sales_volume*isnull(r.wt_avg,1) as sales_volume,
	  s.tran_volume*isnull(r.wt_avg,1) as tran_volume,
	  s.uom,
	  cast(getdate() as timestamp) as audit_loadts
from manuf_fob_invoice_sales s 
left join  stage_base_allocation_rate_by_entity r on s.bar_currtype = r.bar_currtype
		and s.alloc_bar_custno = r.mapped_bar_custno
		
) a;

delete from stage.sgm_allocated_data_rule_21
where fiscal_month_id = fmthid and source_system ='sap_c11';

INSERT INTO stage.sgm_allocated_data_rule_21
(	source_system, 
	org_tranagg_id, 
	posting_week_enddate, 
	fiscal_month_id, 
	bar_entity, 
	bar_acct, 
	org_bar_brand, 
	org_bar_custno, 
	org_bar_product, 
	mapped_bar_brand, 
	mapped_bar_custno, 
	mapped_bar_product, 
	org_shiptocust, 
	org_soldtocust, 
	org_material, 
	alloc_shiptocust, 
	alloc_soldtocust, 
	alloc_material, 
	alloc_bar_product, 
	bar_currtype, 
	org_dataprocessing_ruleid, 
	mapped_dataprocessing_ruleid, 
	dataprocessing_outcome_id, 
	dataprocessing_phase, 
	allocated_amt, 
	sales_volume, 
	tran_volume, 
	uom, 
	audit_loadts)
Select source_system, 
	org_tranagg_id, 
	posting_week_enddate, 
	fiscal_month_id, 
	bar_entity, 
	bar_acct, 
	org_bar_brand, 
	org_bar_custno, 
	org_bar_product, 
	mapped_bar_brand, 
	mapped_bar_custno, 
	mapped_bar_product, 
	org_shiptocust, 
	org_soldtocust, 
	org_material, 
	alloc_shiptocust, 
	alloc_soldtocust, 
	alloc_material, 
	alloc_bar_product, 
	bar_currtype, 
	org_dataprocessing_ruleid, 
	mapped_dataprocessing_ruleid, 
	dataprocessing_outcome_id, 
	dataprocessing_phase, 
	alloc_bar_amt as allocated_amt, 
	alloc_sales_volume as sales_volume, 
	alloc_tran_volume as tran_volume, 
	uom, 
	audit_loadts
from allocated_fob_invoice_sales;
	
--	
--
--Select sum(allocated_amt), mapped_bar_custno, uom, sum(sales_volume), sum(tran_volume)
--from stage.sgm_allocated_data_rule_21
--group by mapped_bar_custno, uom;
----
----
----Select sum(bar_amt), mapped_bar_custno
----from stage_c11_amount_to_allocate_rule_21
----group by mapped_bar_custno
--
--
--
--
---43115817.55000000	Customer_None	
--Select sum(allocated_amt), mapped_bar_custno,source_system , dataprocessing_phase 
--from stage.sgm_allocated_data_rule_21
--where fiscal_month_id=202003
--group by mapped_bar_custno,source_system,dataprocessing_phase
--order by 2;
	
  
exception
when others then raise info 'exception occur while ingesting data in stage.sgm_allocated_data_rule_21_c11';
end;
$_$
;

CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_21_hfm(fmthid integer)
 LANGUAGE plpgsql
AS $_$
--DECALRE Variables here
BEGIN 
	
---step 1 : 1. Keep all current transactions pulled for A40111 (transactions booked to a GTS-NA Entity). Do not apply allocation rule to the account. Sum up $ each month for each customer. Do not use these transactions in UMM
--- : acct exception - fob_invoicesale
	/* create temp table for select ected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid 
	;
----volume is always 0 for hfm data for fob_invoice sales
drop table if exists stage_hfm_amount_to_allocate_rule_21;
create temporary table stage_hfm_amount_to_allocate_rule_21
diststyle key
distkey (org_tranagg_id)
as
 Select 	  audit_rec_src as source_system,
		  bcta.org_tranagg_id,
		  posting_week_enddate,
		  bcta.fiscal_month_id,
		  bcta.bar_entity,
		  bcta.bar_acct,
		  org_bar_brand,
		  org_bar_custno,
		  org_bar_product,
		  mapped_bar_brand,
		  mapped_bar_custno,
		  mapped_bar_product,
		  bcta.shiptocust as org_shiptocust,
		  bcta.soldtocust as org_soldtocust,
		  bcta.material as org_material,
		  bar_currtype,
		  bcta.org_dataprocessing_ruleid, 
		  bcta.mapped_dataprocessing_ruleid,
		  bar_amt
from stage.bods_core_transaction_agg bcta
LEFT JOIN ref_data.data_processing_rule dpr  on bcta.mapped_dataprocessing_ruleid = dpr.data_processing_ruleid 
where dpr.data_processing_ruleid =21
	and bcta.fiscal_month_id = fmthid----fmthid
	and bcta.audit_rec_src in  ('hfm')  -----stored_proc : parameter
	and bcta.bar_acct = 'A40111'
	and bcta.bar_amt <> 0;
drop table if exists bar_product_base;
create temporary table bar_product_base 
as 
select 	name as bar_product, 
			case generation
				when 1  then null
			when 2  then level1 
			when 3  then level2 
			when 4  then level3 
			when 5  then level4 
			when 6  then level5 
			when 7  then level6 
			when 8  then level7 
			when 9  then level8 
			when 10 then level9 
			when 11 then level10 
		end as parent,
		description as bar_product_desc,
		case when membertype is null then 'unknown' else membertype end as membertype,
		case when bar_product is null then 'unknown' else bar_product end as portfolio,
		case when bar_product_desc is null then 'unknown' else bar_product_desc end as portfolio_desc,
		cast(generation as int) as generation,
		case when level1 is null then 'unknown' else level1 end as level01_bar,
		case when level2 is null then 'unknown' else level2 end as level02_bar,
		case when level3 is null then 'unknown' else level3 end as level03_bar,
		case when level4 is null then 'unknown' else level4 end as level04_bar,
		case when level5 is null then 'unknown' else level5 end as level05_bar,
		case when level6 is null then 'unknown' else level6 end as level06_bar,
		case when level7 is null then 'unknown' else level7 end as level07_bar,
		case when level8 is null then 'unknown' else level8 end as level08_bar,
		case when level9 is null then 'unknown' else level9 end as level09_bar
from 	{{ source('bods', 'drm_product') }}
where 	loaddts = ( select max(loaddts) from {{ source('bods', 'drm_product') }} dpc );
--Select s.*
--from {{ source('bods', 'hfm_vw_hfm_actual_trans') }} s 
--inner join (select fyr_id, lower(SUBSTRING(fmth_name,1,3)) as bar_period,
--			   fmth_id as fiscal_month_id,
--			   min(cast(fmth_begin_dte as date)) as fiscal_month_begin_date,
--			   min(cast(fmth_end_dte as date)) as fiscal_month_end_date
--		  from ref_data.calendar 
--		  group by fyr_id, lower(SUBSTRING(fmth_name,1,3)), fmth_id
--		  ) c on cast(s."year" as integer) = c.fyr_id 
--		and lower(s."period") = c.bar_period
--where bar_acct = 'A40111'
--and c.fiscal_month_id=202007
--and bar_bu='GTS';

--build_total_amount_for_rate_calculations
drop table if exists build_total_amount_for_rate_calculations_rule_21_p1;
create temporary table build_total_amount_for_rate_calculations_rule_21_p1
diststyle all
as 
Select 	rb.bar_entity,
		lower(bpb.level07_bar) as bar_division,
		rb.bar_custno, 
		rb.bar_currtype,
		rb.source_system,
		sum(total_bar_amt) as total_bar_amt
from stage.rate_base rb 
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= rb.range_start_date  and 
							dd.range_end_date >= rb.range_end_date
inner join bar_product_base bpb on rb.bar_product = bpb.bar_product
inner join (Select distinct mapped_bar_product as bar_product, 
				mapped_bar_custno as bar_custno
		 from stage_hfm_amount_to_allocate_rule_21 ) in_amt 
		 on  rb.bar_custno = in_amt.bar_custno 
			and lower(bpb.level07_bar) = lower(in_amt.bar_product) ---product division
group by 	rb.bar_custno,
		lower(bpb.level07_bar),
		rb.bar_entity,
		rb.bar_currtype,
		rb.source_system
order by lower(bpb.level07_bar);



drop table if exists build_rate_calculations_rule_21_p1;
----build averages now all combinations 
--Select bar_custno, bar_product,sum(weighted_avg)
--from (
create temporary table build_rate_calculations_rule_21_p1
diststyle all 
as 
Select 	rb.bar_entity,
		rb.bar_product, 
		rb.bar_custno, 
		rc.bar_division,
		'unknown' as shiptocust, 
		rb.soldtocust, 
		rb.material,
		rb.bar_currtype,
		rb.source_system,
	   	(rb.total_bar_amt / rc.total_bar_amt) as weighted_avg
from build_total_amount_for_rate_calculations_rule_21_p1 rc
inner join (select distinct bar_product,level07_bar from bar_product_base) bpb on lower(bpb.level07_bar) = lower(rc.bar_division)
inner join stage.rate_base rb on  rb.bar_custno = rc.bar_custno 
		 and rb.bar_product = bpb.bar_product
		 and rb.bar_entity = rc.bar_entity
		 and rb.bar_currtype = rc.bar_currtype
		 and rb.source_system = rc.source_system
inner join vtbl_date_range dd 
		on 	dd.range_start_date <= rb.range_start_date  and 
		dd.range_end_date >= rb.range_end_date;
--) a 
--group by bar_custno, bar_product
drop table if exists build_p1_mapped_brand_for_material;
create temporary table build_p1_mapped_brand_for_material
diststyle all 
as 	
Select source_system,
	  material,
	  mapped_bar_brand,
	  row_number() over (partition by material order by sales_tran_cnt desc) as rank_tran_cnt
from (
	Select bcta.audit_rec_src as source_system ,bcta.material, mapped_bar_brand, count(1) sales_tran_cnt
	from (select distinct material from build_rate_calculations_rule_21_p1) rt
	inner join stage.bods_core_transaction_agg bcta  on rt.material = bcta.material 
	where bar_acct = 'A40110'
	group by bcta.audit_rec_src,bcta.material,mapped_bar_brand
) mat ;
	
	
drop table if exists sgm_hfm_allocated_data_rule_21_p1; 
create temporary table sgm_hfm_allocated_data_rule_21_p1
diststyle even 
sortkey (posting_week_enddate)
as 
Select 	  in_amt.source_system,
		  org_tranagg_id,
		  posting_week_enddate,
		  fiscal_month_id,
		  in_amt.bar_entity,
		  in_amt.bar_acct,
		  in_amt.org_bar_brand,
		  org_bar_custno,
		  org_bar_product,
		  coalesce(mbm.mapped_bar_brand,in_amt.mapped_bar_brand) as mapped_bar_brand,
		  mapped_bar_custno,
		  mapped_bar_product,
		  org_shiptocust,
		  org_soldtocust,
		  org_material,
		  rt.bar_product as alloc_bar_product,
		  shiptocust as alloc_shiptocust,
		  soldtocust as alloc_soldtocust,
		  rt.material as alloc_material,
		  in_amt.bar_currtype,
		  1 as dataprocessing_outcome_id,
		  'phase 4' as dataprocessing_phase,
		  org_dataprocessing_ruleid,
		  mapped_dataprocessing_ruleid,
		  weighted_avg*in_amt.bar_amt as allocated_amt
from build_rate_calculations_rule_21_p1 rt
inner join stage_hfm_amount_to_allocate_rule_21 in_amt on rt.bar_custno = in_amt.mapped_bar_custno 
		and lower(rt.bar_division) = lower(in_amt.mapped_bar_product)
left join build_p1_mapped_brand_for_material mbm on rt.source_system = mbm.source_system
		and rt.material = mbm.material 
		and rank_tran_cnt=1;	

delete from stage.sgm_allocated_data_rule_21
where fiscal_month_id = fmthid and source_system ='hfm';

INSERT INTO stage.sgm_allocated_data_rule_21
(	source_system, 
	org_tranagg_id, 
	posting_week_enddate, 
	fiscal_month_id, 
	bar_entity, 
	bar_acct, 
	org_bar_brand, 
	org_bar_custno, 
	org_bar_product, 
	mapped_bar_brand, 
	mapped_bar_custno, 
	mapped_bar_product, 
	org_shiptocust, 
	org_soldtocust, 
	org_material, 
	alloc_shiptocust, 
	alloc_soldtocust, 
	alloc_material, 
	alloc_bar_product, 
	bar_currtype, 
	org_dataprocessing_ruleid, 
	mapped_dataprocessing_ruleid, 
	dataprocessing_outcome_id, 
	dataprocessing_phase, 
	allocated_amt, 
	sales_volume, 
	tran_volume, 
	uom, 
	audit_loadts)
Select source_system, 
	org_tranagg_id, 
	posting_week_enddate, 
	fiscal_month_id, 
	bar_entity, 
	bar_acct, 
	org_bar_brand, 
	org_bar_custno, 
	org_bar_product, 
	mapped_bar_brand, 
	mapped_bar_custno, 
	mapped_bar_product, 
	org_shiptocust, 
	org_soldtocust, 
	org_material, 
	alloc_shiptocust, 
	alloc_soldtocust, 
	alloc_material, 
	alloc_bar_product, 
	bar_currtype, 
	org_dataprocessing_ruleid, 
	mapped_dataprocessing_ruleid, 
	dataprocessing_outcome_id, 
	dataprocessing_phase, 
	allocated_amt, 
	0 sales_volume, 
	0 tran_volume, 
	'unknown' as uom, 
	cast(getdate() as timestamp) audit_loadts
from sgm_hfm_allocated_data_rule_21_p1;
  
exception
when others then raise info 'exception occur while ingesting data in stage.sgm_allocated_data_rule_21_hfm';
end;
$_$
;

CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_22(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN   
	
	--TESTING
	--delete from stage.sgm_allocated_data_rule_22;
	--call stage.p_allocate_data_rule_22 (201910)
	--call stage.p_allocate_data_rule_22 (202007)
	--select count(*) from stage.sgm_allocated_data_rule_22
	-- select * from stage.sgm_allocated_data_rule_22
	--select fiscal_month_id, count(*) from stage.sgm_allocated_data_rule_22 group by fiscal_month_id order by 1
/*
 *	This procedure manages the allocations for Rule ID #22
 *
 *		Allocation Exception - Royalty - A40910
 *
 * 		Final Table(s): 
 *			stage.sgm_allocated_data_rule_22
 *
 * 		Rule Logic:	
 * 			assign material/soldto -> special "Royalty SKU/SoldTo" 
 *
 */
	
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	/* copy transactions to be allocated from bods_tran_agg */
	drop table if exists _trans_unalloc
	;
	create temporary table _trans_unalloc as 
		select 	 tran.audit_rec_src as source_system
				,tran.org_tranagg_id
				,tran.posting_week_enddate
				,tran.fiscal_month_id
				,tran.bar_entity
				,tran.bar_acct
				
				,tran.org_bar_brand
				,tran.org_bar_custno
				,tran.org_bar_product
				
				,tran.mapped_bar_brand
				,tran.mapped_bar_custno
				,tran.mapped_bar_product
				
				,tran.shiptocust
				,tran.soldtocust as org_soldtocust
				,tran.material
				
				,tran.bar_currtype
				
				,tran.bar_amt as unallocated_bar_amt
				
				,tran.org_dataprocessing_ruleid
				,tran.mapped_dataprocessing_ruleid
				
				,tran.uom
				,case when tran.org_dataprocessing_ruleid = 1 then tran.sales_volume else 0 end as sales_volume
				,case when tran.org_dataprocessing_ruleid = 1 then tran.tran_volume else 0 end as tran_volume
				
		from 	stage.bods_core_transaction_agg as tran
				inner join ref_data.data_processing_rule as dpr
					on  dpr.data_processing_ruleid = tran.mapped_dataprocessing_ruleid 
				inner join vtbl_date_range as dt_rng
					on 	tran.posting_week_enddate between dt_rng.range_start_date and dt_rng.range_end_date
		where 	0=0
			and dpr.data_processing_ruleid = 22
			and tran.audit_rec_src in  ('sap_c11', 'sap_lawson', 'sap_p10')
	;
	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	stage.sgm_allocated_data_rule_22
	where 	posting_week_enddate between 
			(select range_start_date from vtbl_date_range) and 
			(select range_end_date from vtbl_date_range)
	;
	/* load transactions */
	insert into stage.sgm_allocated_data_rule_22 (
	
				source_system,
				org_tranagg_id,
				
				posting_week_enddate,
				fiscal_month_id,
				
				bar_entity,
				bar_acct,
				
				org_bar_brand,
				org_bar_custno,
				org_bar_product,
				mapped_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				
				org_shiptocust,
				org_soldtocust,
				org_material,
				
				alloc_shiptocust,
				alloc_soldtocust,
				alloc_material,
				alloc_bar_product,
				
				bar_currtype,
								
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_id,
				dataprocessing_phase,
				
				allocated_amt,
				sales_volume,
				tran_volume,
				uom,
				
				audit_loadts
		)
		select 	tran.source_system,
				tran.org_tranagg_id,
				
				tran.posting_week_enddate,
				tran.fiscal_month_id,
				
				tran.bar_entity,
				tran.bar_acct,
				
				tran.org_bar_brand,
				tran.org_bar_custno,
				tran.org_bar_product,
				tran.mapped_bar_brand,
				tran.mapped_bar_custno,
				tran.mapped_bar_product,
				
				tran.shiptocust as org_shiptocust,
				tran.org_soldtocust,
				tran.material as org_material,
				
				'ADJ_ROYALTY' as alloc_shiptocust,
				'ADJ_ROYALTY' as alloc_soldtocust,
				'ADJ_ROYALTY' as alloc_material,
				tran.mapped_bar_product as alloc_bar_product,
				
				bar_currtype,
								
				tran.org_dataprocessing_ruleid,
				tran.mapped_dataprocessing_ruleid,
				1 as dataprocessing_outcome_id,
				'phase 5' as dataprocessing_phase,
				
				tran.unallocated_bar_amt as allocated_amt,
				tran.sales_volume,
				tran.tran_volume,
				tran.uom,
				
				getdate() as audit_loadts
		from 	_trans_unalloc as tran
	;
	
exception
when others then raise info 'exception occur while ingesting data in stage.sgm_allocated_data_rule_22';
end;
$$
;