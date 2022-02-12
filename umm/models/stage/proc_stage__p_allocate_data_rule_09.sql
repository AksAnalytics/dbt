
CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_09(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECALRE Variables here
BEGIN 
	
---step 1 : get amount to be allocated from stage_agg
---Step 2 : Phase 1 : Allocations check if we have sku, shipto, soldto in rate_base for combination of mapped_bar_custno and mapped_bar_product in stage.base_rate
		  -- if yes perform allocation 
---step 3 : Phase 2 : build hierarchy and go one level up in the product, retain customer and check how many combinations are present in rate_base which 
		  ---are required to be allocated 
---step 4 : build transitent rate table 
---step 5 : perform allocation and validate if entire amount is allocated 
---step 5 : create leackage / exception table for anything left over 
---step 6 : delete prior allocation data from rule_09 table if exists   	
---step 7 :  union all entire result set and dump data into final rule_09 table	

	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid 
	;
drop table if exists stage_amount_to_allocate_rule_09;
create temporary table stage_amount_to_allocate_rule_09
diststyle all
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
where dpr.data_processing_ruleid =9
	and bcta.fiscal_month_id = fmthid--fmthid
	and bcta.audit_rec_src in  ('sap_c11', 'sap_p10', 'sap_lawson','hfm'); ----stored_proc : parameter
--	and bcta.bar_acct = 'A40110'
--	and bcta.org_bar_custno = 'HomeDepot';
---Step 2 : Phase 1 : Allocations check if we have sku, shipto, soldto in rate_base for combination of bar_custno and bar_product in stage.base_rate
		  -- if yes perform allocation 
---phase 1 : match 
--
--Select sum(bar_amt), source_system 
--from stage_amount_to_allocate_rule_09
--group by source_system
	
	
		/*
		Select distinct mapped_bar_product, mapped_bar_custno,bar_currtype, source_system
		from stage_amount_to_allocate_rule_09 in_amt
		intersect 
		Select distinct bar_product, bar_custno,bar_currtype, source_system
		from stage.rate_base rb
		inner join vtbl_date_range dd 
						on 	dd.range_start_date <= rb.range_start_date  and 
							dd.range_end_date >= rb.range_end_date
		
		*/
--DFES_AUTOPT_DIV_OTH	

----build_total_amount_for_rate_calculations
drop table if exists build_total_amount_for_rate_calculations_rule_09_p1;
create temporary table build_total_amount_for_rate_calculations_rule_09_p1
diststyle all
as 
Select 	rb.bar_entity,
		rb.bar_product, 
		rb.bar_custno, 
		rb.bar_currtype,
		rb.source_system,
		sum(total_bar_amt) as total_bar_amt
from stage.rate_base rb 
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= rb.range_start_date  and 
							dd.range_end_date >= rb.range_end_date
inner join (Select distinct mapped_bar_product as bar_product, 
				mapped_bar_custno as bar_custno,
				bar_entity,
				bar_currtype,
				source_system
		 from stage_amount_to_allocate_rule_09 ) in_amt 
		 on  rb.bar_custno = in_amt.bar_custno 
			and rb.bar_product = in_amt.bar_product 
			and rb.bar_entity = in_amt.bar_entity
			and rb.bar_currtype  = in_amt.bar_currtype
			and rb.source_system = in_amt.source_system
group by 	rb.bar_product, 
		rb.bar_custno,
		rb.bar_entity,
		rb.bar_currtype,
		rb.source_system; 
	
drop table if exists build_rate_calculations_rule_09_p1;
----build averages now all combinations 
--Select bar_custno, bar_product,sum(weighted_avg)
--from (
create temporary table build_rate_calculations_rule_09_p1
diststyle all 
as 
Select 	rb.bar_entity,
		rb.bar_product, 
		rb.bar_custno, 
		'unknown' as shiptocust, 
		rb.soldtocust, 
		rb.material,
		rb.bar_currtype,
		rb.source_system,
	   	(rb.total_bar_amt / rc.total_bar_amt) as weighted_avg
from build_total_amount_for_rate_calculations_rule_09_p1 rc 
inner join stage.rate_base rb on  rb.bar_custno = rc.bar_custno 
		 and rb.bar_product = rc.bar_product
		 and rb.bar_entity = rc.bar_entity
		 and rb.bar_currtype = rc.bar_currtype
		 and rb.source_system = rc.source_system
inner join vtbl_date_range dd 
		on 	dd.range_start_date <= rb.range_start_date  and 
		dd.range_end_date >= rb.range_end_date;
--) a 
--group by bar_custno, bar_product
--pick the brand based on allocated material
	
	
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
	from (select distinct material from build_rate_calculations_rule_09_p1) rt
	inner join stage.bods_core_transaction_agg bcta  on rt.material = bcta.material 
	where bar_acct = 'A40110'
	group by bcta.audit_rec_src,bcta.material,mapped_bar_brand
) mat ;
	
	
drop table if exists sgm_allocated_data_rule_09_p1; 
create temporary table sgm_allocated_data_rule_09_p1
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
		  mapped_bar_product as alloc_bar_product,
		  shiptocust as alloc_shiptocust,
		  soldtocust as alloc_soldtocust,
		  rt.material as alloc_material,
		  in_amt.bar_currtype,
		  1 as dataprocessing_outcome_id,
		  'phase 1' as dataprocessing_phase,
		  org_dataprocessing_ruleid,
		  mapped_dataprocessing_ruleid,
		  weighted_avg*in_amt.bar_amt as allocated_amt
from build_rate_calculations_rule_09_p1 rt
inner join stage_amount_to_allocate_rule_09 in_amt on rt.bar_custno = in_amt.mapped_bar_custno 
		and rt.bar_product = in_amt.mapped_bar_product
		and rt.bar_entity = in_amt.bar_entity
		and rt.bar_currtype = in_amt.bar_currtype
		and rt.source_system = in_amt.source_system
left join build_p1_mapped_brand_for_material mbm on rt.source_system = mbm.source_system
		and rt.material = mbm.material 
		and rank_tran_cnt=1;
	
	
	
 
/*
--perform gap analysis	
Select sadr.org_tranagg_id,
		sadr.source_system,
		sadr.bar_acct,
		sadr.bar_entity,
		sadr.fiscal_month_id,
		sadr.org_bar_custno,
		sadr.org_bar_product,
		sadr.mapped_bar_custno,
		sadr.mapped_bar_product,
		sadr.org_material,
		sadr.org_shiptocust,
		sadr.org_soldtocust,
		sadr.bar_currtype,
		avg(sadr.bar_amt) as amt_to_allocate,
		sum(alloc.allocated_amt) as allocated_amt,
		sum(alloc.allocated_amt) - avg(sadr.bar_amt) as gap
from stage_amount_to_allocate_rule_09 sadr 
inner join sgm_allocated_data_rule_09_p1 alloc on sadr.org_tranagg_id = alloc.org_tranagg_id
group by sadr.org_tranagg_id,
		sadr.source_system,
		sadr.bar_acct,
		sadr.bar_entity,
		sadr.fiscal_month_id,
		sadr.org_bar_custno,
		sadr.org_bar_product,
		sadr.mapped_bar_custno,
		sadr.mapped_bar_product,
		sadr.org_material,
		sadr.org_shiptocust,
		sadr.org_soldtocust,
		sadr.bar_currtype
order by sum(alloc.allocated_amt) - avg(sadr.bar_amt) desc 
*/
	
	

---step 3 : for non matching pairs - build hierarchy and go two level up in the customer and product 
		  --and check how many combinations are present in rate_base. 
/*
 * pairs not present in base for allocate		  
	Select distinct mapped_bar_product, mapped_bar_custno,bar_entity, bar_currtype,source_system
	from stage_amount_to_allocate_rule_09 in_amt
	except 
	Select distinct bar_product, bar_custno,bar_entity, bar_currtype,source_system
	from stage.rate_base rb;
*/
drop table if exists stage_matching_cust_product_hierarchy;
	
create temporary table stage_matching_cust_product_hierarchy
diststyle all 
as 
with product_hierarchy_allocation_mapping as 
(
select lower(membertype) as level,
	  lower(name) as productno,
	  lower(superior3) as root_2_level_up,
	  lower(superior2) as root_1_level_up,
	  lower(superior1) as root_adjacent,
	  lower(description) as description,
	  lower(plnlevel) as pln_level,
	  generation,
	  start_date,
	  end_date
from ref_data.product_hierarchy_allocation_mapping pham 
cross join vtbl_date_range dd 
where dd.range_end_date between pham.start_date and pham.end_date 
),unalloc_base_prod_cust as 
(
	Select distinct bar_entity,mapped_bar_product, mapped_bar_custno,bar_currtype, source_system 
	from stage_amount_to_allocate_rule_09 in_amt
	except 
	Select distinct bar_entity,bar_product, bar_custno,bar_currtype, source_system
	from stage.rate_base rb
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= rb.range_start_date  and 
							dd.range_end_date >= rb.range_end_date
), unalloc_prod_hierarchy_cross_cust as 
(
Select distinct  unalloc.bar_entity,
	  unalloc.mapped_bar_custno as unalloc_bar_custno,
	  unalloc.mapped_bar_product as unalloc_bar_product,
	  ra.root_1_level_up,
	  ra.productno as alloc_bar_product,
	  ra.description as alloc_bar_product_desc,
	  unalloc.bar_currtype,
	  unalloc.source_system
from product_hierarchy_allocation_mapping pa 
inner join product_hierarchy_allocation_mapping ra on pa.root_adjacent = ra.root_1_level_up 
inner join (select distinct bar_entity, mapped_bar_custno, mapped_bar_product,bar_currtype,source_system
		 from unalloc_base_prod_cust) unalloc on lower(pa.productno) = lower(unalloc.mapped_bar_product)
where ra."level" = 'base'
) Select cph.bar_entity,
		unalloc_bar_custno,
		unalloc_bar_product,
		unalloc_bar_custno as alloc_bar_custno,
		alloc_bar_product,
		cph.bar_currtype,
		cph.source_system,
		root_1_level_up,
		alloc_bar_product_desc
	from unalloc_prod_hierarchy_cross_cust cph
	where exists (select 1 from stage.rate_base rb2 
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= rb2.range_start_date  and 
							dd.range_end_date >= rb2.range_end_date
					where lower(rb2.bar_custno) = loweR(cph.unalloc_bar_custno)
					and lower(cph.alloc_bar_product) = lower(rb2.bar_product)
					and lower(cph.bar_entity) = lower(rb2.bar_entity)
					and lower(cph.bar_currtype) = lower(rb2.bar_currtype)
					and lower(cph.source_system) = lower(rb2.source_system)
				);
--	and  unalloc_bar_product = 'CONSTR_HT_OTH'
--	and unalloc_bar_custno = 'Lowes'

	
		
drop table if exists build_rate_calculations_rule_09_p2;
--Select bar_entity ,
--		unalloc_bar_product, 
--	  unalloc_bar_custno,
--	  sum(weighted_avg)
--from (
create temporary table build_rate_calculations_rule_09_p2
diststyle all 
as 
Select cph.bar_entity,
	  unalloc_bar_product, 
	  unalloc_bar_custno,
	  rb.bar_product, 
	  rb.bar_custno, 
	  rb.soldtocust, 
	  rb.material,
	  cph.bar_currtype,
	  cph.source_system,
	  (rb.total_bar_amt) / sum(rb.total_bar_amt) over (partition by cph.bar_entity,unalloc_bar_product,unalloc_bar_custno,cph.bar_currtype) as weighted_avg
from stage_matching_cust_product_hierarchy cph
inner join stage.rate_base rb  on lower(cph.alloc_bar_product) = lower(rb.bar_product) and lower(cph.alloc_bar_custno) = lower(rb.bar_custno)
			and  lower(cph.bar_entity) = lower(rb.bar_entity)
			and lower(cph.bar_currtype) = lower(rb.bar_currtype)
			and lower(cph.source_system) = lower(rb.source_system)
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= rb.range_start_date  and 
							dd.range_end_date >= rb.range_end_date
where rb.total_bar_amt <> 0;
--)
--group by unalloc_bar_product, 
--	  unalloc_bar_custno,
--	  bar_entity
--order by 4;
	
drop table if exists build_p2_mapped_brand_for_material;
create temporary table build_p2_mapped_brand_for_material
diststyle all 
as 	
Select source_system,
	  material,
	  mapped_bar_brand,
	  row_number() over (partition by material order by sales_tran_cnt desc) as rank_tran_cnt
from (
	Select bcta.audit_rec_src as source_system ,bcta.material, mapped_bar_brand, count(1) sales_tran_cnt
	from (select distinct material from build_rate_calculations_rule_09_p2) rt
	inner join stage.bods_core_transaction_agg bcta  on rt.material = bcta.material 
	where bar_acct = 'A40110'
	group by bcta.audit_rec_src,bcta.material,mapped_bar_brand
) mat; 


drop table if exists sgm_allocated_data_rule_09_p2; 
create temporary table sgm_allocated_data_rule_09_p2
diststyle even 
sortkey (posting_week_enddate)
as 
Select 	  in_amt.source_system,
		  in_amt.org_tranagg_id,
		  in_amt.posting_week_enddate,
		  in_amt.fiscal_month_id,
		  in_amt.bar_entity,
		  in_amt.bar_acct,
		  in_amt.org_bar_brand,
		  in_amt.org_bar_custno,
		  in_amt.org_bar_product,
		  coalesce(mbm.mapped_bar_brand,in_amt.mapped_bar_brand) as mapped_bar_brand,
		  in_amt.mapped_bar_custno,
		  in_amt.mapped_bar_product as org_mapped_bar_product,
		  coalesce(rt.bar_product,in_amt.mapped_bar_product) as mapped_bar_product,
		  in_amt.org_shiptocust,
		  in_amt.org_soldtocust,
		  in_amt.org_material,
		  'unknown' as alloc_shiptocust,
		  rt.soldtocust as alloc_soldtocust,
		  rt.material as alloc_material,
		  rt.bar_product as alloc_bar_product,
		  in_amt.bar_currtype,
		  1 as dataprocessing_outcome_id,
		  'phase 2' as dataprocessing_phase,
		  org_dataprocessing_ruleid,
		  mapped_dataprocessing_ruleid,
		  weighted_avg*in_amt.bar_amt as allocated_amt
from build_rate_calculations_rule_09_p2 rt
inner join stage_amount_to_allocate_rule_09 in_amt on lower(rt.bar_custno) = lower(in_amt.mapped_bar_custno) 
		and lower(rt.unalloc_bar_product) = lower(in_amt.mapped_bar_product)
		and lower(rt.bar_entity) = lower(in_amt.bar_entity)
		and lower(rt.bar_currtype) = lower(in_amt.bar_currtype)
		and lower(rt.source_system) = lower(in_amt.source_system)
left join build_p2_mapped_brand_for_material mbm on rt.source_system = mbm.source_system
		and rt.material = mbm.material
		and rank_tran_cnt=1;
--where rt.unalloc_bar_product = 'DFES_AUTOPT_DIV_OTH'
--order by in_amt.org_tranagg_id



/*
--perform gap analysis	
Select sadr.org_tranagg_id,
		sadr.bar_acct,
		sadr.bar_entity,
		sadr.fiscal_month_id,
		sadr.org_bar_custno,
		sadr.org_bar_product,
		sadr.mapped_bar_custno,
		sadr.mapped_bar_product,
		sadr.mapped_bar_brand,
		sadr.org_material,
		sadr.org_shiptocust,
		sadr.org_soldtocust,
		sadr.bar_currtype,
		abs(avg(sadr.bar_amt)) as amt_to_allocate,
		abs(sum(alloc.allocated_amt)) as allocated_amt,
		abs(sum(alloc.allocated_amt)) - abs(avg(sadr.bar_amt)) as gap
from stage_amount_to_allocate_rule_09 sadr 
inner join sgm_allocated_data_rule_09_p2 alloc on sadr.org_tranagg_id = alloc.org_tranagg_id
group by sadr.org_tranagg_id,
		sadr.bar_acct,
		sadr.bar_entity,
		sadr.fiscal_month_id,
		sadr.org_bar_custno,
		sadr.org_bar_product,
		sadr.mapped_bar_custno,
		sadr.mapped_bar_product,
		sadr.mapped_bar_brand,
		sadr.org_material,
		sadr.org_shiptocust,
		sadr.org_soldtocust,
		sadr.bar_currtype
order by abs(sum(alloc.allocated_amt)) - abs(avg(sadr.bar_amt)) desc
*/
	
	
/******************************************************************************************************************************
 * 
 * 
 * 
 *********************************************************************************************************************************88*/	
/* Traverse down logic test 
 
with unalloc_parent_bar_product as 
( 
  select cast('Tradesman' as varchar(50)) as unalloc_bar_product
)
Select distinct unalloc_bar_product,pham2.name
from ref_data.product_hierarchy_allocation_mapping pham  
inner join unalloc_parent_bar_product unalloc on lower(pham.name)  = lower(unalloc.unalloc_bar_product) and pham.membertype = 'Parent'
inner join ref_data.product_hierarchy_allocation_mapping pham2 on lower(pham.name) = lower(pham2.superior2) and pham2.membertype = 'Base'
cross join vtbl_date_range dd 
Where lower(pham2.name) <> lower(unalloc.unalloc_bar_product)
and lower(pham2.name) <> lower(unalloc.unalloc_bar_product)
and dd.range_end_date between pham.start_date and pham.end_date 
order by 2;
 
 
*/	
	
--	
--	Select  mapped_bar_product, mapped_bar_custno, bar_acct, bar_amt
--	from stage_amount_to_allocate_rule_09 in_amt
--	where mapped_bar_custno='Royalties'
--	order by 2
	
	
-------traverse : two level down if bar_product is received at pareant level, and remained unallocated after phase 1 and 2 
drop table if exists stage_matching_cust_product_hierarchy_p3;
	
create temporary table stage_matching_cust_product_hierarchy_p3
diststyle all 
as 	
with unallocated_bar_product as ( 
Select mapped_bar_product
from (
	Select distinct mapped_bar_product 
	from stage_amount_to_allocate_rule_09 in_amt
	except
	( Select distinct org_mapped_bar_product
	  from sgm_allocated_data_rule_09_p2
	  union 
	  Select distinct mapped_bar_product
	  from sgm_allocated_data_rule_09_p1
	 )
 ) in_amt
where  exists (Select 1 
		  from ref_data.product_hierarchy_allocation_mapping pham
		  cross join vtbl_date_range dd 
		  where dd.range_end_date between pham.start_date and pham.end_date 
		  and pham.membertype = 'Parent'
		  and lower(in_amt.mapped_bar_product) = lower(pham.name)
		  )
)
Select distinct bar_entity,
			mapped_bar_custno as unalloc_bar_custno,
			unal_pr.mapped_bar_product as unalloc_bar_product, 
			mapped_bar_custno as alloc_bar_custno,
			pham2.name as alloc_bar_product,
			in_amt.bar_currtype,
			in_amt.source_system,
			pham2.superior2 as level_1_down,
			pham2.description as alloc_bar_product_desc
from unallocated_bar_product unal_pr 
cross join vtbl_date_range dd 
inner join stage_amount_to_allocate_rule_09 in_amt on unal_pr.mapped_bar_product = in_amt.mapped_bar_product 
inner join ref_data.product_hierarchy_allocation_mapping pham on lower(pham.name)  = lower(unal_pr.mapped_bar_product) and pham.membertype = 'Parent' and dd.range_end_date between pham.start_date and pham.end_date 
inner join ref_data.product_hierarchy_allocation_mapping pham2 on lower(pham.name) = lower(pham2.superior2) and pham2.membertype = 'Base' and dd.range_end_date between pham2.start_date and pham2.end_date 
Where lower(pham2.name) <> lower(unal_pr.mapped_bar_product)
and exists (select 1 from stage.rate_base rb2 
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= rb2.range_start_date  and 
							dd.range_end_date >= rb2.range_end_date
					where loweR(in_amt.mapped_bar_custno)=lower(rb2.bar_custno)
					and lower(pham2.name ) = lower(rb2.bar_product)
--					and lower(in_amt.bar_entity) = lower(rb2.bar_entity)
--					and lower(in_amt.bar_currtype) = lower(rb2.bar_currtype)
--					and lower(in_amt.source_system) = lower(rb2.source_system)
				);

drop table if exists build_rate_calculations_rule_09_p3;
--Select bar_entity ,
--		unalloc_bar_product, 
--	  unalloc_bar_custno,
--	  sum(weighted_avg)
--from (
create temporary table build_rate_calculations_rule_09_p3
diststyle all 
as 
Select cph.bar_entity,
	  unalloc_bar_product, 
	  unalloc_bar_custno,
	  rb.bar_product, 
	  rb.bar_custno, 
	  rb.soldtocust, 
	  rb.material,
	  cph.bar_currtype,
	  cph.source_system,
	  (rb.total_bar_amt) / sum(rb.total_bar_amt) over (partition by cph.bar_entity,unalloc_bar_product,unalloc_bar_custno,cph.bar_currtype) as weighted_avg
from stage_matching_cust_product_hierarchy_p3 cph
inner join stage.rate_base rb  on lower(cph.alloc_bar_product) = lower(rb.bar_product) and lower(cph.alloc_bar_custno) = lower(rb.bar_custno)
--			and  lower(cph.bar_entity) = lower(rb.bar_entity)
--			and lower(cph.bar_currtype) = lower(rb.bar_currtype)
--			and lower(cph.source_system) = lower(rb.source_system)
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= rb.range_start_date  and 
							dd.range_end_date >= rb.range_end_date
where rb.total_bar_amt <> 0;
--)
--group by unalloc_bar_product, 
--	  unalloc_bar_custno,
--	  bar_entity
--order by 4;
	
drop table if exists build_p3_mapped_brand_for_material;
create temporary table build_p3_mapped_brand_for_material
diststyle all 
as 	
Select source_system,
	  material,
	  mapped_bar_brand,
	  row_number() over (partition by material order by sales_tran_cnt desc) as rank_tran_cnt
from (
	Select bcta.audit_rec_src as source_system ,bcta.material, mapped_bar_brand, count(1) sales_tran_cnt
	from (select distinct material from build_rate_calculations_rule_09_p3) rt
	inner join stage.bods_core_transaction_agg bcta  on rt.material = bcta.material 
	where bar_acct = 'A40110'
	group by bcta.audit_rec_src,bcta.material,mapped_bar_brand
) mat; 


drop table if exists sgm_allocated_data_rule_09_p3; 
create temporary table sgm_allocated_data_rule_09_p3
diststyle even 
sortkey (posting_week_enddate)
as 
Select 	  in_amt.source_system,
		  in_amt.org_tranagg_id,
		  in_amt.posting_week_enddate,
		  in_amt.fiscal_month_id,
		  in_amt.bar_entity,
		  in_amt.bar_acct,
		  in_amt.org_bar_brand,
		  in_amt.org_bar_custno,
		  in_amt.org_bar_product,
		  coalesce(mbm.mapped_bar_brand,in_amt.mapped_bar_brand) as mapped_bar_brand,
		  in_amt.mapped_bar_custno,
		  in_amt.mapped_bar_product as org_mapped_bar_product,
		  coalesce(rt.bar_product,in_amt.mapped_bar_product) as mapped_bar_product,
		  in_amt.org_shiptocust,
		  in_amt.org_soldtocust,
		  in_amt.org_material,
		  'unknown' as alloc_shiptocust,
		  rt.soldtocust as alloc_soldtocust,
		  rt.material as alloc_material,
		  rt.bar_product as alloc_bar_product,
		  in_amt.bar_currtype,
		  1 as dataprocessing_outcome_id,
		  'phase 9' as dataprocessing_phase,
		  org_dataprocessing_ruleid,
		  mapped_dataprocessing_ruleid,
		  weighted_avg*in_amt.bar_amt as allocated_amt
from build_rate_calculations_rule_09_p3 rt
inner join stage_amount_to_allocate_rule_09 in_amt on lower(rt.bar_custno) = lower(in_amt.mapped_bar_custno) 
		and lower(rt.unalloc_bar_product) = lower(in_amt.mapped_bar_product)
		and lower(rt.bar_entity) = lower(in_amt.bar_entity)
		and lower(rt.bar_currtype) = lower(in_amt.bar_currtype)
		and lower(rt.source_system) = lower(in_amt.source_system)
left join build_p3_mapped_brand_for_material mbm on rt.source_system = mbm.source_system
		and rt.material = mbm.material
		and rank_tran_cnt=1;
--where rt.unalloc_bar_product = 'DFES_AUTOPT_DIV_OTH'
--order by in_amt.org_tranagg_id

/*
--perform gap analysis	
Select sadr.org_tranagg_id,
		sadr.bar_acct,
		sadr.bar_entity,
		sadr.fiscal_month_id,
		sadr.org_bar_custno,
		sadr.org_bar_product,
		sadr.mapped_bar_custno,
		sadr.mapped_bar_product,
		sadr.mapped_bar_brand,
		sadr.org_material,
		sadr.org_shiptocust,
		sadr.org_soldtocust,
		sadr.bar_currtype,
		abs(avg(sadr.bar_amt)) as amt_to_allocate,
		abs(sum(alloc.allocated_amt)) as allocated_amt,
		abs(sum(alloc.allocated_amt)) - abs(avg(sadr.bar_amt)) as gap
from stage_amount_to_allocate_rule_09 sadr 
inner join sgm_allocated_data_rule_09_p3 alloc on sadr.org_tranagg_id = alloc.org_tranagg_id
group by sadr.org_tranagg_id,
		sadr.bar_acct,
		sadr.bar_entity,
		sadr.fiscal_month_id,
		sadr.org_bar_custno,
		sadr.org_bar_product,
		sadr.mapped_bar_custno,
		sadr.mapped_bar_product,
		sadr.mapped_bar_brand,
		sadr.org_material,
		sadr.org_shiptocust,
		sadr.org_soldtocust,
		sadr.bar_currtype
order by abs(sum(alloc.allocated_amt)) - abs(avg(sadr.bar_amt)) desc
*/
/******************************************************************************************************************************
 * 
 *  Traversing Logic : One level up and then two level down
 * 
 *********************************************************************************************************************************88*/	
/* 
Test Code :  CONSTR_HT
with unalloc_parent_bar_product as 
( 
  select cast('constr_ht' as varchar(50)) as unalloc_bar_product
)
Select distinct unalloc_bar_product,pham.name, pham2.name
from ref_data.product_hierarchy_allocation_mapping pham  
inner join unalloc_parent_bar_product unalloc on lower(pham.name)  = lower(unalloc.unalloc_bar_product) and pham.membertype = 'Parent'
inner join ref_data.product_hierarchy_allocation_mapping pham2 on lower(pham.name) = lower(pham2.superior3) and pham2.membertype = 'Base'
cross join vtbl_date_range dd 
Where lower(pham2.name) <> lower(unalloc.unalloc_bar_product)
and lower(pham2.name) <> lower(unalloc.unalloc_bar_product)
and dd.range_end_date between pham.start_date and pham.end_date 
order by 2;
*/


drop table if exists stage_matching_cust_product_hierarchy_p4;
	
create temporary table stage_matching_cust_product_hierarchy_p4
diststyle all 
as 	
with unallocated_bar_product as ( 
Select mapped_bar_product
from (
	Select distinct mapped_bar_product 
	from stage_amount_to_allocate_rule_09 in_amt
	except
	( Select distinct org_mapped_bar_product
	  from sgm_allocated_data_rule_09_p2
	  union 
	  Select distinct mapped_bar_product
	  from sgm_allocated_data_rule_09_p1
	  union 
	   Select distinct org_mapped_bar_product
	  from sgm_allocated_data_rule_09_p3
	 )
 ) in_amt
where  exists (Select 1 
		  from ref_data.product_hierarchy_allocation_mapping pham
		  cross join vtbl_date_range dd 
		  where dd.range_end_date between pham.start_date and pham.end_date 
		  and pham.membertype = 'Parent'
		  and lower(in_amt.mapped_bar_product) = lower(pham.name)
		  )
)
Select distinct bar_entity,
			mapped_bar_custno as unalloc_bar_custno,
			unal_pr.mapped_bar_product as unalloc_bar_product, 
			mapped_bar_custno as alloc_bar_custno,
			pham2.name as alloc_bar_product,
			in_amt.bar_currtype,
			in_amt.source_system,
			pham2.superior2 as level_1_down,
			pham2.description as alloc_bar_product_desc
from unallocated_bar_product unal_pr 
inner join stage_amount_to_allocate_rule_09 in_amt on unal_pr.mapped_bar_product = in_amt.mapped_bar_product 
cross join vtbl_date_range dd 
inner join ref_data.parent_product_hierarchy_allocation_mapping ppham on lower(unal_pr.mapped_bar_product) = lower(ppham.name) and dd.range_end_date between ppham.start_date and ppham.end_date 
inner join ref_data.product_hierarchy_allocation_mapping pham on lower(pham.name)  = lower(ppham.superior1) and pham.membertype = 'Parent'  and dd.range_end_date between pham.start_date and pham.end_date 
inner join ref_data.product_hierarchy_allocation_mapping pham2 on lower(pham.name) = lower(pham2.superior3) and pham2.membertype = 'Base' and pham2.membertype = 'Base'  and dd.range_end_date between pham2.start_date and pham2.end_date 
Where lower(pham2.name) <> lower(unal_pr.mapped_bar_product)
and exists (select 1 from stage.rate_base rb2 
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= rb2.range_start_date  and 
							dd.range_end_date >= rb2.range_end_date
					where loweR(in_amt.mapped_bar_custno)=lower(rb2.bar_custno)
					and lower(pham2.name ) = lower(rb2.bar_product)
--					and lower(in_amt.bar_entity) = lower(rb2.bar_entity)
--					and lower(in_amt.bar_currtype) = lower(rb2.bar_currtype)
--					and lower(in_amt.source_system) = lower(rb2.source_system)
				);	
drop table if exists build_rate_calculations_rule_09_p4;
--Select bar_entity ,
--		unalloc_bar_product, 
--	  unalloc_bar_custno,
--	  sum(weighted_avg)
--from (
create temporary table build_rate_calculations_rule_09_p4
diststyle all 
as 
Select cph.bar_entity,
	  unalloc_bar_product, 
	  unalloc_bar_custno,
	  rb.bar_product, 
	  rb.bar_custno, 
	  rb.soldtocust, 
	  rb.material,
	  cph.bar_currtype,
	  cph.source_system,
	  (rb.total_bar_amt) / sum(rb.total_bar_amt) over (partition by cph.bar_entity,unalloc_bar_product,unalloc_bar_custno,cph.bar_currtype) as weighted_avg
from stage_matching_cust_product_hierarchy_p4 cph
inner join stage.rate_base rb  on lower(cph.alloc_bar_product) = lower(rb.bar_product) and lower(cph.alloc_bar_custno) = lower(rb.bar_custno)
--			and  lower(cph.bar_entity) = lower(rb.bar_entity)
--			and lower(cph.bar_currtype) = lower(rb.bar_currtype)
--			and lower(cph.source_system) = lower(rb.source_system)
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= rb.range_start_date  and 
							dd.range_end_date >= rb.range_end_date
where rb.total_bar_amt <> 0;
--)
--group by unalloc_bar_product, 
--	  unalloc_bar_custno,
--	  bar_entity
--order by 4;
	
drop table if exists build_p4_mapped_brand_for_material;
create temporary table build_p4_mapped_brand_for_material
diststyle all 
as 	
Select source_system,
	  material,
	  mapped_bar_brand,
	  row_number() over (partition by material order by sales_tran_cnt desc) as rank_tran_cnt
from (
	Select bcta.audit_rec_src as source_system ,bcta.material, mapped_bar_brand, count(1) sales_tran_cnt
	from (select distinct material from build_rate_calculations_rule_09_p4) rt
	inner join stage.bods_core_transaction_agg bcta  on rt.material = bcta.material 
	where bar_acct = 'A40110'
	group by bcta.audit_rec_src,bcta.material,mapped_bar_brand
) mat; 


drop table if exists sgm_allocated_data_rule_09_p4; 
create temporary table sgm_allocated_data_rule_09_p4
diststyle even 
sortkey (posting_week_enddate)
as 
Select 	  in_amt.source_system,
		  in_amt.org_tranagg_id,
		  in_amt.posting_week_enddate,
		  in_amt.fiscal_month_id,
		  in_amt.bar_entity,
		  in_amt.bar_acct,
		  in_amt.org_bar_brand,
		  in_amt.org_bar_custno,
		  in_amt.org_bar_product,
		  coalesce(mbm.mapped_bar_brand,in_amt.mapped_bar_brand) as mapped_bar_brand,
		  in_amt.mapped_bar_custno,
		  in_amt.mapped_bar_product as org_mapped_bar_product,
		  coalesce(rt.bar_product,in_amt.mapped_bar_product) as mapped_bar_product,
		  in_amt.org_shiptocust,
		  in_amt.org_soldtocust,
		  in_amt.org_material,
		  'unknown' as alloc_shiptocust,
		  rt.soldtocust as alloc_soldtocust,
		  rt.material as alloc_material,
		  rt.bar_product as alloc_bar_product,
		  in_amt.bar_currtype,
		  1 as dataprocessing_outcome_id,
		  'phase 11' as dataprocessing_phase,
		  org_dataprocessing_ruleid,
		  mapped_dataprocessing_ruleid,
		  weighted_avg*in_amt.bar_amt as allocated_amt
from build_rate_calculations_rule_09_p4 rt
inner join stage_amount_to_allocate_rule_09 in_amt on lower(rt.bar_custno) = lower(in_amt.mapped_bar_custno) 
		and lower(rt.unalloc_bar_product) = lower(in_amt.mapped_bar_product)
		and lower(rt.bar_entity) = lower(in_amt.bar_entity)
		and lower(rt.bar_currtype) = lower(in_amt.bar_currtype)
		and lower(rt.source_system) = lower(in_amt.source_system)
left join build_p4_mapped_brand_for_material mbm on rt.source_system = mbm.source_system
		and rt.material = mbm.material
		and rank_tran_cnt=1;
--where rt.unalloc_bar_product = 'DFES_AUTOPT_DIV_OTH'
--order by in_amt.org_tranagg_id

/*
--perform gap analysis	
Select sadr.org_tranagg_id,
		sadr.bar_acct,
		sadr.bar_entity,
		sadr.fiscal_month_id,
		sadr.org_bar_custno,
		sadr.org_bar_product,
		sadr.mapped_bar_custno,
		sadr.mapped_bar_product,
		sadr.mapped_bar_brand,
		sadr.org_material,
		sadr.org_shiptocust,
		sadr.org_soldtocust,
		sadr.bar_currtype,
		abs(avg(sadr.bar_amt)) as amt_to_allocate,
		abs(sum(alloc.allocated_amt)) as allocated_amt,
		abs(sum(alloc.allocated_amt)) - abs(avg(sadr.bar_amt)) as gap
from stage_amount_to_allocate_rule_09 sadr 
inner join sgm_allocated_data_rule_09_p3 alloc on sadr.org_tranagg_id = alloc.org_tranagg_id
group by sadr.org_tranagg_id,
		sadr.bar_acct,
		sadr.bar_entity,
		sadr.fiscal_month_id,
		sadr.org_bar_custno,
		sadr.org_bar_product,
		sadr.mapped_bar_custno,
		sadr.mapped_bar_product,
		sadr.mapped_bar_brand,
		sadr.org_material,
		sadr.org_shiptocust,
		sadr.org_soldtocust,
		sadr.bar_currtype
order by abs(sum(alloc.allocated_amt)) - abs(avg(sadr.bar_amt)) desc
*/

		
	
		
----add rest all unallocated data 
drop table if exists sgm_allocated_data_rule_09_p5; 
create temporary table sgm_allocated_data_rule_09_p5
diststyle even 
sortkey (posting_week_enddate)
as 
	Select in_amt.source_system,
		  in_amt.org_tranagg_id,
		  in_amt.posting_week_enddate,
		  in_amt.fiscal_month_id,
		  in_amt.bar_entity,
		  in_amt.bar_acct,
		  in_amt.org_bar_brand,
		  in_amt.org_bar_custno,
		  in_amt.org_bar_product,
		  in_amt.mapped_bar_brand,
		  in_amt.mapped_bar_custno,
		  in_amt.mapped_bar_product,
		  in_amt.org_shiptocust,
		  in_amt.org_soldtocust,
		  in_amt.org_material,
		  'unknown' as alloc_shiptocust,
		  'unknown' as alloc_soldtocust,
		  'unknown' as alloc_material,
		  in_amt.mapped_bar_product as alloc_bar_product,
		  in_amt.bar_currtype,
		  2 as dataprocessing_outcome_id,
		  'phase 100' as dataprocessing_phase,
		  org_dataprocessing_ruleid,
		  mapped_dataprocessing_ruleid,
		  in_amt.bar_amt as allocated_amt
	From (
		Select distinct lower(bar_entity) as bar_entity,
					 lower(mapped_bar_product) mapped_bar_product, 
					 lower(mapped_bar_custno) mapped_bar_custno,
					 lower(bar_currtype) as bar_currtype,
					 lower(source_system) as source_system
		from stage_amount_to_allocate_rule_09 in_amt
		except 
		Select lower(bar_entity) as bar_entity,
			  lower(mapped_bar_product) mapped_bar_product, 
			  lower(mapped_bar_custno) mapped_bar_custno,
			  lower(bar_currtype) as bar_currtype,
			  lower(source_system) as source_system
		from 
		(
			Select distinct bar_entity,mapped_bar_product, mapped_bar_custno,bar_currtype, source_system
			from sgm_allocated_data_rule_09_p1
			union 
			Select distinct bar_entity,org_mapped_bar_product, mapped_bar_custno,bar_currtype,source_system
			from sgm_allocated_data_rule_09_p2
			union 
			Select distinct bar_entity,org_mapped_bar_product, mapped_bar_custno,bar_currtype,source_system
			from sgm_allocated_data_rule_09_p3
			union 
			Select distinct bar_entity,org_mapped_bar_product, mapped_bar_custno,bar_currtype,source_system
			from sgm_allocated_data_rule_09_p4
		) a
	) unalloc	
	inner join stage_amount_to_allocate_rule_09 in_amt on lower(unalloc.mapped_bar_custno) = lower(in_amt.mapped_bar_custno) 
		and lower(unalloc.mapped_bar_product) = lower(in_amt.mapped_bar_product)
		and lower(unalloc.bar_entity) = lower(in_amt.bar_entity)
		and lower(unalloc.bar_currtype) = lower(in_amt.bar_currtype)
		and lower(unalloc.source_system) = lower(in_amt.source_system);
 
delete from stage.sgm_allocated_data_rule_09 where fiscal_month_id = fmthid; 

--Select *
--from stage.sgm_allocated_data_rule_09 sadr 
--where org_tran_agg_id  = 4205712
	
INSERT INTO stage.sgm_allocated_data_rule_09
(
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
  dataprocessing_outcome_id,
  dataprocessing_phase,
  org_dataprocessing_ruleid,
  mapped_dataprocessing_ruleid,
  allocated_amt,
  audit_loadts
)
select *, cast(getdate() as timestamp) as audit_loadts
from (
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
	  dataprocessing_outcome_id,
	  dataprocessing_phase,
	  org_dataprocessing_ruleid,
	  mapped_dataprocessing_ruleid,
	  allocated_amt
From sgm_allocated_data_rule_09_p1
union all 
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
	  dataprocessing_outcome_id,
	  dataprocessing_phase,
	  org_dataprocessing_ruleid,
	  mapped_dataprocessing_ruleid,
	  allocated_amt
From sgm_allocated_data_rule_09_p2
union all
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
	  dataprocessing_outcome_id,
	  dataprocessing_phase,
	  org_dataprocessing_ruleid,
	  mapped_dataprocessing_ruleid,
	  allocated_amt
From sgm_allocated_data_rule_09_p3
union all 
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
	  dataprocessing_outcome_id,
	  dataprocessing_phase,
	  org_dataprocessing_ruleid,
	  mapped_dataprocessing_ruleid,
	  allocated_amt
From sgm_allocated_data_rule_09_p4
union all 
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
	  dataprocessing_outcome_id,
	  dataprocessing_phase,
	  org_dataprocessing_ruleid,
	  mapped_dataprocessing_ruleid,
	  allocated_amt
From sgm_allocated_data_rule_09_p5
);
	
/*
--perform gap analysis - after load 
Select sadr.org_tranagg_id,
		sadr.bar_acct,
		sadr.bar_entity,
		sadr.fiscal_month_id,
		sadr.org_bar_custno,
		sadr.org_bar_product,
		sadr.mapped_bar_custno,
		sadr.mapped_bar_product,
		sadr.org_material,
		sadr.org_shiptocust,
		sadr.org_soldtocust,
		sadr.bar_currtype,
		abs(avg(sadr.bar_amt)) as amt_to_allocate,
		abs(sum(alloc.allocated_amt)) as allocated_amt,
		abs(sum(alloc.allocated_amt)) - abs(avg(sadr.bar_amt)) as gap
from stage_amount_to_allocate_rule_09 sadr 
inner join stage.sgm_allocated_data_rule_09 alloc on sadr.org_tranagg_id = alloc.org_tranagg_id
group by sadr.org_tranagg_id,
		sadr.bar_acct,
		sadr.bar_entity,
		sadr.fiscal_month_id,
		sadr.org_bar_custno,
		sadr.org_bar_product,
		sadr.mapped_bar_custno,
		sadr.mapped_bar_product,
		sadr.org_material,
		sadr.org_shiptocust,
		sadr.org_soldtocust,
		sadr.bar_currtype
order by abs(sum(alloc.allocated_amt)) - abs(avg(sadr.bar_amt)) desc
*/	
 
  
exception
when others then raise info 'exception occur while ingesting data in stage.sgm_allocated_data_rule_09';
end;
$$
;