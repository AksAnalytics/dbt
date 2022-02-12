CREATE OR REPLACE PROCEDURE stage.deployment_testing()
 LANGUAGE plpgsql
AS $$
Begin
	
insert into stage.deployment_testing ( deployment_date)
 select getdate();

END
$$
;

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

CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_101(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECALRE Variables here
BEGIN 
	
/********************************************************************************************************************************
 PRE-CALCULATIONS 
 1. Build map_gpp_portfolio_to_supersbu MAPPING from bods product master 
 2. build fiscal month date range
 3. build hfm rates for fiscal month in process
 
 CALCULATIONS
 1. Get Target warranty costs by cost pools - PTG and Non-PTG 
     - BODS agg data has warranty costs at Super SBU (PTG, HTAS) level 
     - SBD has PTG estimates for C11 using SAP GL Account table - ref_data.PTG_accruals 
     - Calculate PTG and Non-PTG cost pool targerts
 2. GET 12 Month positive Invoice sales with positive cogs for the fiscal month - at SKU and CurrType grain
 3. GET warranty claims using set of 19 GL accounts from C11 Data 
 4. Calculate Claim Rate at SKU grain
 5. SKU's that have positive sales in the processing month, -ve cost (cogs) but no warranty claims : use avg claim rate - 
 	substtitue AVG CLAIM rate at Cost Pool grain
 6. Allocate Cost Pool - Accruals at SKU, Curr_Type grain
 7. For each SKU, calculate % of COGS (cost of sales) for each SKU x customer transactions (lowest level granularity) 
    using rate_base_cogs
     
 *  call stage.p_allocate_data_rule_101(202101)
 **********************************************************************************************************************************/	
	
	
	
/* mapping gpp portfolio to super-SBU */
	drop table if exists map_gpp_portfolio_to_supersbu
	;
	create temporary table map_gpp_portfolio_to_supersbu as 
	with
		cte_base as (
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
					case when bar_product is null then 'unknown' else bar_product end as portfolio,
					cast(generation as int) as generation,
					case when level4 is null then 'unknown' else level4 end as level04_bar,
					case when level7 is null then 'unknown' else level7 end as level07_bar
			from 	bods.drm_product_current
			where 	loaddts = ( select max(loaddts) from bods.drm_product_current dpc )
				and membertype != 'Parent'
		)
		select 	portfolio as gpp_portfolio,
				case when generation <= 4  then case when bar_product = 'Product_None' then bar_product else parent end else level04_bar end as super_sbu,
				case when generation <= 7  then case when bar_product = 'Product_None' then bar_product else parent end else level07_bar end as division
		from 	cte_base 
	;
	
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				max(dt.fmth_id) AS fiscal_month_id
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	/* create temp table for exchange_rate */
	drop table if exists vtbl_exchange_rate
	;
	create temporary table vtbl_exchange_rate as 
		select 	rt.fiscal_month_id, 
				rt.from_currtype,
				rt.fxrate
		from 	ref_data.hfmfxrates_current rt
				inner join vtbl_date_range dt
					on 	dt.fiscal_month_id = rt.fiscal_month_id 
		where 	lower(rt.to_currtype) = 'usd'
	;
drop table if exists stage_warranty_cost_pools;

create temporary table stage_warranty_cost_pools
diststyle all
as 
with warranty_cost_pnl as (
Select 	sum(bar_amt_usd)  as warranty_cost_usd, 
		sum(bar_amt) as warranty_cost, 
		bar_currtype,
		ctda.fiscal_month_id,
		fmth.posting_week_enddate
from stage.bods_core_transaction_agg_agm ctda 
	left join ref_data.hfmfxrates_current hc on ctda.fiscal_month_id = hc.fiscal_month_id and lower(ctda.bar_currtype) = lower(hc.from_currtype)
	inner join ref_data.pnl_acct_agm paa on ctda.bar_acct = paa.bar_acct 
	inner join ref_data.data_processing_rule_agm dpra on dpra.data_processing_ruleid = ctda.dataprocessing_ruleid
	cross join vtbl_date_range dt 
	inner join (select max(wk_end_dte) as posting_week_enddate, 
					fmth_id 
			  from ref_data.calendar c 
			  cross join vtbl_date_range dt 
			  Where fmth_id = dt.fiscal_month_id 
			  group by fmth_id) fmth on ctda.fiscal_month_id = fmth.fmth_id
where 1=1
	and dpra.data_processing_ruleid = 101
	and ctda.fiscal_month_id = dt.fiscal_month_id
group by ctda.fiscal_month_id,bar_currtype,fmth.posting_week_enddate
), ptg_accruals as (
Select 	sum(amt_usd)*-1 as ptg_accruals_usd, 
		sum(isnull(amt,0))*-1 as ptg_accruals, 
		currkey  as currtype,
		pa.fiscal_month_id,
		posting_week_enddate
from ref_data.ptg_accruals pa 
cross join vtbl_date_range dt 
where pa.fiscal_month_id = dt.fiscal_month_id
group by currkey,pa.fiscal_month_id,posting_week_enddate
) Select  a.bar_currtype,
		isnull(b.ptg_accruals,0) accruals,
		cast('PTG' as varchar(10)) as cost_pool,
		a.fiscal_month_id,
		a.posting_week_enddate
from warranty_cost_pnl a
left join ptg_accruals b on a.bar_currtype = b.currtype and a.posting_week_enddate = b.posting_week_enddate
union all 
-----warranty_cost is negative and b.ptg_accruals is positive hence adding them here
select  a.bar_currtype,
		a.warranty_cost - isnull(b.ptg_accruals,0) as accruals,
		cast('Non-PTG' as varchar(10)) as cost_pool,
		a.fiscal_month_id,
		a.posting_week_enddate
from warranty_cost_pnl a
left join ptg_accruals b on a.bar_currtype = b.currtype and a.posting_week_enddate = b.posting_week_enddate;
--
--
--Select *
--from stage_warranty_cost_pools;
--
--Select *
--from vltb_fmth_range

drop table if exists vltb_fmth_range;
create temporary table vltb_fmth_range
as 
Select min(fmth_id) as start_fiscal_month, max(fmth_id) as end_fiscal_month
from (
	Select ROW_NUMBER () over (order by fmth_id desc) as rownumber,  fmth_id
		from (
		Select distinct fmth_id
		from ref_data.calendar c 
		cross join vtbl_date_range dt 
		where fmth_id <= dt.fiscal_month_id
	)a 
)b Where rownumber = 1 or  rownumber=13; 
/* Get previous 12 months sales for SKU's from current current processing month 
   with +ve sales and positive cogs
 */
drop table if exists stage_sales_by_sku;
create temporary table stage_sales_by_sku 
as 
Select sum(amt) as invoice_sales, dp.material, fpcs.bar_currtype 
from dw.fact_pnl_commercial_stacked fpcs 
inner join dw.dim_product dp on fpcs.product_key = dp.product_key 
inner join ( select sum(total_bar_amt) as total_bar_amt, material,bar_currtype
			from stage.rate_base_cogs rb 
			cross join vtbl_date_range dt 
			where rb.fiscal_month_id = dt.fiscal_month_id 
		group by material,bar_currtype
		having sum(total_bar_amt) < 0
		) rb on dp.material = rb.material and fpcs.bar_currtype = rb.bar_currtype
cross join  vltb_fmth_range vfr 
where fpcs.fiscal_month_id between start_fiscal_month and end_fiscal_month
and bar_acct  in ('A40110')
---and lower(material) = '00 20 06 us2'
group by dp.material,fpcs.bar_currtype
having sum(amt) > 0; 



---Select count(1) from stage_sales_by_sku;
-------------step 2 : 12 months claims data
drop table if exists stage_warranty_claims;
create temporary table stage_warranty_claims
as 
Select 	period,
		acct,	
		costctr,	
		bar_acct,	
		bar_entity,	
		bar_custno,
		bar_product,
		bar_bu,
		bar_brand,	
		material,	
		bar_currtype,
		sum(amt) as warranty_amt, 
		sum(amt*isnull(fxrate,1)) as warranty_amt_usd
from bods.c11_0ec_pca3_current s
left join ref_data.calendar dd on cast((case when s.postdate = '' then null else postdate end) as date) = cast(dd.dy_dte as date)
left join ref_data.hfmfxrates_current hc on dd.fmth_id = hc.fiscal_month_id and lower(s.currkey) = lower(hc.from_currtype)
cross join  vltb_fmth_range vfr 
where dd.fmth_id between start_fiscal_month and end_fiscal_month
--and s.material = 'BDFC240'
and acct in (
'0005757000',
'0005757002',
'0005757220',
'0005757140',
'0005757221',
'0005757224',
'0005757222',
'0005757010',
'0005757013',
'0005757014',
'0005757020',
'0005757022',
'0005757023',
'0005757030',
'0005757031',
'0005757035',
'0005768290',
'0005768301',
'0005768300',
'0005757170',
'0005757210',
'0005757212',
'0005757225',
'0005774490',
'0005774330',
'0005776660',
'0005757211')
group by period,
		acct,	
		costctr,	
		bar_acct,	
		bar_entity,	
		bar_custno,
		bar_product,
		bar_bu,
		bar_brand,	
		material,	
		bar_currtype;
--select count(1) from stage_sales_by_sku
---Select count(distinct material) from stage_warranty_claims; 
	
--Select *
--from stage_warranty_claims
--where material = 'DCK675D2';
	
	
	
drop table if exists _dim_prod_sku_to_super_sbu_map
;
create temporary table _dim_prod_sku_to_super_sbu_map as
with
cte_base as (
		select dp.material,
		dp.level04_bar as super_sbu,
		sum(f.amt_usd) as amt_usd
		from dw.fact_pnl_commercial_stacked f
		inner join dw.dim_product dp on dp.product_key = f.product_key
		where f.bar_acct = 'A40110' and
		lower(dp.level04_bar) != 'unknown'
		group by dp.material,
		dp.level04_bar
		),
cte_rnk as (
	select base.material,base.super_sbu,base.amt_usd,
		rank() over(partition by material order by amt_usd desc) as rnk
	from cte_base as base
	)
select rnk.material,
rnk.super_sbu
from cte_rnk as rnk
where rnk.rnk = 1;		
	
	
drop table if exists stage_base_claim_rate_by_sku;
---filter out any sku's without invoice sales
---use mapping table Bill is building to map sku to portfolio
---c.warranty_amt < s.invoice_sales : handling edge cases where warranty amount is greater than invoice sales: they will be defualted to AVG claim rate calc
create temporary table stage_base_claim_rate_by_sku
as 
Select 	c.material ,
		c.warranty_amt,
		s.invoice_sales, 
		case when s.invoice_sales = 0 then 0 else (cast(c.warranty_amt as numeric(19,8))/cast(s.invoice_sales as numeric(19,8))) end as claim_rate,
		case when lower(map_gpp.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
		c.bar_currtype
from (
	Select cl.bar_currtype,
		  cl.material, 
		  sum(warranty_amt)	warranty_amt
	from stage_warranty_claims cl
	inner join ( select sum(total_bar_amt) as total_bar_amt, rb.material,rb.bar_currtype
			from stage.rate_base_cogs rb 
			cross join vtbl_date_range dt 
			where rb.fiscal_month_id = dt.fiscal_month_id 
		group by rb.material,rb.bar_currtype
		having sum(total_bar_amt) <0
		) rb on cl.material = rb.material and cl.bar_currtype = rb.bar_currtype
	group by cl.bar_currtype,
		  cl.material
	) c 
cross join vtbl_date_range dt 
inner join stage_sales_by_sku s on c.material = s.material and c.bar_currtype = s.bar_currtype
inner join _dim_prod_sku_to_super_sbu_map map_gpp on lower(map_gpp.material) = lower(c.material) 
where c.warranty_amt < s.invoice_sales; 

/*************************Edge case : warranty_amt >= invoice sales ************************************************/ 
drop table if exists stage_base_claim_rate_by_sku_edge_cases;
---filter out any sku's without invoice sales
---use mapping table Bill is building to map sku to portfolio
---c.warranty_amt < s.invoice_sales : handling edge cases where warranty amount is greater than invoice sales: they will be defualted to AVG claim rate calc
create temporary table stage_base_claim_rate_by_sku_edge_cases
as 
Select 	c.material ,
		c.warranty_amt,
		s.invoice_sales, 
		---claim rate should be avg claim rate at cost pool level
		cast(NULL as numeric(19,8)) claim_rate,
		case when lower(map_gpp.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
		c.bar_currtype
from (
	Select cl.bar_currtype,
		  cl.material, 
		  sum(warranty_amt)	warranty_amt
	from stage_warranty_claims cl
	inner join ( select sum(total_bar_amt) as total_bar_amt, rb.material,rb.bar_currtype
			from stage.rate_base_cogs rb 
			cross join vtbl_date_range dt 
			where rb.fiscal_month_id = dt.fiscal_month_id 
		group by rb.material,rb.bar_currtype
		having sum(total_bar_amt) <0
		) rb on cl.material = rb.material and cl.bar_currtype = rb.bar_currtype
	group by cl.bar_currtype,
		  cl.material
	) c 
cross join vtbl_date_range dt 
inner join stage_sales_by_sku s on c.material = s.material and c.bar_currtype = s.bar_currtype
inner join _dim_prod_sku_to_super_sbu_map map_gpp on lower(map_gpp.material) = lower(c.material) 
where c.warranty_amt >= s.invoice_sales; 

	
drop table if exists stage_warranty_by_cost_pool;
create temporary table stage_warranty_by_cost_pool
as
Select 	  case when lower(map_gpp.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
	 	  cl.bar_currtype,
		  sum(warranty_amt)	warranty_amt
	from stage_warranty_claims cl
	inner join ( select sum(total_bar_amt) as total_bar_amt, rb.material,rb.bar_currtype
			from stage.rate_base_cogs rb 
			cross join vtbl_date_range dt 
			where rb.fiscal_month_id = dt.fiscal_month_id 
		group by rb.material,rb.bar_currtype
		having sum(total_bar_amt) <0
		) rb on cl.material = rb.material and cl.bar_currtype = rb.bar_currtype
	inner join  _dim_prod_sku_to_super_sbu_map map_gpp on lower(map_gpp.material) = lower(cl.material)
	group by cl.bar_currtype,
		  case when lower(map_gpp.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end;
		 
  

--Select *
--from stage_base_claim_rate_by_sku
--where material in ('J556627-10SG')
--select count(1) from stage_base_claim_rate_by_sku ;

---get average warranty cost by super SBU 
drop table if exists stage_avg_claim_rate_by_costpool;
create temporary table stage_avg_claim_rate_by_costpool
as 
Select cr.cost_pool, cr.bar_currtype,
	sum(invoice_sales) as invoice_sales, 
	cp.warranty_amt,
	case when cr.cost_pool = 'PTG' and sum(invoice_sales) <> 0 then  cast(cp.warranty_amt as numeric(19,8)) / cast(sum(invoice_sales) as numeric(19,8)) else null end as avg_claim_rate_ptg, 
	case when cr.cost_pool = 'Non-PTG' and sum(invoice_sales) <> 0 then cast(cp.warranty_amt as numeric(19,8))/cast(sum(invoice_sales) as numeric(19,8)) else null end as avg_claim_rate_non_ptg
from stage_base_claim_rate_by_sku cr 
inner join stage_warranty_by_cost_pool cp on cr.bar_currtype = cp.bar_currtype and cr.cost_pool=cp.cost_pool
where 1=1
group by cr.cost_pool,cr.bar_currtype,cp.warranty_amt;
--Select *
--from stage_avg_claim_rate_by_costpool;
----SKU's that have positive sales in the processing month, -ve cost but no warranty claims : use avg claim rate
drop table if exists stage_sku_postive_sales_withno_claims;
create temporary table stage_sku_postive_sales_withno_claims
as 
Select case when lower(map_gpp.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
	  fpcs.alloc_material as material,
	  fpcs.bar_currtype, 
	  sum(amt) as invoice_sales
--select count(distinct fpcs.alloc_material),sum(amt) as invoice_sales
from dw.fact_pnl_commercial_stacked fpcs 
inner join ( select sum(total_bar_amt) as total_bar_amt, rb.material,rb.bar_currtype
			from stage.rate_base_cogs rb 
			cross join vtbl_date_range dt 
			where rb.fiscal_month_id = dt.fiscal_month_id 
		group by rb.material,rb.bar_currtype
		having sum(total_bar_amt) < 0   ---negative cogs
		) rb on fpcs.alloc_material = rb.material and fpcs.bar_currtype = rb.bar_currtype
cross join vtbl_date_range dt 
inner join _dim_prod_sku_to_super_sbu_map map_gpp on lower(map_gpp.material) = lower(fpcs.alloc_material)
where fpcs.bar_acct = 'A40110'
and fpcs.amt>0 
and fpcs.fiscal_month_id = dt.fiscal_month_id 
--and fpcs.alloc_material ='CMST24800RB'
and not exists (select 1 from stage_warranty_claims cl 
			 where fpcs.alloc_material = cl.material 
			 and fpcs.bar_currtype = cl.bar_currtype)
group by 	case when lower(map_gpp.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end,
		fpcs.alloc_material, 
		fpcs.bar_currtype;
--select count(1) from stage_sku_postive_sales_withno_claims
	
--Select *
--from stage_sku_postive_sales_withno_claims 
--where material = 'DCK675D2';
--	
--select *
--from stage_base_claim_rate_by_sku
--where material = 'DCK675D2';	
	
insert into stage_base_claim_rate_by_sku
Select sc.material ,
	0 as warranty_amt,
	sc.invoice_sales, 
	case when sc.cost_pool = 'PTG' then cp.avg_claim_rate_ptg else cp.avg_claim_rate_non_ptg end as claim_rate,
	sc.cost_pool,
	sc.bar_currtype
from stage_sku_postive_sales_withno_claims sc
inner join stage_avg_claim_rate_by_costpool cp on sc.cost_pool = cp.cost_pool and sc.bar_currtype = cp.bar_currtype
left join stage_base_claim_rate_by_sku t on sc.material = t.material and sc.bar_currtype = t.bar_currtype 
where 1=1 
--and cp.accruals!=0 
and t.material is null ;
------edge case : sku's
insert into stage_base_claim_rate_by_sku
Select sc.material ,
	sc.warranty_amt,
	sc.invoice_sales, 
	case when sc.cost_pool = 'PTG' then cp.avg_claim_rate_ptg else cp.avg_claim_rate_non_ptg end as claim_rate,
	sc.cost_pool,
	sc.bar_currtype
from stage_base_claim_rate_by_sku_edge_cases sc
inner join stage_avg_claim_rate_by_costpool cp on sc.cost_pool = cp.cost_pool and sc.bar_currtype = cp.bar_currtype;



--select count(1) from stage_sku_postive_sales_withno_claims;	
---Select count(1) from stage_base_claim_rate_by_sku

--select *
--from stage_avg_claim_rate_by_costpool;

--limit sku which are sold in same month 
drop table if exists stage_sales_by_sku_for_processing_month;
create temporary table stage_sales_by_sku_for_processing_month 
as 
Select sum(amt) as invoice_sales_pm, dp.material, fpcs.bar_currtype 
from dw.fact_pnl_commercial_stacked fpcs 
inner join dw.dim_product dp on fpcs.product_key = dp.product_key 
inner join ( select sum(total_bar_amt) as total_bar_amt, material,bar_currtype
			from stage.rate_base_cogs rb 
			cross join vtbl_date_range dt 
			where rb.fiscal_month_id = dt.fiscal_month_id 
		group by material,bar_currtype
		having sum(total_bar_amt) < 0
		) rb on dp.material = rb.material and fpcs.bar_currtype = rb.bar_currtype
cross join vtbl_date_range dt 
where fpcs.fiscal_month_id = dt.fiscal_month_id 
and bar_acct  in ('A40110')
---and lower(material) = '00 20 06 us2'
group by dp.material,fpcs.bar_currtype
having sum(amt) > 0; 
---Select count(1), count(distinct material) from stage_sales_by_sku_for_processing_month;

drop table if exists stage_warranty_cost_allocated_amt;
create temporary table stage_warranty_cost_allocated_amt 
distkey(material)
as
--Select sum(allocated_amt), sum(allocation_rate), cost_pool,bar_currtype
--from (
Select 	wa.fiscal_month_id, 
		wa.posting_week_enddate,
		material,
		warranty_amt,
		invoice_sales,
		a.cost_pool,
		a.bar_currtype,
		total_sales_bysbu,
		avg_claim_rate, 
		cast((isnull(invoice_sales_pm,0)*avg_claim_rate) as numeric(19,8)) / 
			cast(sum(isnull(invoice_sales_pm,0)*avg_claim_rate) over (partition by a.bar_currtype,a.cost_pool) as numeric (19,8)) as allocation_rate,
		cast((isnull(invoice_sales_pm,0)*avg_claim_rate) as numeric(19,8)) / 
			cast(sum(isnull(invoice_sales_pm,0)*avg_claim_rate) over (partition by a.bar_currtype,a.cost_pool) as numeric (19,8))*accruals as allocated_amt
from (
	Select 	clr.bar_currtype,
			clr.material, 
			clr.warranty_amt,
			clr.invoice_sales,
			clr.cost_pool, 
			cls.invoice_sales as total_sales_bysbu,
			isnull(sspm.invoice_sales_pm,0) as invoice_sales_pm,
			coalesce(clr.claim_rate,avg_claim_rate_ptg,avg_claim_rate_non_ptg) as avg_claim_rate
	from stage_base_claim_rate_by_sku clr
	left join stage_avg_claim_rate_by_costpool cls on clr.cost_pool = cls.cost_pool and clr.bar_currtype = cls.bar_currtype
	left join stage_sales_by_sku_for_processing_month sspm on clr.material = sspm.material and clr.bar_currtype = sspm.bar_currtype
	) a
	left join stage_warranty_cost_pools wa on a.cost_pool = wa.cost_pool and a.bar_currtype = wa.bar_currtype
--)group by cost_pool, bar_currtype;
;

delete from stage.warranty_cost_allocated_amt_101_transient 
using vtbl_date_range dt
where warranty_cost_allocated_amt_101_transient.fiscal_month_id = dt.fiscal_month_id;

insert into stage.warranty_cost_allocated_amt_101_transient
select * from stage_warranty_cost_allocated_amt;

---Select count(1), count(distinct material) from stage_warranty_cost_allocated_amt
---select count(1) from rate_base_cogs_pct_of_total;
/* rate table based on standard cost */
	drop table if exists rate_base_cogs_pct_of_total;
	create temporary table rate_base_cogs_pct_of_total as 
		with
			cte_rate_base_cogs as (
				select 	rb.fiscal_month_id,
						rb.bar_entity,
						rb.soldtocust,
						rb.shiptocust,
						rb.bar_custno,
						rb.material,
						rb.bar_product,
						rb.bar_brand,
						rb.super_sbu,
						rb.cost_pool,
						rb.total_bar_amt,
						rb.bar_currtype,
						sum(rb.total_bar_amt) over( partition by rb.fiscal_month_id, rb.cost_pool,rb.material, rb.bar_currtype ) as total_bar_amt_partition
				from 	stage.rate_base_cogs rb
				cross join vtbl_date_range dt
				inner join stage_warranty_cost_allocated_amt wc 
						on rb.material = wc.material and rb.cost_pool = wc.cost_pool and 
						   rb.bar_currtype = wc.bar_currtype
				where rb.fiscal_month_id = dt.fiscal_month_id
			)
		select 	cte_rb.fiscal_month_id,
				cte_rb.bar_entity,
				cte_rb.soldtocust,
				cte_rb.shiptocust,
				cte_rb.bar_custno,
				cte_rb.material,
				cte_rb.bar_product,
				cte_rb.bar_brand,
				cte_rb.super_sbu,
				cte_rb.total_bar_amt,
				cte_rb.total_bar_amt_partition,
				cte_rb.cost_pool,
				cte_rb.bar_currtype,
				CAST(cte_rb.total_bar_amt as decimal(20,8))
					/ CAST(cte_rb.total_bar_amt_partition as decimal(20,8)) as pct_of_total
		from 	cte_rate_base_cogs cte_rb
		where 	total_bar_amt_partition != 0
	;

--
--
--Select *
--from rate_base_cogs_pct_of_total rb
--where not exists (select 1 from stage_warranty_cost_allocated_amt ca
--			where rb.material = ca.material 
--			and rb.bar_currtype = ca.bar_currtype
--			and rb.cost_pool = ca.cost_pool 
--			)

--Select avg(total_bar_amt), cast(sum(pct_of_total) as numeric(19,12)), material, cost_pool, bar_currtype
--from rate_base_cogs_pct_of_total
--------where material = '59100CD'
--group by material, cost_pool, bar_currtype
--having cast(sum(pct_of_total) as numeric(19,12)) <1
--order by 2 asc;
--
--Select *
--from stage_warranty_cost_allocated_amt
--where material = '59100CD'

--Select sum(total_bar_amt) as total_cogs, cost_pool, bar_currtype, sum(total_bar_amt_usd)
--from stage.rate_base_cogs
--group by cost_pool, bar_currtype


/* use division method to avoid multiplication overflow error - SELECT CAST(2 AS DECIMAL(38, 19)) / (1 / CAST(2 AS DECIMAL(38, 19)))
 * https://matthewrwilton.wordpress.com/2016/08/11/avoiding-numeric-overflows-in-redshift-decimal-multiplication/
 */
delete from stage.agm_allocated_data_rule_101 
using vtbl_date_range dt
where agm_allocated_data_rule_101.fiscal_month_id = dt.fiscal_month_id and dataprocessing_phase = 'phase 21';
INSERT INTO stage.agm_allocated_data_rule_101
(
  source_system,
  fiscal_month_id,
  posting_week_enddate,
  bar_entity,
  bar_acct,
  material,
  bar_product,
  bar_brand,
  soldtocust,
  shiptocust,
  bar_custno,
  dataprocessing_ruleid,
  dataprocessing_outcome_id,
  dataprocessing_phase,
  bar_currtype,
  super_sbu,
  cost_pool,
  allocated_amt,
  allocated_amt_usd,
  audit_loadts
)
--Select stg.bar_currtype, stg.cost_pool, sum(allocated_amt), count(1)
--from (
SELECT cast('adj-wa-tran' as varchar(20)) as source_system,
       wc.fiscal_month_id,
       wc.posting_week_enddate,
       stg.bar_entity,
       cast('AGM-ADJ-WA' as varchar(20)) as bar_acct,
       stg.material,
       stg.bar_product,
       COALESCE(stg.bar_brand, 'unknown') as bar_brand,
       stg.soldtocust,
       stg.shiptocust,
       stg.bar_custno,
       cast(101 as integer) dataprocessing_ruleid,
       cast(1 as integer) dataprocessing_outcome_id,
       cast('phase 21' as varchar(10)) as dataprocessing_phase,
       wc.bar_currtype,
       stg.super_sbu,
       wc.cost_pool,
       cast(wc.allocated_amt as numeric(38,12)) / (1 / cast(pct_of_total as numeric(38,12))) allocated_amt,
       case when fx.from_currtype is null then cast(wc.allocated_amt as numeric(38,12)) / (1 / cast(pct_of_total as numeric(38,12)))
       	  else CAST(fx.fxrate as decimal(38,8))*cast(wc.allocated_amt as numeric(38,12)) / (1 / cast(pct_of_total as numeric(38,12)))	 
       end as allocated_amt_usd,
       cast(getdate() as timestamp) as audit_loadts
FROM rate_base_cogs_pct_of_total stg 
cross join vtbl_date_range dt 
inner join stage_warranty_cost_allocated_amt wc 
						on stg.material = wc.material and stg.cost_pool = wc.cost_pool and 
						   stg.bar_currtype = wc.bar_currtype
left outer join vtbl_exchange_rate as fx
					on 	fx.fiscal_month_id = stg.fiscal_month_id and 
						lower(fx.from_currtype) = lower(stg.bar_currtype)
where cast(pct_of_total as numeric(38,12)) !=0
--left outer join ref_data.sku_barbrand_mapping_sgm as map_bb
--					on 	lower(map_bb.material) = lower(stg.material) and 
--						map_bb.ss_fiscal_month_id = dt.fiscal_month_id 
--) stg 
--group by stg.bar_currtype, stg.cost_pool
					;
			
/*					
Select *
from stage_warranty_cost_allocated_amt wc   
left join rate_base_cogs_pct_of_total stg
						on stg.material = wc.material and stg.cost_pool = wc.cost_pool and 
						   stg.bar_currtype = wc.bar_currtype
where stg.material is null
order by wc.allocated_amt desc
										
*/																				
--	
--Select *
--from stage.rate_base_cogs
--where material = 'CMCST920M1'
--and fiscal_month_id = fmthid
--order by bar_currtype;
					
					
					
/* _gap analysis of final allocations and cost pools
  Select s.*, t.allocated_amt, 
 	   s.accruals - t.allocated_amt as _gap, 
 	   (s.accruals - t.allocated_amt) / cast(s.accruals as numeric(19,8)) *100 as _percent_gap_allocation
 from stage_warranty_cost_pools s
 left join (Select sum(allocated_amt) as allocated_amt, cost_pool,bar_currtype,sum(allocated_amt_usd), count(1) as rec_cnt 
		  from stage.agm_allocated_data_rule_101
		  where fiscal_month_id = 202009
		  and dataprocessing_phase = 'phase 21'
		  group by cost_pool,bar_currtype) t on s.cost_pool = t.cost_pool and  s.bar_currtype = t.bar_currtype 
 */	

--additional queries to validate
--Select sum(_gap),sum(tobe_allocated),sum(allocated_amt),cost_pool,bar_currtype
--from (
--Select avg(allocated_amt) tobe_allocated,
--	  sum(cast(wc.allocated_amt as numeric(38,12)) / (1 / cast(pct_of_total as numeric(38,12)))) as allocated_amt, 
--	  abs(avg(allocated_amt)) - abs (sum(cast(wc.allocated_amt as numeric(38,12)) / (1 / cast(pct_of_total as numeric(38,12))))) as _gap,
--	  stg.material,stg.cost_pool, stg.bar_currtype
--from stage_warranty_cost_allocated_amt wc   
--left join rate_base_cogs_pct_of_total stg
--						on stg.material = wc.material and stg.cost_pool = wc.cost_pool and 
--						   stg.bar_currtype = wc.bar_currtype
--where stg.material is null
--group by stg.material,stg.cost_pool, stg.bar_currtype
--) group by cost_pool,bar_currtype;

  
exception
when others then raise info 'exception occur while ingesting data in stage.agm_allocated_data_rule_101';
end;
$$
;

CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_101_bnr_gap(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECALRE Variables here
BEGIN 
	
/*******************************************************************************************************************************
 
 1. Get Allocated Warranty Cost after first level of allocation at cost pool grain 
 2. Get B&R -financials at cost pool grain 
 3. findout the gap 
 4. allocate gap using % of cogs
  
 ************************************************************************************************************************************/

/* mapping gpp portfolio to super-SBU */
	drop table if exists map_gpp_portfolio_to_supersbu
	;
	create temporary table map_gpp_portfolio_to_supersbu as 
	with
		cte_base as (
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
					case when bar_product is null then 'unknown' else bar_product end as portfolio,
					cast(generation as int) as generation,
					case when level4 is null then 'unknown' else level4 end as level04_bar,
					case when level7 is null then 'unknown' else level7 end as level07_bar
			from 	bods.drm_product_current
			where 	loaddts = ( select max(loaddts) from bods.drm_product_current dpc )
				and membertype != 'Parent'
		)
		select 	portfolio as gpp_portfolio,
				case when generation <= 4  then case when bar_product = 'Product_None' then bar_product else parent end else level04_bar end as super_sbu,
				case when generation <= 7  then case when bar_product = 'Product_None' then bar_product else parent end else level07_bar end as division
		from 	cte_base 
	;
	
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				max(dt.fmth_id) AS fiscal_month_id
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	/* create temp table for exchange_rate */
	drop table if exists vtbl_exchange_rate
	;
	create temporary table vtbl_exchange_rate as 
		select 	rt.fiscal_month_id, 
				rt.from_currtype,
				rt.fxrate
		from 	ref_data.hfmfxrates_current rt
				inner join vtbl_date_range dt
					on 	dt.fiscal_month_id = rt.fiscal_month_id 
		where 	lower(rt.to_currtype) = 'usd'
	;	

	
---get cost pool distribution based on previous allocation 
drop table if exists bnr_gap_to_allocate_for_wc;
create temporary table bnr_gap_to_allocate_for_wc
as 
with bnr_reported_cost as 
(
	Select sum(amt_reported) amt_reported, acct_category, sum(amt_local_cur) as amt_reported_local
	from (
		Select sum(amt_local_cur*paa.multiplication_factor) amt_local_cur,
			  sum(amt_reported*paa.multiplication_factor) amt_reported,
			  account,acct_category
		from ref_data.agm_bnr_financials_extract abfe 
		inner join (select distinct name, level4 from ref_data.entity) rbh on abfe.entity = rbh.name
		inner join ref_data.pnl_acct_agm paa on abfe.account = paa.bar_acct 
		cross join vtbl_date_range dt 
		where abfe.fiscal_month_id = dt.fiscal_month_id
			and rbh.level4 = 'GTS_NA'
			and scenario = 'Actual_Ledger'
			and acct_category = 'Reported Warranty Cost'
		group by account,acct_category
	) group by acct_category	
)
SELECT    cost_pool,
			bar_currtype ,
			total_amt_usd,
			aadr.fiscal_month_id, 
			posting_week_enddate,
			cast(isnull(hc.fxrate,1) as numeric(9,5)) as fxrate,
		     sum(allocated_amt_usd) as allocated_amt, 
		     cast(sum(allocated_amt_usd) as numeric(19,8)) / cast(total_amt_usd as numeric(19,8)) as pct_of_total,
		     (total_amt_usd- amt_reported)*-1 as _gap,
		     (total_amt_usd- amt_reported)*-1*cast(sum(allocated_amt_usd) as numeric(19,8)) / cast(total_amt_usd as numeric(19,8))* (1/cast(isnull(hc.fxrate,1) as numeric(9,5))) as _gap_to_allocate
	FROM stage.agm_allocated_data_rule_101 aadr
	cross join (select sum(allocated_amt_usd) as total_amt_usd 
			   from stage.agm_allocated_data_rule_101 a
			   cross join vtbl_date_range dt 
			   where a.fiscal_month_id = dt.fiscal_month_id
			   and dataprocessing_phase='phase 21')	
	cross join (Select amt_reported from bnr_reported_cost) bnr 
	left join ref_data.hfmfxrates_current hc on aadr.fiscal_month_id = hc.fiscal_month_id and lower(aadr.bar_currtype) = lower(hc.from_currtype)
	cross join vtbl_date_range dt 
	where aadr.fiscal_month_id = dt.fiscal_month_id
	and dataprocessing_phase='phase 21'  --in previous phase 
	and dataprocessing_outcome_id =1 --allocated
	group by aadr.fiscal_month_id,posting_week_enddate,cost_pool,bar_currtype,total_amt_usd,amt_reported,isnull(hc.fxrate,1);
 
	/* +ve sales with negative cogs for processing month */ 
	drop table if exists stage_sales_by_sku;
	create temporary table stage_sales_by_sku 
	as 
	Select sum(amt) as invoice_sales, dp.material, fpcs.bar_currtype 
	from dw.fact_pnl_commercial_stacked fpcs 
	inner join dw.dim_product dp on fpcs.product_key = dp.product_key 
	inner join ( select sum(total_bar_amt) as total_bar_amt, material,bar_currtype
				from stage.rate_base_cogs rb 
				cross join vtbl_date_range dt 
				where rb.fiscal_month_id = dt.fiscal_month_id 
			group by material,bar_currtype
			having sum(total_bar_amt) < 0
			) rb on dp.material = rb.material and fpcs.bar_currtype = rb.bar_currtype
	cross join  vtbl_date_range dt 
	where fpcs.fiscal_month_id = dt.fiscal_month_id 
	and bar_acct  in ('A40110')
	---and lower(material) = '00 20 06 us2'
	group by dp.material,fpcs.bar_currtype
	having sum(amt) > 0;
/* rate table based on standard cost */
	drop table if exists rate_base_cogs_pct_of_total;
	create temporary table rate_base_cogs_pct_of_total as 
		with
			cte_rate_base_cogs as (
				select 	rb.fiscal_month_id,
						rb.bar_entity,
						rb.soldtocust,
						rb.shiptocust,
						rb.bar_custno,
						rb.material,
						rb.bar_product,
						rb.bar_brand,
						rb.super_sbu,
						rb.cost_pool,
						rb.total_bar_amt,
						rb.bar_currtype,
						sum(rb.total_bar_amt) over( partition by rb.fiscal_month_id, rb.cost_pool, rb.bar_currtype ) as total_bar_amt_partition
				from 	stage.rate_base_cogs rb
				cross join vtbl_date_range dt 
				inner join stage_sales_by_sku s on rb.material = s.material and rb.bar_currtype = s.bar_currtype
				where rb.fiscal_month_id = dt.fiscal_month_id
			)
		select 	cte_rb.fiscal_month_id,
				cte_rb.bar_entity,
				cte_rb.soldtocust,
				cte_rb.shiptocust,
				cte_rb.bar_custno,
				cte_rb.material,
				cte_rb.bar_product,
				cte_rb.bar_brand,
				cte_rb.super_sbu,
				cte_rb.total_bar_amt,
				cte_rb.total_bar_amt_partition,
				cte_rb.cost_pool,
				cte_rb.bar_currtype,
				CAST(cte_rb.total_bar_amt as decimal(20,8))
					/ CAST(cte_rb.total_bar_amt_partition as decimal(20,8)) as pct_of_total
		from 	cte_rate_base_cogs cte_rb
		where 	total_bar_amt_partition != 0
	;

/*********************validate 
 
 Select avg(total_bar_amt), cast(sum(pct_of_total) as numeric(19,12)),  cost_pool, bar_currtype, count(1)
from rate_base_cogs_pct_of_total
--------where material = '59100CD'
group by  cost_pool, bar_currtype
 
 */
--delete from stage.agm_allocated_data_rule_101 
--where dataprocessing_phase = 'phase 22';
delete from stage.agm_allocated_data_rule_101 
using vtbl_date_range dt
where agm_allocated_data_rule_101.fiscal_month_id = dt.fiscal_month_id  and dataprocessing_phase = 'phase 91';
INSERT INTO stage.agm_allocated_data_rule_101
(
  source_system,
  fiscal_month_id,
  posting_week_enddate,
  bar_entity,
  bar_acct,
  material,
  bar_product,
  bar_brand,
  soldtocust,
  shiptocust,
  bar_custno,
  dataprocessing_ruleid,
  dataprocessing_outcome_id,
  dataprocessing_phase,
  bar_currtype,
  super_sbu,
  cost_pool,
  allocated_amt,
  allocated_amt_usd,
  audit_loadts
)
--Select stg.bar_currtype, stg.cost_pool, sum(allocated_amt), count(1)
--from (
SELECT cast('adj-wa-tran-gap' as varchar(20)) as source_system,
       wc.fiscal_month_id,
       wc.posting_week_enddate,
       stg.bar_entity,
       cast('AGM-ADJ-WA' as varchar(20)) as bar_acct,
       stg.material,
       stg.bar_product,
       COALESCE( stg.bar_brand, 'unknown') as bar_brand,
       stg.soldtocust,
       stg.shiptocust,
       stg.bar_custno,
       cast(101 as integer) dataprocessing_ruleid,
       cast(1 as integer) dataprocessing_outcome_id,
       cast('phase 91' as varchar(10)) as dataprocessing_phase,
       wc.bar_currtype,
       stg.super_sbu,
       wc.cost_pool,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 then 0 else 
       cast(wc._gap_to_allocate as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12))) end allocated_amt,
	   case when fx.from_currtype is null then cast(wc._gap_to_allocate as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12)))
		       	  else CAST(fx.fxrate as decimal(38,8))*cast(wc._gap_to_allocate as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12)))	 
		       end as allocated_amt_usd,
       cast(getdate() as timestamp) as audit_loadts
FROM rate_base_cogs_pct_of_total stg 
cross join vtbl_date_range dt 
inner join bnr_gap_to_allocate_for_wc wc 
						on  stg.cost_pool = wc.cost_pool and 
						   stg.bar_currtype = wc.bar_currtype
left outer join vtbl_exchange_rate as fx
					on 	fx.fiscal_month_id = stg.fiscal_month_id and 
						lower(fx.from_currtype) = lower(stg.bar_currtype)
 where cast(stg.pct_of_total as numeric(38,12)) !=0      
--) stg 
--group by stg.bar_currtype, stg.cost_pool
;

/******************************************
 _gap analysis of final allocations and cost pools
  Select _gap_to_allocate, t.allocated_amt, _gap_to_allocate-t.allocated_amt as _gap
 from bnr_gap_to_allocate_for_wc s
 left join (Select sum(allocated_amt) as allocated_amt, cost_pool,bar_currtype,sum(allocated_amt_usd), count(1) as rec_cnt 
		  from stage.agm_allocated_data_rule_101
		  where dataprocessing_phase = 'phase 22'
		  group by cost_pool,bar_currtype) t on s.cost_pool = t.cost_pool and  s.bar_currtype = t.bar_currtype 
 * 
 */

  
exception
when others then raise info 'exception occur while ingesting data in stage.agm_allocated_data_rule_101';
end;
$$
;

CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_13(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN   
	
	--TESTING
	--delete from stage.sgm_allocated_data_rule_13;
	--call stage.p_allocate_data_rule_13 (202007)
	--call stage.p_allocate_data_rule_13 (202006)
	--select count(*) from stage.sgm_allocated_data_rule_13
	--select fiscal_month_id, count(*) from stage.sgm_allocated_data_rule_13 group by fiscal_month_id order by 1
/*
 *	This procedure manages the allocations for Rule ID #13
 *
 *		Allocation Group: Product - Partial Customer 
 *		Known: 	 sku, bar_product, & bar_custno
 *		Unknown: soldto (shipto)
 *
 * 		Final Table(s): 
 *			stage.sgm_allocated_data_rule_13
 *
 * 		Rule Logic:	
 * 			Allocate to all past historical shipto, soldto combinations 
 * 			for historical records of SKU purchase for soldtos within 
 *			bar customer hierarchy
 *
 *		Implementation Steps:
 * 			Part 01: Allocate transactions across soldtos found in the 
 *					 base rate table for the same bar_custno, material, 
 *					 & bar_product
 *  		Part 02: Capture Leakage
 *			Part 03: Load results into stage.sgm_allocated_data_rule_13
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
				
		from 	stage.bods_core_transaction_agg as tran
				inner join ref_data.data_processing_rule as dpr
					on  dpr.data_processing_ruleid = tran.mapped_dataprocessing_ruleid 
				inner join vtbl_date_range as dt_rng
					on 	tran.posting_week_enddate between dt_rng.range_start_date and dt_rng.range_end_date
		where 	0=0
			and dpr.data_processing_ruleid = 13
			and tran.audit_rec_src in  ('sap_c11', 'sap_lawson', 'sap_p10')
			
			/* filter for examples */
--			and tran.bar_custno in ('rona')
--			and tran.bar_custno in ('rona', 'ace')
--			and tran.bar_custno in ('rona', 'ace', 'ind_oth')
--			and tran.bar_custno in ('rona', 'retail_oth')
	;
--	/* create list of unique bar_custno from unallocated trans */
--	drop table if exists _list_BarCust
--	;
--	create temporary table _list_BarCust as 
--		select 	distinct trans.mapped_bar_custno as bar_custno
--		from 	_trans_unalloc trans
--	;

	/* create list of unique bar_custno|material|bar_product from unallocated trans */
	drop table if exists _list_BarCust_Material
	;
	create temporary table _list_BarCust_Material as 
		select 	distinct 
				trans.mapped_bar_custno as bar_custno, 
				trans.material, 
				trans.mapped_bar_product as bar_product,
				trans.bar_currtype, 
				trans.source_system
		from 	_trans_unalloc trans
	;
	/* grab subset of rate base table w/ matching bar_custno, bar_product, material */
	drop table if exists _rate_rule13_part01
	;
	create temporary table _rate_rule13_part01 as 
		select 	rb.bar_entity,
				rb.soldtocust,
				rb.bar_custno,
				rb.material,
				rb.bar_product,
				rb.total_bar_amt,
				rb.bar_currtype,
				rb.source_system,
				sum(rb.total_bar_amt) 
					over( 
						partition by
							rb.bar_entity,
							rb.bar_custno, 
							rb.material, 
							rb.bar_product,
							rb.bar_currtype,
							rb.source_system
					) as total_bar_amt_bar_custno 
		from 	_list_BarCust_Material as bcm
				inner join stage.rate_base rb 
					on 	rb.bar_custno = bcm.bar_custno and 
						rb.material = bcm.material and 
						rb.bar_product = bcm.bar_product and 
						rb.bar_currtype  = bcm.bar_currtype and
						rb.source_system  = bcm.source_system
				inner join vtbl_date_range dt 
					on 	dt.range_start_date <= rb.range_start_date and 
						dt.range_end_date >= rb.range_end_date 
	;
	/* Part 01: Allocations @ bar_custno */
	drop table if exists _part01_allocated_trans_rate
	;
	create temporary table _part01_allocated_trans_rate as 
		select 	 tran.source_system
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
				,tran.org_soldtocust
				,tran.material
				
				,rb.soldtocust as alloc_soldtocust
				
				,tran.bar_currtype
				,tran.org_dataprocessing_ruleid
				,tran.mapped_dataprocessing_ruleid
				
				,tran.unallocated_bar_amt
				
				,rb.total_bar_amt
				,rb.total_bar_amt_bar_custno
			
				,(rb.total_bar_amt / rb.total_bar_amt_bar_custno) as rate_p01
				
				,tran.unallocated_bar_amt * 
					(rb.total_bar_amt / rb.total_bar_amt_bar_custno) as allocated_bar_amt
				
		from 	_trans_unalloc as tran 
				inner join _rate_rule13_part01 as rb 
					on  rb.bar_custno = tran.mapped_bar_custno and
						rb.material = tran.material and 
						rb.bar_product = tran.mapped_bar_product and 
						rb.bar_entity = tran.bar_entity and 
						rb.bar_currtype = tran.bar_currtype and 
						rb.source_system = tran.source_system
		where 	rb.total_bar_amt_bar_custno != 0
	;
	-- create list of transactions (org_tranagg_id)
	drop table if exists _part01_allocated_trans
	;
	create temporary table _part01_allocated_trans as 
		select 	distinct org_tranagg_id
		from 	_part01_allocated_trans_rate
	;

-- 	/* validation 01: allocated = unallocated on the transactions that were allocated */
--	select 	'Allocated' as resultset, sum(allocated_bar_amt) as amt
--	from 	_part01_allocated_trans_rate
--	union all
--	select 	'Unallocated' as resultset, sum(unallocated_bar_amt) as amt
--	from 	_trans_unalloc tr
--			inner join _part01_allocated_trans tra 
--				on 	tra.org_tranagg_id = tr.org_tranagg_id
--	;
--
-- 	/* validation 02: allocations != 100% */
--	select 	alloc_trans.org_tranagg_id, sum(alloc_trans.rate_p01)
--	from 	_part01_allocated_trans_rate alloc_trans
--	group by alloc_trans.org_tranagg_id
--	having 	round(abs(sum(alloc_trans.rate_p01) * 100),0) != 100
--	order by sum(alloc_trans.rate_p01)
--	;
	

/* ------------------------------------------------------------------ 
 * 	Part 02: Capture Leakage
 * ------------------------------------------------------------------
 */
	drop table if exists _part02_leakage
	;
	/* Part 04: transactions that couldn't be allocated in previous 3 parts 
	 * 		i.e. no combination of bar_custno/material in rate_base
	 */
	create temporary table _part02_leakage as 
		select 	org_tranagg_id
		from 	_trans_unalloc
		except (
			select 	org_tranagg_id from _part01_allocated_trans
		)
	;
/* ------------------------------------------------------------------ 
 * 	Part 03: Load results into stage.sgm_allocated_data_rule_13
 * ------------------------------------------------------------------
 */
	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	stage.sgm_allocated_data_rule_13
	where 	posting_week_enddate between 
			(select range_start_date from vtbl_date_range) and 
			(select range_end_date from vtbl_date_range)
	;
	/* load allocated transactions */
	insert into stage.sgm_allocated_data_rule_13 (
	
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
				
				tran.shiptocust as alloc_shiptocust,
				tran.alloc_soldtocust,
				tran.material as alloc_material,
				tran.mapped_bar_product as alloc_bar_product,
				
				bar_currtype,
								
				tran.org_dataprocessing_ruleid,
				tran.mapped_dataprocessing_ruleid,
				1 as dataprocessing_outcome_id,
				'phase 1' as dataprocessing_phase,
				
				tran.allocated_bar_amt as allocated_amt,
				getdate() as audit_loadts
		from 	_part01_allocated_trans_rate tran
	;

	/* load leakage (original transactions) */
	insert into stage.sgm_allocated_data_rule_13 (
	
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
				
				tran.shiptocust as alloc_shiptocust,
				tran.org_soldtocust as alloc_soldtocust,
				tran.material as alloc_material,
				tran.mapped_bar_product as alloc_bar_product,
				
				bar_currtype,
								
				tran.org_dataprocessing_ruleid,
				tran.mapped_dataprocessing_ruleid,
				2 as dataprocessing_outcome_id,
				'phase 100' as dataprocessing_phase,
				
				tran.unallocated_bar_amt as allocated_amt,
				
				getdate() as audit_loadts
		from 	_part02_leakage leak
				inner join _trans_unalloc as tran
					on 	tran.org_tranagg_id = leak.org_tranagg_id
	;
	/* 	Validation: compare total amount between original unallocated transactions
	 *    and the allocated table;
	 */
--	select 	'orig' as recordset, round(sum(unallocated_bar_amt),2) as amt,
--			count(distinct org_tranagg_id) orig_trans_count
--	from _trans_unalloc
--	union all
--	select 	'result' as recordset, round(sum(allocated_amt),2) as amt,
--			count(distinct org_tranagg_id) orig_trans_count
--	from stage.sgm_allocated_data_rule_13
--	union all
--	select 	'result-allocated' as recordset, round(sum(allocated_amt),2) as amt,
--			count(distinct org_tranagg_id) orig_trans_count
--	from stage.sgm_allocated_data_rule_13
--	where 	dataprocessing_outcome_id = 1
--	union all
--	select 	'result-unallocated' as recordset, round(sum(allocated_amt),2) as amt,
--			count(distinct org_tranagg_id) orig_trans_count
--	from stage.sgm_allocated_data_rule_13
--	where 	dataprocessing_outcome_id = 2
--	order by 1
--	;
--
--Select sum(bar_amt), audit_rec_src
--from stage.bods_core_transaction_agg bcta 
--where mapped_dataprocessing_ruleid =13 
--and fiscal_month_id = 202001
--group by audit_rec_src;
----
----
----
--Select sum(allocated_amt), source_system 
--from stage.sgm_allocated_data_rule_13 bcta 
--where mapped_dataprocessing_ruleid =13 
--and fiscal_month_id = 202001
--group by source_system;
	
exception
when others then raise info 'exception occur while ingesting data in stage.sgm_allocated_data_rule_13';
end;
$$
;

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
 left join sapc11.kna1_current kc on lower(shiptocust) = lower(kc.kunnr) 
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
from 	bods.drm_product_current
where 	loaddts = ( select max(loaddts) from bods.drm_product_current dpc );
--Select s.*
--from bods.hfm_vw_hfm_actual_trans_current s 
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

CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_23(fmthid integer)
 LANGUAGE plpgsql
AS $_$
BEGIN   
	

/*
 *	This procedure manages the allocations for Rule ID #23
 *
 *		Allocation Exception - RSA & Price Adjustments - A40115
 *
 * 		Final Table(s): 
 *			stage.sgm_allocated_data_rule_23
 *
 * 		Rule Logic:	
 * 			"It's complicated"
 *
 */

	
	/*
		truncate table stage.sgm_allocated_data_rule_23;
		call stage.p_allocate_data_rule_23 (202109);
		select count(*), round(sum(allocated_amt),2) from stage.sgm_allocated_data_rule_23 where fiscal_month_id = 202007; -- (-33096637.58)
	 */
	
/*
	Step 1 - Find RSA Target
		create date_range table for filtering
		create list of retail bar_customers
		create list of commercial bar_customeers
		create temp table containing RSA transactions (A40115/rule23)
		create temp table containing RSA trans for C11-retail customers
		create temp table containing RSA trans for C11-commercial customers		<--- handled as standard allocation (based on orig_ruleid) STEP 4
		create temp table containing RSA trans for lawson						<--- handled as standard allocation (based on orig_ruleid) STEP 4
		create exchange rate table for current month
		create list of bar_product division mappings
		
		aggregate RSA trans for C11-Retail to bar_custno and division ($100)
		
	Step 2
		Step2A - prepare RSA bible data
		create table containing RSA bible trans (US/CAD) for current month w/ all USD
			Convert RSA CAD Bible, all CAD RSA$ to USD using conversion table 
			For both RSA USA, RSA CAD Bible, bring in Month#, Demand Group, Division, Brand, SKU, RSA$, PCR, MGSV
		
		Step2B - allocate RSA bible data (Non-MGSV)
		For Non-MGSV:
			create mapping of demand_group / soldtonumber (based on dim customer)
			create temp table of unique combinations of:
				demand_group, soldto, sku (for filtering base rate table)
			create base rate table for unique combinations of demand_group, soldto, sku
			create temp table of allocated RSA transactions Non-MGSV
		*	create temp table of unallocated RSA transactions Non-MGSV (use "virtual SKU/SoldTo#, etc)
		
		Step2C - allocate RSA bible data (MGSV)
		For MGSV:
			create mapping of demand_group / soldtonumber (based on dim customer)
			create temp table of unique combinations of:
				demand_group, soldto, sku (for filtering base rate table)
			create base rate table for unique combinations of demand_group, soldto
			create temp table of allocated RSA transactions MGSV
		*	create temp table of unallocated RSA transactions MGSV (use "virtual SKU/SoldTo#, etc)
			
		Step2D - fill out rest of transactions
		*	add bar_product (use gpp_division & brand from RSA for mapping)
			add bar_customer (use ref_data.soldto_barcust_mapping)
			split transactions across entities
	
	Step 3
		fill gap for RSA trans for C11-retail customers
		
		create transactions for gaps in amounts between:
			BODS transactions (A40115)
			RSA allocated transactions
		
*/


/*
		https://patorjk.com/software/taag/
--                                                                      
--   ad88888ba  888888888888  88888888888  88888888ba          88  
--  d8"     "8b      88       88           88      "8b       ,d88  
--  Y8,              88       88           88      ,8P     888888  
--  `Y8aaaaa,        88       88aaaaa      88aaaaaa8P'         88  
--    `"""""8b,      88       88"""""      88""""""'           88  
--          `8b      88       88           88                  88  
--  Y8a     a8P      88       88           88                  88  
--   "Y88888P"       88       88888888888  88                  88
--                                                                      
                                                                 
*/
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	dt.fmth_id as fiscal_month_id,
				cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
		group by dt.fmth_id
	;

	/* create list of RETAIL bar_custno
	 *  (based on same logic as dim_customer)
	 */
	drop table if exists tmp_retail_bar_custno
	;
	create temporary table tmp_retail_bar_custno as 
		SELECT 	name AS bar_custno
		FROM 	bods.drm_customer_current
		WHERE 	0=0 
			AND loaddts = ( SELECT max(loaddts) FROM bods.drm_customer_current )
			AND membertype != 'Parent'
			AND CASE
					WHEN CAST(generation AS int) <= 6 THEN
					CASE
						WHEN name = 'Customer_None' THEN name
						ELSE
						CASE
							generation 
							WHEN 1 THEN NULL
							WHEN 2 THEN level1
							WHEN 3 THEN level2
							WHEN 4 THEN level3
							WHEN 5 THEN level4
							WHEN 6 THEN level5
							WHEN 7 THEN level6
							WHEN 8 THEN level7
							WHEN 9 THEN level8
							WHEN 10 THEN level9
							WHEN 11 THEN level10
						END
					END
					ELSE level6
				END = 'Retail'
	;
	/* RSA (A40115) transactions for current period */
	drop table if exists tmp_rsa
	;
	CREATE TEMPORARY TABLE tmp_rsa AS
		SELECT	tran.*,
				e.level5 as EntitySourceRegion
		FROM	stage.bods_core_transaction_agg AS tran
				inner join ref_data.entity e 
					on 	lower(e.name) = lower(tran.bar_entity)
				INNER JOIN vtbl_date_range AS dt_rng 
					ON 	tran.posting_week_enddate BETWEEN dt_rng.range_start_date AND dt_rng.range_end_date
		WHERE	0 = 0
			AND tran.bar_acct = 'A40115' /* RSA */
			AND tran.mapped_dataprocessing_ruleid = 23
	;
	/* Type 1: A40115 transactions from C11 Retail Customers 
	 * 
	 * 		Exception Rule
	 */
	drop table if exists tmp_rsa_c11_retail
	;
	CREATE TEMPORARY TABLE tmp_rsa_c11_retail AS
		SELECT	tran.*
		FROM	tmp_rsa as tran
				INNER JOIN tmp_retail_bar_custno cust 
					ON 	lower(cust.bar_custno) = lower(tran.mapped_bar_custno)
		WHERE	0 = 0
			AND tran.audit_rec_src IN ('sap_c11')
	;

/* DEBUG: confirm inputs match outputs amount/count */
--select 1 as sort, 'TOTAL' as src, sum(bar_amt), count(*) from tmp_rsa
--union all
--select 2 as sort, 'C11 Retail' as src, sum(bar_amt), count(*) from tmp_rsa_c11_retail
--order by 1
--;
	/* create bar_product to (Level7) Division Mapping
	 *  (based on same logic as dim_product)
	 */
	drop table if exists tmp_bar_product_division_mapping
	;
	create temporary table tmp_bar_product_division_mapping as 
	with
		cte_base as (
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
					cast(generation as int) as generation,
					case when level7 is null then 'unknown' else level7 end as level07_bar
			from 	bods.drm_product_current
			where 	loaddts = ( select max(loaddts) from bods.drm_product_current dpc )
				and membertype != 'Parent'
		)
		select 	bar_product,
				case 
					when generation <= 7 then 
						case when bar_product = 'Product_None' then bar_product else parent end 
					else level07_bar 
				end as division
		from 	cte_base 
	;
	/* create temp table for exchange_rate */
	drop table if exists vtbl_exchange_rate
	;
	create temporary table vtbl_exchange_rate as 
		select 	rt.fiscal_month_id, 
				rt.from_currtype,
				rt.fxrate
		from 	ref_data.hfmfxrates_current rt
		where 	lower(rt.to_currtype) = 'usd'
	;

	/* STEP 01
	 * For C11 Retail customers, sum up $ by base customer and level 07 BA&R division 
	 */
	drop table if exists tmp_rsa_c11_retail_step1
	;
	CREATE TEMPORARY TABLE tmp_rsa_c11_retail_step1 AS
		SELECT	tran.audit_rec_src AS source_system,
				tran.EntitySourceRegion,
				tran.mapped_bar_custno AS bar_custno,
				case 
					-- handle CONSTR_METAL_STR special scenario
					-- actual DIVISION coming in as PROD
					when prd_div.division is null and upper(tran.org_bar_product) = 'CONSTR_METAL_STR' then 'CONSTR_METAL_STR'
					else  COALESCE (prd_div.division, 'unknown' )
				end as division,
				SUM(tran.bar_amt) as bar_amt,
				SUM( 
					CASE 
						WHEN rt.fxrate IS NOT NULL THEN rt.fxrate * tran.bar_amt 
						ELSE tran.bar_amt 
					END 
				) AS total_rsa_amt_usd
		FROM	tmp_rsa_c11_retail AS tran
				LEFT OUTER JOIN vtbl_exchange_rate rt 
					ON 	rt.fiscal_month_id = tran.fiscal_month_id AND 
						lower(rt.from_currtype) = lower(tran.bar_currtype)
				left outer join tmp_bar_product_division_mapping as prd_div
					on 	lower(prd_div.bar_product) = lower(tran.org_bar_product)
		GROUP BY
			tran.audit_rec_src,
			tran.EntitySourceRegion,
			tran.mapped_bar_custno,
			case 
				when prd_div.division is null and upper(tran.org_bar_product) = 'CONSTR_METAL_STR' then 'CONSTR_METAL_STR'
				else  COALESCE (prd_div.division, 'unknown' )
			end,
			COALESCE (prd_div.division, 'unknown' )
	;

/* DEBUG: confirm input amount matches output amount */
--select 	'output', sum(bar_amt)
--from 	tmp_rsa_c11_retail_step1
--union all
--select 	'input', sum(bar_amt)
--from 	tmp_rsa_c11_retail
--order by 1
--;
/* DEBUG: confirm input amount matches output amount */
--select 	'Match: -33,096,637.58', round(sum(total_rsa_amt_usd),2) as bar_amt_usd
--from 	tmp_rsa_c11_retail_step1
--;

/* DEBUG: Check to see if any C11 retail transactions fall out because of missing mapping */
--select 	count(*) as tx_count,
--		sum(case when prd_div.bar_product is null then 0 else 1 end) as tx_div_match,
--		sum(case when prd_div.bar_product is null then 1 else 0 end) as tx_div_miss,
--		sum(bar_amt) as tx_amt,
--		sum(case when prd_div.bar_product is null then 0 else bar_amt end) as amt_div_match,
--		sum(case when prd_div.bar_product is null then bar_amt else 0 end) as amt_div_miss
--from 	tmp_rsa_c11_retail AS tran
--		left outer join tmp_bar_product_division_mapping as prd_div
--			on 	lower(prd_div.bar_product) = lower(tran.org_bar_product)
--;
/* DEBUG: BAR_Products found in BODS RSA transactions, but not in dim_product 
 * 
 * 		CONSTR_METAL_STR <-- this is actually a BA&R Division
 */
--select 	distinct lower(tran.org_bar_product)
--from 	tmp_rsa_c11_retail AS tran
--		left outer join tmp_bar_product_division_mapping as prd_div
--			on 	lower(prd_div.bar_product) = lower(tran.org_bar_product)
--where 	prd_div.bar_product is null
--;
/*
--                                                                      
--   ad88888ba  888888888888  88888888888  88888888ba       ad888888b,  
--  d8"     "8b      88       88           88      "8b     d8"     "88  
--  Y8,              88       88           88      ,8P             a8P  
--  `Y8aaaaa,        88       88aaaaa      88aaaaaa8P'          ,d8P"   
--    `"""""8b,      88       88"""""      88""""""'          a8P"      
--          `8b      88       88           88               a8P'        
--  Y8a     a8P      88       88           88              d8"          
--   "Y88888P"       88       88888888888  88              88888888888  
--                                                                      
--                                                                      
*/

	/* STEP 02-A
	 * Scrub out all transactions with RSA $ = 0 
	 * Convert RSA CAD Bible, all CAD RSA$ to USD using conversion table 
	 * For both RSA USA, RSA CAD Bible, bring in Month#, Demand Group, Division, Brand, SKU, RSA$, PCR
	 */
	drop table if exists tmp_rsa_step2a_CAD
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2a_CAD AS
		select 	case 
					when rsa_src.demand_group = 'HD' then 'CDNHD'
					when rsa_src.demand_group = 'CTC' then 'CDNTIRE'
					when rsa_src.demand_group = 'LOWES' then 'CDNLOWES'
					when rsa_src.demand_group = 'AMAZON' then 'CDNAMAZON'
					when rsa_src.demand_group = 'WALMART' then 'CDNWAL'
					when rsa_src.demand_group = 'RGMASS' then 'CDNRG'
					when rsa_src.demand_group = 'HDYOW' then 'CDNHD'
					else rsa_src.demand_group
				end as demand_group,
				rsa_src.division,
				rsa_src.brand,
				rsa_src.sku,
				rsa_src.fiscal_month_id,
				rsa_src.amt * -1 as amt,
				((rsa_src.amt * -1) * rt.fxrate) as amt_usd,
				rsa_src.pcr,
				rsa_src.mgsv
		from 	ref_data.rsa_bible AS rsa_src
				INNER JOIN vtbl_date_range AS dt_rng 
					ON 	dt_rng.fiscal_month_id = rsa_src.fiscal_month_id
				INNER JOIN vtbl_exchange_rate rt 
					ON 	rt.fiscal_month_id = rsa_src.fiscal_month_id AND 
						lower(rt.from_currtype) = 'cad'
		where 	rsa_src.source_system = 'rsa_bible_cad' and 
				rsa_src.amt != 0
	;

	drop table if exists tmp_rsa_step2a_US
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2a_US AS
		select 	rsa_src.demand_group,
				rsa_src.division,
				rsa_src.brand,
				rsa_src.sku,
				rsa_src.fiscal_month_id,
				rsa_src.amt * -1 as amt,
				rsa_src.amt * -1 as amt_usd,
				rsa_src.pcr,
				rsa_src.mgsv
		from 	ref_data.rsa_bible AS rsa_src
				INNER JOIN vtbl_date_range AS dt_rng 
					ON 	dt_rng.fiscal_month_id = rsa_src.fiscal_month_id
		where 	rsa_src.source_system = 'rsa_bible_us' and 
				rsa_src.amt != 0
	;
/* DEBUG: confirm matching amounts w/ requirements */
--select 	'CAD', mgsv, round(sum(amt_usd),2) as amt_usd, round(sum(amt),2) as amt
--from 	tmp_rsa_step2a_CAD
--group by mgsv
--union all 
--select 	'US', mgsv, round(sum(amt_usd),2) as amt_usd, round(sum(amt),2) as amt
--from 	tmp_rsa_step2a_US
--group by mgsv
--order by 1,2
--;

	drop table if exists tmp_rsa_step2a
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2a AS
		select 	'GTS_CA' as EntitySourceRegion,
				rsa_src.demand_group,
				rsa_src.division,
				rsa_src.brand,
				rsa_src.sku,
				rsa_src.fiscal_month_id,
				rsa_src.amt_usd,
				rsa_src.pcr,
				rsa_src.mgsv
		from 	tmp_rsa_step2a_CAD as rsa_src
		union all 
		select 	'GTS_US' as EntitySourceRegion,
				rsa_src.demand_group,
				rsa_src.division,
				rsa_src.brand,
				rsa_src.sku,
				rsa_src.fiscal_month_id,
				rsa_src.amt_usd,
				rsa_src.pcr,
				rsa_src.mgsv
		from 	tmp_rsa_step2a_US as rsa_src
	;

	drop table if exists map_rsa_demandgroup_2_bar_custno
	;
	CREATE TEMPORARY TABLE map_rsa_demandgroup_2_bar_custno AS
		select 'LOWESFOB' as demand_group,'Lowes'as bar_custno union all
		select 'HDYOW' as demand_group,'HomeDepot'as bar_custno union all
		select 'AMAZONFOB' as demand_group,'Amazon'as bar_custno union all
		select 'TARGET' as demand_group,'Target'as bar_custno union all
		select 'SEARSCOM' as demand_group,'SearsKmart'as bar_custno union all
		select 'SEARSFOB' as demand_group,'SearsKmart'as bar_custno union all
		select 'TARGETFOB' as demand_group,'Target'as bar_custno union all
		select 'CTC' as demand_group,'CanadianTire'as bar_custno
	;
	/* STEP 02-B
	 * For a non-miscellaneous SKU: 
	 * 		Spread RSA $ across all soldto with a positive invoice sale (A40110)
	 * 		of the SKU within the month for the Demand Group 
	 * 
	 * 	NOTE: We are only allocating to perfect transactions (ruleid 1)
	 *	 	that had an invoiced sale (A40110) for that current month
	 *
	 */
	drop table if exists tmp_rsa_step2b_NonMGSV_dg_sku_soldto
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2b_NonMGSV_dg_sku_soldto AS
		with
			cte_NonMGSV_demandgroups as (
				select 	distinct 
						rsa.EntitySourceRegion,
						lower(rsa.demand_group) as demand_group,
						rsa.sku
				from 	tmp_rsa_step2a as rsa
				where 	rsa.mgsv = 'Non-MGSV' and 
						lower(rsa.demand_group) not in (
							select 	distinct lower(demand_group)
							from 	map_rsa_demandgroup_2_bar_custno
						)
			),
			cte_retail_customers as (
				select 	sbm.soldtocust 
				from 	ref_data.soldto_barcust_mapping sbm 
						inner join tmp_retail_bar_custno retail
							on lower(retail.bar_custno) = lower(sbm.bar_custno)
				where 	sbm.current_flag = 1
			)
		select 	distinct
				dmd.kunnr as soldto_number,
				dg.demand_group,
				dg.sku,
				dg.EntitySourceRegion
		from 	sapc11.kna1_current as dmd
				inner join cte_NonMGSV_demandgroups as dg 
					on	lower(dg.demand_group) = lower(dmd.bran1)
				inner join cte_retail_customers as retail_cust
					on 	lower(retail_cust.soldtocust) = lower(dmd.kunnr)
	;	
	drop table if exists tmp_rsa_step2b_NonMGSV_dg_sku_soldto_mapping
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2b_NonMGSV_dg_sku_soldto_mapping AS
		with
			cte_NonMGSV_demandgroups_mapped as (
				select 	distinct 
						rsa.EntitySourceRegion,
						lower(rsa.demand_group) as demand_group,
						lower(mapping.bar_custno) as bar_custno,
						rsa.sku
				from 	tmp_rsa_step2a as rsa
						inner join map_rsa_demandgroup_2_bar_custno as mapping
							on 	lower(mapping.demand_group) = lower(rsa.demand_group)
				where 	rsa.mgsv = 'Non-MGSV'
			),
			cte_retail_customers as (
				select 	sbm.soldtocust 
				from 	ref_data.soldto_barcust_mapping sbm 
						inner join tmp_retail_bar_custno retail
							on lower(retail.bar_custno) = lower(sbm.bar_custno)
				where 	sbm.current_flag = 1
			)
		select 	distinct
				sbm.soldtocust as soldto_number,
				mapping.demand_group,
				mapping.bar_custno,
				mapping.sku,
				mapping.EntitySourceRegion
		from 	ref_data.soldto_barcust_mapping sbm
				inner join cte_NonMGSV_demandgroups_mapped as mapping
					on	lower(mapping.bar_custno) = lower(sbm.bar_custno)
				inner join cte_retail_customers as retail_cust
					on 	lower(retail_cust.soldtocust) = lower(sbm.soldtocust)
	;
	drop table if exists tmp_rsa_step2b_NonMGSV_dg_sku_soldto_final
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2b_NonMGSV_dg_sku_soldto_final AS
		select 	distinct soldto_number, demand_group, sku, EntitySourceRegion
		from 	tmp_rsa_step2b_NonMGSV_dg_sku_soldto
		union all 
		select 	distinct soldto_number, demand_group, sku, EntitySourceRegion
		from 	tmp_rsa_step2b_NonMGSV_dg_sku_soldto_mapping
	;
				
/* DEBUG: confirm many-to-one relationship between soldtonum and demand group */
--select 	dmd.kunnr as soldto_number, count(distinct dmd.bran1) as num_brands
--from 	sapc11.kna1_current as dmd 
--group by dmd.kunnr  
--having count(distinct dmd.bran1) > 1
--;
/* DEBUG: demand group in RSA bible, not in Dim Customer */
--with
--	cte_NonMGSV_demandgroups as (
--		select 	distinct 
--				lower(rsa.demand_group) as demand_group,
--				rsa.sku
--		from 	tmp_rsa_step2a rsa
--		where 	rsa.mgsv = 'Non-MGSV'
--	)
--select 	distinct
--		dg.demand_group
--from 	sapc11.kna1_current as dmd
--		right outer join cte_NonMGSV_demandgroups as dg 
--			on	lower(dg.demand_group) = lower(dmd.bran1)
--where 	dmd.bran1 is null
--;
/* DEBUG: demand group is sourced from sapc11.kna1_current 
 * 
 * the following demand groups are found in RSA bible but not source table
 * 		'HDYOW','LOWESFOB','CTC'
 */
--select 	distinct bran1
--from 	sapc11.kna1_current
--where 	upper(bran1) in ('HDYOW','LOWESFOB','CTC')
--;
	/* create rate base table */
	drop table if exists tmp_rsa_step2b_rate_base
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2b_rate_base AS
		select 	dt_rng.fiscal_month_id,
				dg_sku_soldto.demand_group,
				rb.soldtocust as soldtocust,
				rb.material as material,
				e.level5 as EntitySourceRegion,
				SUM(
					case 
						when xrt.fxrate is not null then cast(xrt.fxrate * rb.total_bar_amt as decimal(38, 8)) 
						else cast(rb.total_bar_amt as decimal(38, 8))
					end
				) as total_bar_amt_usd
		from 	stage.rate_base rb
				inner join ref_data.entity e 
					on 	lower(e."name") = lower(rb.bar_entity)
				inner join vtbl_date_range as dt_rng
					on  dt_rng.range_start_date = rb.range_start_date and 
						dt_rng.range_end_date = rb.range_end_date
				inner join tmp_rsa_step2b_NonMGSV_dg_sku_soldto_final as dg_sku_soldto 
					on 	lower(dg_sku_soldto.soldto_number) = lower(rb.soldtocust) and 
						lower(dg_sku_soldto.sku) = lower(rb.material)
				left outer join vtbl_exchange_rate xrt 
					on 	xrt.fiscal_month_id = dt_rng.fiscal_month_id and
						lower(xrt.from_currtype) = lower(rb.bar_currtype)
		group by
				dt_rng.fiscal_month_id,
				dg_sku_soldto.demand_group,
				rb.soldtocust,
				rb.material,
				e.level5
		having 	SUM(
					case 
						when xrt.fxrate is not null then cast(xrt.fxrate * rb.total_bar_amt as decimal(38, 8)) 
						else cast(rb.total_bar_amt as decimal(38, 8))
					end
				) > 0
	;
	/* create rate table for demand group / sku in RSA source */
	drop table if exists tmp_rsa_step2b_NonMGSV_rate
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2b_NonMGSV_rate AS
	with
		cte_rt_total as (
			select 	rt.fiscal_month_id,
					rt.demand_group,
					rt.soldtocust, 
					rt.material,
					rt.EntitySourceRegion,
					rt.total_bar_amt_usd as amt_usd,
					sum(rt.total_bar_amt_usd) over( 
						partition by rt.fiscal_month_id, lower(rt.demand_group), 
							lower(rt.material), lower(rt.EntitySourceRegion)
					) as amt_usd_partition_total
			from 	tmp_rsa_step2b_rate_base rt 
			where 	rt.total_bar_amt_usd > 0
		)
		select	rtt.fiscal_month_id,
				rtt.demand_group,
				rtt.soldtocust, 
				rtt.material,
				rtt.EntitySourceRegion,
				CAST(rtt.amt_usd as decimal(20,8)) as amt_usd,
				CAST(rtt.amt_usd_partition_total AS DECIMAL(20,8)) as amt_usd_partition_total,
				CAST(rtt.amt_usd as decimal(20,8)) / 
					CAST(NULLIF(rtt.amt_usd_partition_total, 0) AS DECIMAL(20,8)) as pct_of_total
		from 	cte_rt_total as rtt
		where 	rtt.amt_usd_partition_total != 0
	;
/* DEBUG: clusters with cumulative pct of total != 100% */
--select 	rt.fiscal_month_id, lower(rt.material), lower(rt.demand_group), lower(rt.EntitySourceRegion), sum(rt.pct_of_total)
--from 	tmp_rsa_step2b_NonMGSV_rate rt
--group by rt.fiscal_month_id, lower(rt.material), lower(rt.demand_group), lower(rt.EntitySourceRegion)
--having round(sum(rt.pct_of_total),4) != 1
--order by 5 asc
--;
/* DEBUG: validate against example in excel */
--select 	rt.*
--from 	tmp_rsa_step2b_NonMGSV_rate rt
--where 	0=0
--	and lower(rt.demand_group) = 'ace'
--	and lower(rt.material) = 'cmcb002b'
--;
	/* allocate RSA amounts */
	drop table if exists tmp_rsa_step2b_NonMGSV_allocated
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2b_NonMGSV_allocated AS
		select 	rsa.mgsv,
				rsa.pcr,
				rsa.fiscal_month_id,
				
				rsa.EntitySourceRegion,
				rsa.demand_group,
				rt.soldtocust,
				NULL as bar_custno,
				rsa.sku as material,
				NULL as bar_product,
				rsa.division,
				rsa.brand,
				
				CAST(rsa.amt_usd AS DECIMAL(16, 8)) as amt_usd,
				CAST(NULLIF(rt.pct_of_total, 0) AS DECIMAL(16, 8)) as alloc_pct,
				NVL(CAST(rsa.amt_usd AS DECIMAL(16, 8)) / (1 / CAST(NULLIF(rt.pct_of_total, 0) AS DECIMAL(16, 8))), 0) as alloc_amt_usd
		from 	tmp_rsa_step2a as rsa
				inner join tmp_rsa_step2b_NonMGSV_rate as rt
					on 	rt.fiscal_month_id = rsa.fiscal_month_id and 
						lower(rt.material) = lower(rsa.sku) and 
						lower(rt.demand_group) = lower(rsa.demand_group) and 
						lower(rt.EntitySourceRegion) = lower(rsa.EntitySourceRegion)
		where 	rsa.mgsv = 'Non-MGSV'
	;
/* DEBUG: validate against example in excel */
--select 	rt.*
--from 	tmp_rsa_step2b_NonMGSV_allocated rt
--where 	0=0
--	and lower(rt.demand_group) = 'ace'
--	and lower(rt.material) = 'cmcb002b'
--;
	/* create rate base table for FOB fallout */
	drop table if exists tmp_rsa_step2b_rate_base_fob
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2b_rate_base_fob AS
		select 	fob_tx.fiscal_month_id,
				e.level5 as EntitySourceRegion,
				map_cust.demand_group,
				map_cust.bar_custno,
				fob_tx.alloc_soldtocust as soldtocust,
				fob_tx.alloc_material as material,
				SUM(
					case 
						when xrt.fxrate is not null then cast(xrt.fxrate * fob_tx.allocated_amt as decimal(38, 8)) 
						else cast(fob_tx.allocated_amt as decimal(38, 8))
					end
				) as total_bar_amt_usd
		from 	stage.sgm_allocated_data_rule_21 as fob_tx
				inner join ref_data.entity e
					on 	lower(e.name) = lower(fob_tx.bar_entity)
				inner join map_rsa_demandgroup_2_bar_custno as map_cust
					on 	lower(map_cust.bar_custno) = lower(fob_tx.mapped_bar_custno) and 
						lower(map_cust.demand_group) like '%fob%'
				inner join vtbl_date_range as dt_rng
					on  dt_rng.fiscal_month_id = fob_tx.fiscal_month_id 
				left outer join vtbl_exchange_rate xrt 
					on 	xrt.fiscal_month_id = dt_rng.fiscal_month_id and
						lower(xrt.from_currtype) = lower(fob_tx.bar_currtype)
		where 	fob_tx.dataprocessing_outcome_id = 1 and 
				fob_tx.allocated_amt != 0
		group by fob_tx.fiscal_month_id,
				e.level5,
				map_cust.demand_group,
				map_cust.bar_custno,
				fob_tx.alloc_soldtocust,
				fob_tx.alloc_material
		having 	SUM(
					case 
						when xrt.fxrate is not null then cast(xrt.fxrate * fob_tx.allocated_amt as decimal(38, 8)) 
						else cast(fob_tx.allocated_amt as decimal(38, 8))
					end
				) > 0
	;

	/* create rate table for demand group / sku in RSA source */
	drop table if exists tmp_rsa_step2b_NonMGSV_rate_fob
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2b_NonMGSV_rate_fob AS
	with
		cte_rt_total as (
			select 	rt.fiscal_month_id,
					rt.demand_group,
					rt.soldtocust, 
					rt.material,
					rt.EntitySourceRegion,
					rt.total_bar_amt_usd as amt_usd,
					sum(rt.total_bar_amt_usd) over( 
						partition by rt.fiscal_month_id, lower(rt.demand_group), 
							lower(rt.material), lower(rt.EntitySourceRegion)
					) as amt_usd_partition_total
			from 	tmp_rsa_step2b_rate_base_fob rt 
			where 	rt.total_bar_amt_usd > 0
		)
		select	rtt.fiscal_month_id,
				rtt.demand_group,
				rtt.soldtocust, 
				rtt.material,
				rtt.EntitySourceRegion,
				CAST(rtt.amt_usd as decimal(20,8)) as amt_usd,
				CAST(rtt.amt_usd_partition_total AS DECIMAL(20,8)) as amt_usd_partition_total,
				CAST(rtt.amt_usd as decimal(20,8)) / 
					CAST(NULLIF(rtt.amt_usd_partition_total, 0) AS DECIMAL(20,8)) as pct_of_total
		from 	cte_rt_total as rtt
		where 	rtt.amt_usd_partition_total != 0
	;

/* DEBUG: clusters with cumulative pct of total != 100% */
--select 	rt.fiscal_month_id, lower(rt.material), lower(rt.demand_group), sum(rt.pct_of_total)
--from 	tmp_rsa_step2b_NonMGSV_rate_fob rt
--group by rt.fiscal_month_id, lower(rt.material), lower(rt.demand_group)
--having round(sum(rt.pct_of_total),4) != 1
--order by 4 asc
--;

	/* allocate RSA amounts */
	drop table if exists tmp_rsa_step2b_NonMGSV_allocated_fob
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2b_NonMGSV_allocated_fob AS
		select 	rsa.mgsv,
				rsa.pcr,
				rsa.fiscal_month_id,
				
				rsa.EntitySourceRegion,
				rsa.demand_group,
				rt_fob.soldtocust,
				NULL as bar_custno,
				rsa.sku as material,
				NULL as bar_product,
				rsa.division,
				rsa.brand,
				
				CAST(rsa.amt_usd AS DECIMAL(16, 8)) as amt_usd,
				CAST(NULLIF(rt_fob.pct_of_total, 0) AS DECIMAL(16, 8)) as alloc_pct,
				NVL(CAST(rsa.amt_usd AS DECIMAL(16, 8)) / (1 / CAST(NULLIF(rt_fob.pct_of_total, 0) AS DECIMAL(16, 8))), 0) as alloc_amt_usd
		from 	tmp_rsa_step2a as rsa
				inner join tmp_rsa_step2b_NonMGSV_rate_fob as rt_fob
					on 	rt_fob.fiscal_month_id = rsa.fiscal_month_id and 
						lower(rt_fob.material) = lower(rsa.sku) and 
						lower(rt_fob.demand_group) = lower(rsa.demand_group) and 
						lower(rt_fob.EntitySourceRegion) = lower(rsa.EntitySourceRegion)
				left outer join tmp_rsa_step2b_NonMGSV_rate as rt
					on 	rt.fiscal_month_id = rsa.fiscal_month_id and 
						lower(rt.material) = lower(rsa.sku) and 
						lower(rt.demand_group) = lower(rsa.demand_group) and 
						lower(rt.EntitySourceRegion) = lower(rsa.EntitySourceRegion)
		where 	rsa.mgsv = 'Non-MGSV' and 
				rt.material is null
	;

--select 	sum(alloc_amt_usd)
--from 	tmp_rsa_step2b_NonMGSV_allocated
--;
--select 	sum(alloc_amt_usd)
--from 	tmp_rsa_step2b_NonMGSV_allocated_fob
--;
	/* unallocated RSA amounts */
	drop table if exists tmp_rsa_step2b_NonMGSV_unallocated
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2b_NonMGSV_unallocated AS
		select 	rsa.mgsv,
				rsa.pcr,
				rsa.fiscal_month_id,
				
				rsa.EntitySourceRegion,
				rsa.demand_group,
				'RSA_Non-MGSV_Unallocated' as soldtocust, 
				'RSA_Non-MGSV_Unallocated' as bar_custno, 
				
				rsa.sku as material,
				'RSA_Non-MGSV_Unallocated' as bar_product,
				rsa.division,
				rsa.brand,
				
				CAST(rsa.amt_usd AS DECIMAL(16, 8)) as amt_usd
		from 	tmp_rsa_step2a as rsa
				left outer join tmp_rsa_step2b_NonMGSV_rate as rt
					on 	rt.fiscal_month_id = rsa.fiscal_month_id and 
						lower(rt.material) = lower(rsa.sku) and 
						lower(rt.demand_group) = lower(rsa.demand_group) and 
						lower(rt.EntitySourceRegion) = lower(rsa.EntitySourceRegion)
				left outer join tmp_rsa_step2b_NonMGSV_allocated_fob as rt_fob
					on 	rt_fob.fiscal_month_id = rsa.fiscal_month_id and 
						lower(rt_fob.material) = lower(rsa.sku) and 
						lower(rt_fob.demand_group) = lower(rsa.demand_group) and 
						lower(rt_fob.EntitySourceRegion) = lower(rsa.EntitySourceRegion)
		where 	rsa.mgsv = 'Non-MGSV' and 
				rt.material is null and 
				rt_fob.material is null
	;
/* DEBUG: confirm input amount matches output amount */
--select 	'input', round(sum(amt_usd),2), count(*)
--from 	tmp_rsa_step2a where mgsv = 'Non-MGSV'
--union all
--select 	'output - allocated', round(sum(alloc_amt_usd),2), count(*)
--from 	tmp_rsa_step2b_NonMGSV_allocated
--union all
--select 	'output - allocated fob', round(sum(alloc_amt_usd),2), count(*)
--from 	tmp_rsa_step2b_NonMGSV_allocated_fob
--union all
--select 	'output - unallocated', round(sum(amt_usd),2), count(*)
--from 	tmp_rsa_step2b_NonMGSV_unallocated
--order by 1
--;
/* DEBUG: check for intersection >> shoudl return zero rows */
--select 	demand_group, material
--from 	tmp_rsa_step2b_NonMGSV_allocated
--intersect
--select 	demand_group, material
--from 	tmp_rsa_step2b_NonMGSV_allocated_fob
--;

/* DEBUG: confirm input ALLOCATED amount matches output amount */
--select 	'output - allocated', round(sum(alloc_amt_usd),2), count(*)
--from 	tmp_rsa_step2b_NonMGSV_allocated
--union all
--select 	'input - allocated', round(sum(amt_usd),2), count(*)
--from 	tmp_rsa_step2a rsa
--		inner join (
--			select 	distinct demand_group, material
--			from 	tmp_rsa_step2b_NonMGSV_rate
--		) as dg_sku
--			on 	lower(dg_sku.material) = lower(rsa.sku) and 
--				lower(dg_sku.demand_group) = lower(rsa.demand_group)
--where 	rsa.mgsv = 'Non-MGSV'
--order by 1
--;
/* DEBUG: confirm input UNALLOCATED amount matches output amount */
--select 	'output - unallocated', round(sum(amt_usd),2), count(*)
--from 	tmp_rsa_step2b_NonMGSV_unallocated
--union all
--select 	'input - unallocated', round(sum(amt_usd),2), count(*)
--from 	tmp_rsa_step2a rsa
--		left outer join (
--			select 	distinct demand_group, material
--			from 	tmp_rsa_step2b_NonMGSV_rate
--		) as dg_sku
--			on 	lower(dg_sku.material) = lower(rsa.sku) and 
--				lower(dg_sku.demand_group) = lower(rsa.demand_group)
--where 	rsa.mgsv = 'Non-MGSV' and 
--		dg_sku.material is null
--order by 1
--;

	/* STEP 02-C
	 * For a miscellaneous SKU: 
	 * 		Spread RSA $ across all soldto with a positive invoice sale (A40110)
	 * 		of the SKU within the month for the Demand Group 
	 * 
	 * 	NOTE: We are only allocating to perfect transactions (ruleid 1)
	 *	 	that had an invoiced sale (A40110) for that current month
	 *
	 */
/* DEBUG: example from excel */
--select 	*
--from 	tmp_rsa_step2a rsa
--where 	rsa.mgsv = 'MGSV' and 
--		lower(rsa.brand) = 'dewalt' and 
--		lower(rsa.demand_group) = 'hd'
--;


	drop table if exists tmp_rsa_step2c_MGSV_soldto_sku
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2c_MGSV_soldto_sku AS
		with
			cte_MGSV_dg_div_brand as (
				select 	distinct
						lower(rsa.EntitySourceRegion) as EntitySourceRegion,
						lower(rsa.demand_group) as demand_group,
						lower(rsa.division) as division,
						lower(rsa.brand) as brand
				from 	tmp_rsa_step2a as rsa
				where 	rsa.mgsv = 'MGSV' and 
						lower(rsa.demand_group) not in (
							select 	distinct lower(demand_group)
							from 	map_rsa_demandgroup_2_bar_custno
						)
			),
			cte_retail_customers as (
				select 	sbm.soldtocust 
				from 	ref_data.soldto_barcust_mapping sbm 
						inner join tmp_retail_bar_custno retail
							on lower(retail.bar_custno) = lower(sbm.bar_custno)
				where 	sbm.current_flag = 1
			)
		select 	distinct
				rsa.EntitySourceRegion,
				rsa.demand_group,
				dmd.kunnr as soldto_number,
				rsa.division,
				rsa.brand,
				sku.material
		from 	cte_MGSV_dg_div_brand as rsa
				inner join sapc11.kna1_current as dmd
					on	lower(rsa.demand_group) = lower(dmd.bran1)
				inner join cte_retail_customers retail_cust
					on 	lower(retail_cust.soldtocust) = lower(dmd.kunnr)
				inner join ref_data.sku_gpp_mapping sku
					on 	lower(sku.gpp_division) = lower(rsa.division) 
				inner join (
					select 	distinct 
							e.level5 as EntitySourceRegion,
							tr.mapped_bar_brand as brand,
							tr.material,
							tr.soldtocust
					from 	stage.bods_core_transaction_agg tr
							inner join vtbl_date_range as dt_rng
								on  dt_rng.fiscal_month_id = tr.fiscal_month_id
							inner join ref_data.entity e 
								on 	lower(e.name) = lower(tr.bar_entity)
					where 	tr.mapped_dataprocessing_ruleid = 1 and 
							/* sales invoice */
							tr.bar_acct = 'A40110' and 
							tr.bar_amt >= 0
				) as bods 
					on 	lower(bods.material) = lower(sku.material) and 
						lower(bods.soldtocust) = lower(dmd.kunnr) and
						lower(bods.brand) = lower(rsa.brand) and 
						lower(bods.EntitySourceRegion) = lower(rsa.EntitySourceRegion)
	;
	drop table if exists tmp_rsa_step2c_MGSV_soldto_sku_mapped
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2c_MGSV_soldto_sku_mapped AS
		with
			cte_MGSV_dg_div_brand_mapped as (
				select 	distinct 
						lower(rsa.EntitySourceRegion) as EntitySourceRegion,
						lower(rsa.demand_group) as demand_group,
						lower(mapping.bar_custno) as bar_custno,
						lower(rsa.division) as division,
						lower(rsa.brand) as brand
				from 	tmp_rsa_step2a as rsa
						inner join map_rsa_demandgroup_2_bar_custno as mapping
							on 	lower(mapping.demand_group) = lower(rsa.demand_group)
				where 	rsa.mgsv = 'MGSV'
			),
			cte_retail_customers as (
				select 	sbm.soldtocust 
				from 	ref_data.soldto_barcust_mapping sbm 
						inner join tmp_retail_bar_custno retail
							on lower(retail.bar_custno) = lower(sbm.bar_custno)
				where 	sbm.current_flag = 1
			)
		select 	distinct
				rsa.EntitySourceRegion,
				rsa.demand_group,
				sbm.soldtocust as soldto_number,
				rsa.division,
				rsa.brand,
				sku.material
		from 	cte_MGSV_dg_div_brand_mapped as rsa
				inner join ref_data.soldto_barcust_mapping sbm 
					on 	lower(sbm.bar_custno) = lower(rsa.bar_custno)
				inner join cte_retail_customers retail_cust
					on 	lower(retail_cust.soldtocust) = lower(sbm.soldtocust)
				inner join ref_data.sku_gpp_mapping sku
					on 	lower(sku.gpp_division) = lower(rsa.division) 
				inner join (
					select 	distinct 
							e.level5 as EntitySourceRegion,
							tr.mapped_bar_brand as brand,
							tr.material,
							tr.soldtocust
					from 	stage.bods_core_transaction_agg tr
							inner join vtbl_date_range as dt_rng
								on  dt_rng.fiscal_month_id = tr.fiscal_month_id 
							inner join ref_data.entity e 
								on 	lower(e.name) = lower(tr.bar_entity)
					where 	tr.mapped_dataprocessing_ruleid = 1 and 
							/* sales invoice */
							tr.bar_acct = 'A40110' and 
							tr.bar_amt >= 0
				) as bods 
					on 	lower(bods.material) = lower(sku.material) and 
						lower(bods.soldtocust) = lower(sbm.soldtocust) and
						lower(bods.brand) = lower(rsa.brand) and 
						lower(bods.EntitySourceRegion) = lower(rsa.EntitySourceRegion)
	;
	drop table if exists tmp_rsa_step2c_MGSV_soldto_sku_final
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2c_MGSV_soldto_sku_final AS
		select 	distinct demand_group, soldto_number, division, brand, material, EntitySourceRegion
		from 	tmp_rsa_step2c_MGSV_soldto_sku
		union all 
		select 	distinct demand_group, soldto_number, division, brand, material, EntitySourceRegion
		from 	tmp_rsa_step2c_MGSV_soldto_sku_mapped
	;

/* DEBUG: match soldto/sku combos in excel example 
 * 
 */
--SELECT 	'MATCH: 2460', count(distinct (lower(soldto_number) || '|' || lower(material)))
--FROM 	tmp_rsa_step2c_MGSV_soldto_sku 
--WHERE 	lower(demand_group) = 'hd' and 
--		lower(division) = '21' and 
--		lower(brand) = 'dewalt'
--;
	/* create rate base table */
	drop table if exists tmp_rsa_step2c_rate_base
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2c_rate_base AS
		select 	dt_rng.fiscal_month_id,
				e.level5 as EntitySourceRegion,
				soldto_sku.demand_group,
				soldto_sku.division,
				soldto_sku.brand,
				rb.soldtocust,
				rb.material,
				SUM(
					case 
						when xrt.fxrate is not null then cast(xrt.fxrate * rb.total_bar_amt as decimal(38, 8)) 
						else cast(rb.total_bar_amt as decimal(38, 8))
					end
				) as total_bar_amt_usd
		from 	stage.rate_base rb
				inner join ref_data.entity e 
					on 	lower(e."name") = lower(rb.bar_entity)
				inner join vtbl_date_range as dt_rng
					on  dt_rng.range_start_date = rb.range_start_date and 
						dt_rng.range_end_date = rb.range_end_date
				inner join tmp_rsa_step2c_MGSV_soldto_sku_final as soldto_sku 
					on 	lower(soldto_sku.soldto_number) = lower(rb.soldtocust) and 
						lower(soldto_sku.material) = lower(rb.material) and 
						lower(soldto_sku.EntitySourceRegion) = lower(e.level5)
				left outer join vtbl_exchange_rate xrt 
					on 	xrt.fiscal_month_id = dt_rng.fiscal_month_id and
						lower(xrt.from_currtype) = lower(rb.bar_currtype)
		group by
				dt_rng.fiscal_month_id,
				soldto_sku.demand_group,
				soldto_sku.division,
				soldto_sku.brand,
				rb.soldtocust,
				rb.material,
				e.level5
		having 	SUM(
					case 
						when xrt.fxrate is not null then cast(xrt.fxrate * rb.total_bar_amt as decimal(38, 8)) 
						else cast(rb.total_bar_amt as decimal(38, 8))
					end
				) > 0
	;

	/* create rate table for demand group / sku in RSA source */
	drop table if exists tmp_rsa_step2c_MGSV_rate
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2c_MGSV_rate AS
	with
		cte_rt_total as (
			select 	rt.fiscal_month_id,
					rt.EntitySourceRegion,
					rt.demand_group,
					rt.division,
					rt.brand,
					rt.soldtocust,
					rt.material,
					rt.total_bar_amt_usd as amt_usd,
					sum(rt.total_bar_amt_usd) over( 
						partition by rt.fiscal_month_id, lower(rt.demand_group), 
							lower(rt.division), lower(rt.brand), lower(rt.EntitySourceRegion)
					) as amt_usd_partition_total
			from 	tmp_rsa_step2c_rate_base rt 
			where 	rt.total_bar_amt_usd > 0 -- positive invoice sale
		)
		select	rtt.fiscal_month_id,
				rtt.EntitySourceRegion,
				rtt.demand_group,
				rtt.division,
				rtt.brand,
				rtt.soldtocust,
				rtt.material,
				CAST(rtt.amt_usd as decimal(20,8)) as amt_usd,
				CAST(rtt.amt_usd_partition_total AS DECIMAL(20,8)) as amt_usd_partition_total,
				CAST(rtt.amt_usd as decimal(20,8)) / 
					CAST(NULLIF(rtt.amt_usd_partition_total, 0) AS DECIMAL(20,8)) as pct_of_total
		from 	cte_rt_total as rtt
		where 	rtt.amt_usd_partition_total != 0
	;

/* DEBUG: clusters with cumulative pct of total != 100% */
--select 	rt.fiscal_month_id, lower(rt.demand_group) as demand_group, lower(rt.division) as division, lower(rt.brand) , sum(rt.pct_of_total)
--from 	tmp_rsa_step2c_MGSV_rate rt
--group by rt.fiscal_month_id, lower(rt.demand_group), lower(rt.division), lower(rt.brand) 
--having round(sum(rt.pct_of_total),4) != 1
--;

	/* allocate RSA amounts */
	drop table if exists tmp_rsa_step2c_MGSV_allocated
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2c_MGSV_allocated AS
		select 	rsa.mgsv,
				rsa.pcr,
				rsa.fiscal_month_id,
				
				rsa.EntitySourceRegion,
				rsa.demand_group,
				rt.soldtocust, 
				NULL as bar_custno,
				rt.material,
				NULL as bar_product,
				rsa.division,
				rsa.brand,
				
				CAST(rsa.amt_usd AS DECIMAL(20, 8)) as amt_usd,
				CAST(NULLIF(rt.pct_of_total, 0) AS DECIMAL(20, 8)) as alloc_pct,
				CAST(rsa.amt_usd AS DECIMAL(20, 8)) * CAST(rt.pct_of_total AS DECIMAL(20, 8)) as alloc_amt_usd
--				NVL(CAST(rsa.amt_usd AS DECIMAL(20, 8)) / (1 / CAST(NULLIF(rt.pct_of_total, 0) AS DECIMAL(20, 8))), 0) as alloc_amt_usd
		from 	tmp_rsa_step2a as rsa
				inner join tmp_rsa_step2c_MGSV_rate as rt
					on 	rt.fiscal_month_id = rsa.fiscal_month_id and 
						lower(rt.demand_group) = lower(rsa.demand_group) and 
						lower(rt.division) = lower(rsa.division) and 
						lower(rt.brand) = lower(rsa.brand) and 
						lower(rt.EntitySourceRegion) = lower(rsa.EntitySourceRegion)
		where 	rsa.mgsv = 'MGSV'
	;
	/* unallocated RSA amounts */
	drop table if exists tmp_rsa_step2c_MGSV_unallocated
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2c_MGSV_unallocated AS
		select 	rsa.mgsv,
				rsa.pcr,
				rsa.fiscal_month_id,
				
				rsa.EntitySourceRegion,
				rsa.demand_group,
				'RSA_MGSV_Unallocated' as soldtocust, 
				'RSA_MGSV_Unallocated' as bar_custno, 
				
				'RSA_MGSV_Unallocated' as material,
				'RSA_MGSV_Unallocated' as bar_product,
				rsa.division,
				rsa.brand,
				
				CAST(rsa.amt_usd AS DECIMAL(16, 8)) as amt_usd
		from 	tmp_rsa_step2a as rsa
				left outer join tmp_rsa_step2c_MGSV_rate as rt
					on 	rt.fiscal_month_id = rsa.fiscal_month_id and 
						lower(rt.demand_group) = lower(rsa.demand_group) and 
						lower(rt.division) = lower(rsa.division) and 
						lower(rt.brand) = lower(rsa.brand) and 
						lower(rt.EntitySourceRegion) = lower(rsa.EntitySourceRegion)
		where 	rsa.mgsv = 'MGSV' and 
				rt.brand is null
	;


/* DEBUG: confirm input amount matches output amount */
--select 	'input', round(sum(amt_usd),2), count(*)
--from 	tmp_rsa_step2a where mgsv = 'MGSV'
--union all
--select 	'output - allocated', round(sum(alloc_amt_usd),2), count(*)
--from 	tmp_rsa_step2c_MGSV_allocated
--union all
--select 	'output - unallocated', round(sum(amt_usd),2), count(*)
--from 	tmp_rsa_step2c_MGSV_unallocated
--order by 1
--;
/* DEBUG: confirm input ALLOCATED amount matches output amount */
--select 	'output - allocated', round(sum(alloc_amt_usd),2), count(*)
--from 	tmp_rsa_step2c_MGSV_allocated
--union all
--select 	'input - allocated', round(sum(amt_usd),2), count(*)
--from 	tmp_rsa_step2a rsa
--		inner join (
--			select 	distinct demand_group, brand, division
--			from 	tmp_rsa_step2c_MGSV_rate
--		) as dg_sku
--			on 	lower(dg_sku.demand_group) = lower(rsa.demand_group) and 
--				lower(dg_sku.division) = lower(rsa.division) and 
--				lower(dg_sku.brand) = lower(rsa.brand)
--where 	rsa.mgsv = 'MGSV'
--order by 1
--;
/* DEBUG: confirm input UNALLOCATED amount matches output amount */
--select 	'output - unallocated', round(sum(amt_usd),2), count(*)
--from 	tmp_rsa_step2c_MGSV_unallocated
--union all
--select 	'input - unallocated', round(sum(amt_usd),2), count(*)
--from 	tmp_rsa_step2a rsa
--		left outer join (
--			select 	distinct demand_group, brand, division
--			from 	tmp_rsa_step2c_MGSV_rate
--		) as dg_sku
--			on 	lower(dg_sku.demand_group) = lower(rsa.demand_group) and 
--				lower(dg_sku.division) = lower(rsa.division) and 
--				lower(dg_sku.brand) = lower(rsa.brand)
--where 	rsa.mgsv = 'MGSV' and 
--		dg_sku.brand is null
--order by 1
--;
	/* STEP 02-D - fill out rest of transactions: 
	 *		add Customer, Product, & Entity Info
	 */
	drop table if exists tmp_rsa_step2d_alloc
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2d_alloc AS
	with
		cte_rsa_alloc as (
			select 	rsa.mgsv,
					rsa.pcr,
					rsa.fiscal_month_id,
					rsa.EntitySourceRegion,
					rsa.demand_group,
					rsa.soldtocust,
					rsa.material,
					rsa.division as gpp_division,
					rsa.brand,
					cast(rsa.amt_usd as decimal(20,8)) as amt_usd,
					cast(rsa.alloc_pct as decimal(20,8)) as alloc_pct,
					cast(rsa.alloc_amt_usd as decimal(20,8)) as alloc_amt_usd,
					'non-mgsv allocated' as rsa_tran_group
			from 	tmp_rsa_step2b_NonMGSV_allocated rsa
			UNION ALL 
			select 	rsa.mgsv,
					rsa.pcr,
					rsa.fiscal_month_id,
					rsa.EntitySourceRegion,
					rsa.demand_group,
					rsa.soldtocust,
					rsa.material,
					rsa.division as gpp_division,
					rsa.brand,
					cast(rsa.amt_usd as decimal(20,8)) as amt_usd,
					cast(rsa.alloc_pct as decimal(20,8)) as alloc_pct,
					cast(rsa.alloc_amt_usd as decimal(20,8)) as alloc_amt_usd,
					'mgsv allocated' as rsa_tran_group
			from 	tmp_rsa_step2c_MGSV_allocated rsa
		)
		select 	rsa.mgsv,
				rsa.pcr,
				rsa.fiscal_month_id,
				rsa.EntitySourceRegion,
				rsa.demand_group,
				rsa.soldtocust,
				COALESCE(custmap.bar_custno, 'unknown') as bar_custno,
				rsa.material,
				COALESCE(prodmap.bar_product, 'unknown') as bar_product,
				rsa.gpp_division,
				rsa.brand,
				rsa.amt_usd,
				rsa.alloc_pct,
				rsa.alloc_amt_usd,
				rsa.rsa_tran_group
		from 	cte_rsa_alloc rsa
				left outer join ref_data.soldto_barcust_mapping as custmap 
					on 	lower(custmap.soldtocust) = lower(rsa.soldtocust) and custmap.current_flag = 1
				left outer join ref_data.sku_barproduct_mapping as prodmap 
					on 	lower(prodmap.material) = lower(rsa.material) and prodmap.current_flag = 1
	;
	drop table if exists tmp_rsa_step2d_alloc_fob
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2d_alloc_fob AS
	with
		cte_rsa_alloc as (
			select 	rsa.mgsv,
					rsa.pcr,
					rsa.fiscal_month_id,
					rsa.EntitySourceRegion,
					rsa.demand_group,
					rsa.soldtocust,
					rsa.material,
					rsa.division as gpp_division,
					rsa.brand,
					cast(rsa.amt_usd as decimal(20,8)) as amt_usd,
					cast(rsa.alloc_pct as decimal(20,8)) as alloc_pct,
					cast(rsa.alloc_amt_usd as decimal(20,8)) as alloc_amt_usd,
					'non-mgsv allocated' as rsa_tran_group
			from 	tmp_rsa_step2b_NonMGSV_allocated_fob rsa
		)
		select 	rsa.mgsv,
				rsa.pcr,
				rsa.fiscal_month_id,
				rsa.EntitySourceRegion,
				rsa.demand_group,
				rsa.soldtocust,
				COALESCE(custmap.bar_custno, 'unknown') as bar_custno,
				rsa.material,
				COALESCE(prodmap.bar_product, 'unknown') as bar_product,
				rsa.gpp_division,
				rsa.brand,
				rsa.amt_usd,
				rsa.alloc_pct,
				rsa.alloc_amt_usd,
				rsa.rsa_tran_group
		from 	cte_rsa_alloc rsa
				left outer join map_rsa_demandgroup_2_bar_custno custmap
					on 	lower(custmap.demand_group) = lower(rsa.demand_group)
				left outer join ref_data.sku_barproduct_mapping as prodmap 
					on 	lower(prodmap.material) = lower(rsa.material) and prodmap.current_flag = 1
	; 

/* DEBUG: compare input and output */
--select 	'input', round(sum(amt),2) as amt, sum(cnt) as cnt
--from 	(
--			select 	round(sum(alloc_amt_usd),2) as amt, count(*) as cnt
--			from 	tmp_rsa_step2b_NonMGSV_allocated
--			union all 
--			select 	round(sum(alloc_amt_usd),2) as amt, count(*) as cnt
--			from 	tmp_rsa_step2b_NonMGSV_allocated_fob
--			union all 
--			select 	round(sum(alloc_amt_usd),2) as amt, count(*) as cnt
--			from 	tmp_rsa_step2c_MGSV_allocated
--		)
--union all 
--select 	'output', round(sum(amt),2) as amt, sum(cnt) as cnt
--from 	(
--			select 	round(sum(alloc_amt_usd),2) as amt, count(*) as cnt
--			from 	tmp_rsa_step2d_alloc
--			union all 
--			select 	round(sum(alloc_amt_usd),2) as amt, count(*) as cnt
--			from 	tmp_rsa_step2d_alloc_fob
--		)
--order by 1
--;
	drop table if exists tmp_rsa_step2d_unalloc
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2d_unalloc AS
		select 	rsa.mgsv,
				rsa.pcr,
				rsa.fiscal_month_id,
				rsa.EntitySourceRegion,
				rsa.demand_group,
				rsa.soldtocust,
				'RSA_Non-MGSV_Unallocated' as bar_custno,
				rsa.material,
				COALESCE(prodmap.bar_product, 'unknown') as bar_product,
				rsa.division,
				rsa.brand,
				cast(rsa.amt_usd as decimal(20,8)) as amt_usd,
				cast(NULL as decimal(20,8)) AS alloc_pct,
				cast(NULL as decimal(20,8)) AS alloc_amt_usd,
				'non-mgsv unallocated' as rsa_tran_group
		from 	tmp_rsa_step2b_NonMGSV_unallocated rsa
				left outer join ref_data.sku_barproduct_mapping as prodmap 
					on 	lower(prodmap.material) = lower(rsa.material) and prodmap.current_flag = 1
		UNION ALL 
		select 	rsa.mgsv,
				rsa.pcr,
				rsa.fiscal_month_id,
				rsa.EntitySourceRegion,
				rsa.demand_group,
				rsa.soldtocust,
				'RSA_Non-MGSV_Unallocated' as bar_custno,
				rsa.material,
				'RSA_Non-MGSV_Unallocated' as bar_product,
				rsa.division,
				rsa.brand,
				cast(rsa.amt_usd as decimal(20,8)) as amt_usd,
				cast(NULL as decimal(20,8)) AS alloc_pct,
				cast(NULL as decimal(20,8)) AS alloc_amt_usd,
				'mgsv unallocated' as rsa_tran_group
		from 	tmp_rsa_step2c_MGSV_unallocated rsa
	;


	/* create rate base table for bar_entity */
	drop table if exists tmp_rsa_step2d_rate_base
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2d_rate_base AS
		select 	dt_rng.fiscal_month_id,
				rb.bar_custno,
				rb.bar_entity,
				e.level5 as EntitySourceRegion,
				SUM(
					case 
						when xrt.fxrate is not null then cast(xrt.fxrate * rb.total_bar_amt as decimal(38, 8)) 
						else cast(rb.total_bar_amt as decimal(38, 8))
					end
				) as total_bar_amt_usd
		from 	stage.rate_base rb
				inner join ref_data.entity e 
					on 	lower(e.name) = lower(rb.bar_entity)
				inner join vtbl_date_range as dt_rng
					on  dt_rng.range_start_date = rb.range_start_date and 
						dt_rng.range_end_date = rb.range_end_date
				left outer join vtbl_exchange_rate xrt 
					on 	xrt.fiscal_month_id = dt_rng.fiscal_month_id and
						lower(xrt.from_currtype) = lower(rb.bar_currtype)
		group by
				dt_rng.fiscal_month_id,
				rb.bar_custno,
				rb.bar_entity,
				e.level5
		having 	SUM(
					case 
						when xrt.fxrate is not null then cast(xrt.fxrate * rb.total_bar_amt as decimal(38, 8)) 
						else cast(rb.total_bar_amt as decimal(38, 8))
					end
				) > 0
	;

	/* create rate table for bar_entity */
	drop table if exists tmp_rsa_step2d_rate
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2d_rate AS
	with
		cte_rt_total as (
			select 	rt.fiscal_month_id,
					rt.EntitySourceRegion,
					rt.bar_custno, 
					rt.bar_entity,
					rt.total_bar_amt_usd as amt_usd,
					sum(rt.total_bar_amt_usd) over( 
						partition by rt.fiscal_month_id, lower(rt.bar_custno), lower(rt.EntitySourceRegion)
					) as amt_usd_partition_total
			from 	tmp_rsa_step2d_rate_base rt 
			where 	rt.total_bar_amt_usd > 0
		)
		select	rtt.fiscal_month_id,
				rtt.EntitySourceRegion,
				rtt.bar_custno,
				rtt.bar_entity,
				CAST(rtt.amt_usd as decimal(20,8)) as amt_usd,
				CAST(rtt.amt_usd_partition_total AS DECIMAL(20,8)) as amt_usd_partition_total,
				CAST(rtt.amt_usd as decimal(20,8)) / 
					CAST(NULLIF(rtt.amt_usd_partition_total, 0) AS DECIMAL(20,8)) as pct_of_total
		from 	cte_rt_total as rtt
		where 	rtt.amt_usd_partition_total != 0
	;

/* DEBUG: clusters with cumulative pct of total != 100% */
--select 	rt.fiscal_month_id, lower(rt.bar_custno), sum(rt.pct_of_total)
--from 	tmp_rsa_step2d_rate rt
--group by rt.fiscal_month_id, lower(rt.bar_custno)
--having round(sum(rt.pct_of_total),4) != 1
--order by 3 asc
--;
	/* allocate transactions across bar_entity */
	drop table if exists tmp_rsa_step2d_alloc_entity
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2d_alloc_entity AS
		select 	rsa.mgsv,
				rsa.pcr,
				rsa.fiscal_month_id,
				rsa.EntitySourceRegion,
				rsa.demand_group,
				rsa.soldtocust,
				rsa.bar_custno,
				rsa.material,
				rsa.bar_product,
				rsa.gpp_division,
				rsa.brand,
				rsa.rsa_tran_group,
				rt.bar_entity,
				CAST(rsa.alloc_amt_usd AS DECIMAL(20, 8)) as amt_usd,
				CAST(NULLIF(rt.pct_of_total, 0) AS DECIMAL(16, 8)) as alloc_pct,
				NVL(CAST(rsa.alloc_amt_usd AS DECIMAL(20, 8)) / (1 / CAST(NULLIF(rt.pct_of_total, 0) AS DECIMAL(16, 8))), 0) as alloc_amt_usd
		from 	tmp_rsa_step2d_alloc as rsa
				inner join tmp_rsa_step2d_rate as rt
					on 	rt.fiscal_month_id = rsa.fiscal_month_id and 
						lower(rt.bar_custno) = lower(rsa.bar_custno) and 
						lower(rt.EntitySourceRegion) = lower(rsa.EntitySourceRegion)
	;
	/* create rate base fob table for bar_entity  */
	drop table if exists tmp_rsa_step2d_rate_base_fob
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2d_rate_base_fob AS
		select 	dt_rng.fiscal_month_id,
				rb.mapped_bar_custno as bar_custno,
				rb.bar_entity,
				e.level5 as EntitySourceRegion,
				SUM(
					case 
						when xrt.fxrate is not null then cast(xrt.fxrate * rb.allocated_amt as decimal(38, 8)) 
						else cast(rb.allocated_amt as decimal(38, 8))
					end
				) as total_bar_amt_usd
		from 	stage.sgm_allocated_data_rule_21 rb
				inner join ref_data.entity e 
					on 	lower(e.name) = lower(rb.bar_entity)
				inner join vtbl_date_range as dt_rng
					on  dt_rng.fiscal_month_id = rb.fiscal_month_id
				left outer join vtbl_exchange_rate xrt 
					on 	xrt.fiscal_month_id = dt_rng.fiscal_month_id and
						lower(xrt.from_currtype) = lower(rb.bar_currtype)
		where 	rb.dataprocessing_outcome_id = 1
		group by
				dt_rng.fiscal_month_id,
				rb.mapped_bar_custno,
				rb.bar_entity,
				e.level5
		having 	SUM(
					case 
						when xrt.fxrate is not null then cast(xrt.fxrate * rb.allocated_amt as decimal(38, 8)) 
						else cast(rb.allocated_amt as decimal(38, 8))
					end
				) > 0
	;
	/* create rate fob table for bar_entity */
	drop table if exists tmp_rsa_step2d_rate_fob
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2d_rate_fob AS
	with
		cte_rt_total as (
			select 	rt.fiscal_month_id,
					rt.EntitySourceRegion,
					rt.bar_custno, 
					rt.bar_entity,
					rt.total_bar_amt_usd as amt_usd,
					sum(rt.total_bar_amt_usd) over( 
						partition by rt.fiscal_month_id, lower(rt.bar_custno), lower(rt.EntitySourceRegion)
					) as amt_usd_partition_total
			from 	tmp_rsa_step2d_rate_base_fob rt 
			where 	rt.total_bar_amt_usd > 0
		)
		select	rtt.fiscal_month_id,
				rtt.EntitySourceRegion,
				rtt.bar_custno,
				rtt.bar_entity,
				CAST(rtt.amt_usd as decimal(20,8)) as amt_usd,
				CAST(rtt.amt_usd_partition_total AS DECIMAL(20,8)) as amt_usd_partition_total,
				CAST(rtt.amt_usd as decimal(20,8)) / 
					CAST(NULLIF(rtt.amt_usd_partition_total, 0) AS DECIMAL(20,8)) as pct_of_total
		from 	cte_rt_total as rtt
		where 	rtt.amt_usd_partition_total != 0
	;

/* DEBUG: clusters with cumulative pct of total != 100% */
--select 	rt.fiscal_month_id, lower(rt.bar_custno), sum(rt.pct_of_total)
--from 	tmp_rsa_step2d_rate_fob rt
--group by rt.fiscal_month_id, lower(rt.bar_custno)
--having round(sum(rt.pct_of_total),4) != 1
--order by 3 asc
--;
	/* allocate transactions across bar_entity */
	drop table if exists tmp_rsa_step2d_alloc_entity_fob
	;
	CREATE TEMPORARY TABLE tmp_rsa_step2d_alloc_entity_fob AS
		select 	rsa.mgsv,
				rsa.pcr,
				rsa.fiscal_month_id,
				rsa.EntitySourceRegion,
				rsa.demand_group,
				rsa.soldtocust,
				rsa.bar_custno,
				rsa.material,
				rsa.bar_product,
				rsa.gpp_division,
				rsa.brand,
				rsa.rsa_tran_group,
				rt.bar_entity,
				CAST(rsa.alloc_amt_usd AS DECIMAL(20, 8)) as amt_usd,
				CAST(NULLIF(rt.pct_of_total, 0) AS DECIMAL(16, 8)) as alloc_pct,
				NVL(CAST(rsa.alloc_amt_usd AS DECIMAL(20, 8)) / (1 / CAST(NULLIF(rt.pct_of_total, 0) AS DECIMAL(16, 8))), 0) as alloc_amt_usd
		from 	tmp_rsa_step2d_alloc_fob as rsa
				inner join tmp_rsa_step2d_rate_fob as rt
					on 	rt.fiscal_month_id = rsa.fiscal_month_id and 
						lower(rt.bar_custno) = lower(rsa.bar_custno) and 
						lower(rt.EntitySourceRegion) = lower(rsa.EntitySourceRegion)
	;

/* DEBUG: compare input and output */
--select 	'input', round(sum(alloc_amt_usd),2) as amt, count(*)
--from 	tmp_rsa_step2d_alloc
--union all 
--select 	'input-fob', round(sum(alloc_amt_usd),2) as amt, count(*)
--from 	tmp_rsa_step2d_alloc_fob
--union all 
--select 	'output', round(sum(alloc_amt_usd),2) as amt, count(*)
--from 	tmp_rsa_step2d_alloc_entity
--union all 
--select 	'output-fob', round(sum(alloc_amt_usd),2) as amt, count(*)
--from 	tmp_rsa_step2d_alloc_entity_fob
--union all 
--select 	'input-unalloc', round(sum(amt_usd),2) as amt, count(*)
--from 	tmp_rsa_step2d_unalloc
--order by 1
--;

/*
--                                                                      
--   ad88888ba  888888888888  88888888888  88888888ba       ad888888b,  
--  d8"     "8b      88       88           88      "8b     d8"     "88  
--  Y8,              88       88           88      ,8P             a8P  
--  `Y8aaaaa,        88       88aaaaa      88aaaaaa8P'          aad8"   
--    `"""""8b,      88       88"""""      88""""""'            ""Y8,   
--          `8b      88       88           88                      "8b  
--  Y8a     a8P      88       88           88              Y8,     a88  
--   "Y88888P"       88       88888888888  88               "Y888888P'  
--                                                                      
*/
	/* Step 03a RSA amounts in BODS by bar customer/divison */
	drop table if exists tmp_rsa_input
	;
	CREATE TEMPORARY TABLE tmp_rsa_input AS
		select 	lower(rsa.bar_custno) as bar_custno,
				lower(rsa.EntitySourceRegion) as EntitySourceRegion,
				lower(rsa.division) as division, 
				round(sum(rsa.total_rsa_amt_usd),2) as bods_rsa_amt_usd
		from 	tmp_rsa_c11_retail_step1 as rsa
		group by lower(rsa.bar_custno),
				lower(rsa.EntitySourceRegion),
				lower(rsa.division)
	;
	/* Step 03b (NonMGSV) RSA amounts in RSA Bible by bar customer/divison */
	drop table if exists tmp_rsa_output_allocated_nonmgsv
	;
	CREATE TEMPORARY TABLE tmp_rsa_output_allocated_nonmgsv AS
		select 	rsa.bar_custno,
				rsa.EntitySourceRegion,
				div_map.division,
				round(sum(alloc_amt_usd),2) as rsa_alloc_amt_usd
		from 	tmp_rsa_step2d_alloc_entity rsa
				inner join tmp_bar_product_division_mapping div_map
					on 	lower(div_map.bar_product) = lower(rsa.bar_product)
		where 	rsa.mgsv = 'Non-MGSV'
		group by rsa.bar_custno,
				rsa.EntitySourceRegion,
				div_map.division
	;
	/* Step 03b FOB (NonMGSV) RSA amounts in RSA Bible by bar customer/divison */
	drop table if exists tmp_rsa_output_allocated_nonmgsv_fob
	;
	CREATE TEMPORARY TABLE tmp_rsa_output_allocated_nonmgsv_fob AS
		select 	rsa.bar_custno,
				div_map.division,
				rsa.EntitySourceRegion,
				round(sum(alloc_amt_usd),2) as rsa_alloc_amt_usd
		from 	tmp_rsa_step2d_alloc_entity_fob rsa
				inner join tmp_bar_product_division_mapping div_map
					on 	lower(div_map.bar_product) = lower(rsa.bar_product)
		where 	rsa.mgsv = 'Non-MGSV'
		group by rsa.bar_custno,
				div_map.division,
				rsa.EntitySourceRegion
	;
	/* Step 03b (MGSV) RSA amounts in RSA Bible by bar customer/divison */
	drop table if exists tmp_rsa_output_allocated_mgsv
	;
	CREATE TEMPORARY TABLE tmp_rsa_output_allocated_mgsv AS
		select 	rsa.bar_custno,
				div_map.division,
				rsa.EntitySourceRegion,
				round(sum(alloc_amt_usd),2) as rsa_alloc_amt_usd
		from 	tmp_rsa_step2d_alloc_entity rsa
				inner join tmp_bar_product_division_mapping div_map
					on 	lower(div_map.bar_product) = lower(rsa.bar_product)
		where 	rsa.mgsv = 'MGSV'
		group by rsa.bar_custno,
				div_map.division,
				rsa.EntitySourceRegion
	;
	/* Step 03b RSA amounts in RSA Bible by bar customer/divison */
	drop table if exists tmp_rsa_output_allocated
	;
	CREATE TEMPORARY TABLE tmp_rsa_output_allocated AS
	with
		cte_combined as (
			select 	bar_custno,
					division,
					EntitySourceRegion,
					rsa_alloc_amt_usd
			from 	tmp_rsa_output_allocated_nonmgsv 
			union all 
			select 	bar_custno,
					division,
					EntitySourceRegion,
					rsa_alloc_amt_usd
			from 	tmp_rsa_output_allocated_nonmgsv_fob
			union all 
			select 	bar_custno,
					division,
					EntitySourceRegion,
					rsa_alloc_amt_usd
			from 	tmp_rsa_output_allocated_mgsv 
		)
		select 	lower(bar_custno) as bar_custno,
				lower(division) as division,
				lower(EntitySourceRegion) as EntitySourceRegion,
				round(sum(rsa_alloc_amt_usd),2) as rsa_alloc_amt_usd
		from 	cte_combined
		group by lower(bar_custno),
				lower(division),
				lower(EntitySourceRegion)
	;
	/* Step 03c Reconcile Transactions (GAP)
	 * 
	 * 	gap amount for bar_custno/division combinations that exist in:
	 * 			BODS-only
	 * 			RSA-only
	 * 			Both
	 */
	drop table if exists tmp_rsa_output_gap
	;
	CREATE TEMPORARY TABLE tmp_rsa_output_gap AS
		select	case 
					when bods.bar_custno is not null and rsa_alloc.bar_custno is not null then 'both'
					when bods.bar_custno is not null and rsa_alloc.bar_custno is null then 'BODS-only'
					when bods.bar_custno is null and rsa_alloc.bar_custno is not null then 'RSA-only'
				end as row_group,
				
				lower(bods.EntitySourceRegion) as bods_EntitySourceRegion,
				lower(bods.bar_custno) as bods_bar_custno,
				lower(bods.division) as bods_bar_division,
				NVL(bods.bods_rsa_amt_usd,0) as bods_amt_usd,
				
				lower(rsa_alloc.EntitySourceRegion) as rsa_EntitySourceRegion,
				lower(rsa_alloc.bar_custno) as rsa_bar_custno,
				lower(rsa_alloc.division) as rsa_bar_division,
				NVL(rsa_alloc.rsa_alloc_amt_usd,0) as rsa_amt_usd,
				
				NVL(bods.bods_rsa_amt_usd,0) - NVL(rsa_alloc.rsa_alloc_amt_usd,0) as gap_amt_usd
		from 	tmp_rsa_input as bods
				left outer join tmp_rsa_output_allocated as rsa_alloc
					on 	lower(rsa_alloc.bar_custno) = lower(bods.bar_custno) and 
						lower(rsa_alloc.division) = lower(bods.division) and 
						lower(rsa_alloc.EntitySourceRegion) = lower(bods.EntitySourceRegion)
		where 	rsa_alloc.bar_custno is null
		union all
		select	case 
					when bods.bar_custno is not null and rsa_alloc.bar_custno is not null then 'both'
					when bods.bar_custno is not null and rsa_alloc.bar_custno is null then 'BODS-only'
					when bods.bar_custno is null and rsa_alloc.bar_custno is not null then 'RSA-only'
				end as row_group,
				
				lower(bods.EntitySourceRegion) as bods_EntitySourceRegion,
				lower(bods.bar_custno) as bods_bar_custno,
				lower(bods.division) as bods_bar_division,
				NVL(bods.bods_rsa_amt_usd,0) as bods_amt_usd,
				
				lower(rsa_alloc.EntitySourceRegion) as rsa_EntitySourceRegion,
				lower(rsa_alloc.bar_custno) as rsa_bar_custno,
				lower(rsa_alloc.division) as rsa_bar_division,
				NVL(rsa_alloc.rsa_alloc_amt_usd,0) as rsa_amt_usd,
				
				NVL(bods.bods_rsa_amt_usd,0) - NVL(rsa_alloc.rsa_alloc_amt_usd,0) as gap_amt_usd
		from 	tmp_rsa_input as bods
				inner join tmp_rsa_output_allocated as rsa_alloc
					on 	lower(rsa_alloc.bar_custno) = lower(bods.bar_custno) and 
						lower(rsa_alloc.division) = lower(bods.division) and 
						lower(rsa_alloc.EntitySourceRegion) = lower(bods.EntitySourceRegion)
		union all
		select	case 
					when bods.bar_custno is not null and rsa_alloc.bar_custno is not null then 'both'
					when bods.bar_custno is not null and rsa_alloc.bar_custno is null then 'BODS-only'
					when bods.bar_custno is null and rsa_alloc.bar_custno is not null then 'RSA-only'
				end as row_group,
				
				lower(bods.EntitySourceRegion) as bods_EntitySourceRegion,
				lower(bods.bar_custno) as bods_bar_custno,
				lower(bods.division) as bods_bar_division,
				NVL(bods.bods_rsa_amt_usd,0) as bods_amt_usd,
				
				lower(rsa_alloc.EntitySourceRegion) as rsa_EntitySourceRegion,
				lower(rsa_alloc.bar_custno) as rsa_bar_custno,
				lower(rsa_alloc.division) as rsa_bar_division,
				NVL(rsa_alloc.rsa_alloc_amt_usd,0) as rsa_amt_usd,
				
				NVL(bods.bods_rsa_amt_usd,0) - NVL(rsa_alloc.rsa_alloc_amt_usd,0) as gap_amt_usd
		from 	tmp_rsa_input as bods
				right outer join tmp_rsa_output_allocated as rsa_alloc
					on 	lower(rsa_alloc.bar_custno) = lower(bods.bar_custno) and 
						lower(rsa_alloc.division) = lower(bods.division) and 
						lower(rsa_alloc.EntitySourceRegion) = lower(bods.EntitySourceRegion)
		where 	bods.bar_custno is null
	;
	drop table if exists tmp_rsa_output_gap_summarized
	;
	CREATE TEMPORARY TABLE tmp_rsa_output_gap_summarized AS
		select	COALESCE( bods_bar_custno, rsa_bar_custno) as bar_custno,
				COALESCE( bods_bar_division, rsa_bar_division) as division,
				COALESCE( bods_EntitySourceRegion, rsa_EntitySourceRegion) as EntitySourceRegion,
				SUM(gap_amt_usd) as gap_amt_usd
		from 	tmp_rsa_output_gap gap
		group by 
				COALESCE( bods_bar_custno, rsa_bar_custno),
				COALESCE( bods_bar_division, rsa_bar_division),
				COALESCE( bods_EntitySourceRegion, rsa_EntitySourceRegion)
	;
	/* create rate table by bar_custno and allocate across entities */
	drop table if exists tmp_adj_entity_rate_base
	;
	CREATE TEMPORARY TABLE tmp_adj_entity_rate_base AS
		select 	dt_rng.fiscal_month_id,
				rb.bar_custno,
				rb.bar_entity,
				e.level5 as EntitySourceRegion,
				SUM(
					case 
						when xrt.fxrate is not null then cast(xrt.fxrate * rb.total_bar_amt as decimal(38, 8)) 
						else cast(rb.total_bar_amt as decimal(38, 8))
					end
				) as total_bar_amt_usd
		from 	stage.rate_base rb
				inner join ref_data.entity e 
					on 	lower(e.name) = lower(rb.bar_entity)
				inner join vtbl_date_range as dt_rng
					on  dt_rng.range_start_date = rb.range_start_date and 
						dt_rng.range_end_date = rb.range_end_date
				left outer join vtbl_exchange_rate xrt 
					on 	xrt.fiscal_month_id = dt_rng.fiscal_month_id and
						lower(xrt.from_currtype) = lower(rb.bar_currtype)
		group by
				dt_rng.fiscal_month_id,
				rb.bar_custno,
				rb.bar_entity,
				e.level5
		having 	SUM(
					case 
						when xrt.fxrate is not null then cast(xrt.fxrate * rb.total_bar_amt as decimal(38, 8)) 
						else cast(rb.total_bar_amt as decimal(38, 8))
					end
				) > 0
	;
	drop table if exists tmp_adj_entity_rate
	;
	CREATE TEMPORARY TABLE tmp_adj_entity_rate AS
	with
		cte_rt_total as (
			select 	rt.fiscal_month_id,
					rt.bar_custno,
					rt.bar_entity,
					rt.EntitySourceRegion,
					rt.total_bar_amt_usd as amt_usd,
					sum(rt.total_bar_amt_usd) over( 
						partition by rt.fiscal_month_id, lower(rt.bar_custno), lower(rt.EntitySourceRegion)
					) as amt_usd_partition_total
			from 	tmp_adj_entity_rate_base rt 
			where 	rt.total_bar_amt_usd > 0
		)
		select	rtt.fiscal_month_id,
				rtt.bar_custno,
				rtt.bar_entity,
				rtt.EntitySourceRegion,
				CAST(rtt.amt_usd as decimal(20,8)) as amt_usd,
				CAST(rtt.amt_usd_partition_total AS DECIMAL(20,8)) as amt_usd_partition_total,
				CAST(rtt.amt_usd as decimal(20,8)) / 
					CAST(NULLIF(rtt.amt_usd_partition_total, 0) AS DECIMAL(20,8)) as pct_of_total
		from 	cte_rt_total as rtt
		where 	rtt.amt_usd_partition_total != 0
	;
/* DEBUG: clusters with cumulative pct of total != 100% */
--select 	rt.fiscal_month_id, lower(rt.bar_custno), sum(rt.pct_of_total)
--from 	tmp_adj_entity_rate rt
--group by rt.fiscal_month_id, lower(rt.bar_custno)
--having round(sum(rt.pct_of_total),4) != 1
--order by 3 asc
--;
	/* allocate ADJ_RSA across entities by bar_custno */
	drop table if exists tmp_rsa_output_gap_summarized_allocated
	;
	CREATE TEMPORARY TABLE tmp_rsa_output_gap_summarized_allocated AS
		select 	rt.fiscal_month_id,
				rt.bar_custno,
				rt.bar_entity,
				gap.EntitySourceRegion,
				gap.division,
				CAST(gap.gap_amt_usd AS DECIMAL(16, 8)) as amt_usd,
				CAST(NULLIF(rt.pct_of_total, 0) AS DECIMAL(16, 8)) as alloc_pct,
				NVL(CAST(gap.gap_amt_usd AS DECIMAL(16, 8)) / (1 / CAST(NULLIF(rt.pct_of_total, 0) AS DECIMAL(16, 8))), 0) as alloc_amt_usd
		from 	tmp_rsa_output_gap_summarized as gap
				inner join tmp_adj_entity_rate as rt
					on 	lower(rt.bar_custno) = lower(gap.bar_custno) and 
						lower(rt.EntitySourceRegion) = lower(gap.EntitySourceRegion)
	;
/* DEBUG: input = output */
--select 	1, 'input', round(sum(gap_amt_usd),2) as amt_usd
--from 	tmp_rsa_output_gap_summarized
--UNION ALL 
--select 	2, 'output', round(sum(alloc_amt_usd),2) as amt_usd
--from 	tmp_rsa_output_gap_summarized_allocated
--order by 1
--;
	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	stage.sgm_allocated_data_rule_23
	where 	fiscal_month_id = (select fiscal_month_id from vtbl_date_range)
		and source_system = 'rsa_bible'
	;
	/* load to final transaction table (RSA Alloc) */
	INSERT INTO stage.sgm_allocated_data_rule_23 (
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
				rsa_reconcile_bar_custno,
				rsa_reconcile_bar_division,
				bar_currtype,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_id,
				dataprocessing_phase,
				allocated_amt,
				rsa_mgsv,
				rsa_pcr,
				audit_loadts
		)
		select 	'rsa_bible' as source_system,
				-1 as org_tranagg_id,
				dt_rng.range_end_date as posting_week_enddate,
				dt_rng.fiscal_month_id,
				rsa.bar_entity,
				'A40115' as bar_acct,
				
				rsa.brand as org_bar_brand,
				NULL as org_bar_custno,
				NULL as org_bar_product,
				coalesce(sbm.bar_brand, rsa.brand) as mapped_bar_brand,
				rsa.bar_custno as mapped_bar_custno,
				rsa.bar_product as mapped_bar_product,
				'unknown' as org_shiptocust,
				rsa.soldtocust as org_soldtocust,
				rsa.material as org_material,
				'unknown' as alloc_shiptocust,
				rsa.soldtocust as alloc_soldtocust,
				rsa.material as alloc_material,
				rsa.bar_product as alloc_bar_product,
				
				cast(null as varchar(50)) as rsa_reconcile_bar_custno,
				cast(null as varchar(50)) as rsa_reconcile_bar_division,
				
				case 
					when lower(rsa.EntitySourceRegion) = 'gts_ca' then 'cad'
					else 'usd' 
				end as bar_currtype,
				dpr.data_processing_ruleid as org_dataprocessing_ruleid,
				dpr.data_processing_ruleid as mapped_dataprocessing_ruleid,
				1 as dataprocessing_outcome_id,
				'phase 6' as dataprocessing_phase,
				
				rsa.alloc_amt_usd as allocated_amt,
				rsa.mgsv as rsa_mgsv,
				rsa.pcr as rsa_pcr,
				getdate() as audit_loadts
		from 	tmp_rsa_step2d_alloc_entity as rsa
				inner join vtbl_date_range as dt_rng
					on 	dt_rng.fiscal_month_id = rsa.fiscal_month_id
				cross join (
					select 	max(dpr.data_processing_ruleid) as data_processing_ruleid 
					from 	ref_data.data_processing_rule dpr 
					where 	dpr.bar_acct = 'A40115'
				) as dpr
 				left join ref_data.sku_barbrand_mapping sbm on lower(rsa.material) = lower(sbm.material) and sbm.current_flag =1
	;
	/* load to final transaction table (RSA Alloc) */
	INSERT INTO stage.sgm_allocated_data_rule_23 (
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
				rsa_reconcile_bar_custno,
				rsa_reconcile_bar_division,
				bar_currtype,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_id,
				dataprocessing_phase,
				allocated_amt,
				rsa_mgsv,
				rsa_pcr,
				audit_loadts
		)
		select 	'rsa_bible' as source_system,
				-1 as org_tranagg_id,
				dt_rng.range_end_date as posting_week_enddate,
				dt_rng.fiscal_month_id,
				rsa.bar_entity,
				'A40115' as bar_acct,
				rsa.brand as org_bar_brand,
				NULL as org_bar_custno,
				NULL as org_bar_product,
				coalesce(case when lower(sbm.bar_brand) = 'brand_none' then rsa.brand else sbm.bar_brand end, rsa.brand) as mapped_bar_brand,
				rsa.bar_custno as mapped_bar_custno,
				rsa.bar_product as mapped_bar_product,
				'unknown' as org_shiptocust,
				rsa.soldtocust as org_soldtocust,
				rsa.material as org_material,
				'unknown' as alloc_shiptocust,
				rsa.soldtocust as alloc_soldtocust,
				rsa.material as alloc_material,
				rsa.bar_product as alloc_bar_product,
				
				cast(null as varchar(50)) as rsa_reconcile_bar_custno,
				cast(null as varchar(50)) as rsa_reconcile_bar_division,
				
				case 
					when lower(rsa.EntitySourceRegion) = 'gts_ca' then 'cad'
					else 'usd' 
				end as bar_currtype,
				dpr.data_processing_ruleid as org_dataprocessing_ruleid,
				dpr.data_processing_ruleid as mapped_dataprocessing_ruleid,
				1 as dataprocessing_outcome_id,
				'phase 6' as dataprocessing_phase,
				
				rsa.alloc_amt_usd as allocated_amt,
				rsa.mgsv as rsa_mgsv,
				rsa.pcr as rsa_pcr,
				getdate() as audit_loadts
		from 	tmp_rsa_step2d_alloc_entity_fob as rsa
				inner join vtbl_date_range as dt_rng
					on 	dt_rng.fiscal_month_id = rsa.fiscal_month_id
				cross join (
					select 	max(dpr.data_processing_ruleid) as data_processing_ruleid 
					from 	ref_data.data_processing_rule dpr 
					where 	dpr.bar_acct = 'A40115'
				) as dpr
 				left join ref_data.sku_barbrand_mapping sbm on lower(rsa.material) = lower(sbm.material) and sbm.current_flag =1
	;
	/* load to final transaction table (GAP) */
	INSERT INTO stage.sgm_allocated_data_rule_23 (
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
				rsa_reconcile_bar_custno,
				rsa_reconcile_bar_division,
				bar_currtype,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_id,
				dataprocessing_phase,
				allocated_amt,
				rsa_mgsv,
				rsa_pcr,
				audit_loadts
		)
		select 	'rsa_bible' as source_system,
				-1 as org_tranagg_id,
				dt_rng.range_end_date as posting_week_enddate,
				dt_rng.fiscal_month_id,
				gap.bar_entity,
				'A40115' as bar_acct,
				'N/A' as org_bar_brand,
				'ADJ_RSA' as org_bar_custno,
				'ADJ_RSA' as org_bar_product,
				'N/A' as mapped_bar_brand,
				gap.bar_custno as mapped_bar_custno,
				'ADJ_RSA' as mapped_bar_product,
				'ADJ_RSA' as org_shiptocust,
				'ADJ_RSA' as org_soldtocust,
				'ADJ_RSA' as org_material,
				'ADJ_RSA' as alloc_shiptocust,
				'ADJ_RSA' as alloc_soldtocust,
				'ADJ_RSA' as alloc_material,
				'ADJ_RSA' as alloc_bar_product,
				
				gap.bar_custno as rsa_reconcile_bar_custno,
				/* 	use this to build product rows 
				 * 	(Bar_Product -> (Level8) Bar_Category -> Division) 
				 * */
				gap.division as rsa_reconcile_bar_division,
				case 
					when lower(gap.EntitySourceRegion) = 'gts_ca' then 'cad'
					else 'usd' 
				end as bar_currtype,
				dpr.data_processing_ruleid as org_dataprocessing_ruleid,
				dpr.data_processing_ruleid as mapped_dataprocessing_ruleid,
				2 as dataprocessing_outcome_id,
				'phase 100' as dataprocessing_phase,
				gap.alloc_amt_usd as allocated_amt,
				'gap' as rsa_mgsv,
				'gap' as rsa_pcr,
				getdate() as audit_loadts
		from 	tmp_rsa_output_gap_summarized_allocated as gap
				cross join vtbl_date_range dt_rng
				cross join (
					select 	max(dpr.data_processing_ruleid) as data_processing_ruleid 
					from 	ref_data.data_processing_rule dpr 
					where 	dpr.bar_acct = 'A40115'
				) as dpr
	;

--select 	1, 'BODS Target', round(sum(total_rsa_amt_usd),2) as bar_amt_usd
--from 	tmp_rsa_c11_retail_step1
--UNION ALL 
--select 	2, 'RSA Bible', round(sum(amt_usd),2) as bar_amt_usd
--from 	tmp_rsa_step2a
--union all
--select 	3, '---> RSA Bible (allocated)', round(sum(alloc_amt_usd),2) as amt
--from 	tmp_rsa_step2d_alloc_entity
--union all
--select 	4, '---> RSA Bible (allocated-fob)', round(sum(alloc_amt_usd),2) as amt
--from 	tmp_rsa_step2d_alloc_entity_fob
--union all
--select 	5, '---> RSA Bible (unallocated)', round(sum(amt_usd),2) as amt
--from 	tmp_rsa_step2d_unalloc
--union all
--select 	6, 'GAP summarized', round(sum(gap_amt_usd),2) as amt
--from 	tmp_rsa_output_gap_summarized
--union all
--select 	7, 'UMM', round(sum(allocated_amt),2) as amt
--from 	stage.sgm_allocated_data_rule_23
--where 	fiscal_month_id = 202109
--order by 1
--;
--
--
--select 	1, 'RSA Bible' as Grp, 
--		round(sum(case when EntitySourceRegion = 'GTS_CA' then amt_usd / 0.7883800000 else 0 end),2) as bar_amt_cad_GTS_CA,
--		round(sum(case when EntitySourceRegion = 'GTS_US' then amt_usd else 0 end),2) as bar_amt_usd_GTS_US
--from 	tmp_rsa_step2a
--union all
--select 	2, '---> RSA Bible (allocated)', 
--		round(sum(case when EntitySourceRegion = 'gts_ca' then rsa_alloc_amt_usd / 0.7883800000 else 0 end),2) as bar_amt_cad_GTS_CA,
--		round(sum(case when EntitySourceRegion = 'gts_us' then rsa_alloc_amt_usd else 0 end),2) as bar_amt_usd_GTS_US
--from 	tmp_rsa_output_allocated
--union all
--select 	3, '---> RSA Bible (unallocated)', 
--		round(sum(case when EntitySourceRegion = 'GTS_CA' then amt_usd / 0.7883800000 else 0 end),2) as bar_amt_cad_GTS_CA,
--		round(sum(case when EntitySourceRegion = 'GTS_US' then amt_usd else 0 end),2) as bar_amt_usd_GTS_US
--from 	tmp_rsa_step2d_unalloc
--union all
--select 	6, 'BODS Target', 
--		round(sum(case when EntitySourceRegion = 'gts_ca' then bods_rsa_amt_usd / 0.7883800000 else 0 end),2) as bar_amt_cad_GTS_CA,
--		round(sum(case when EntitySourceRegion = 'gts_us' then bods_rsa_amt_usd else 0 end),2) as bar_amt_usd_GTS_US
--from 	tmp_rsa_input
--union all
--select 	7, 'GAP', 
--		round(sum(case when EntitySourceRegion = 'gts_ca' then gap_amt_usd / 0.7883800000 else 0 end),2) as bar_amt_cad_GTS_CA,
--		round(sum(case when EntitySourceRegion = 'gts_us' then gap_amt_usd else 0 end),2) as bar_amt_usd_GTS_US
--from 	tmp_rsa_output_gap_summarized
--union all
--select 	8, '---> GAP (allocated)', 
--		round(sum(case when EntitySourceRegion = 'gts_ca' then alloc_amt_usd / 0.7883800000 else 0 end),2) as bar_amt_cad_GTS_CA,
--		round(sum(case when EntitySourceRegion = 'gts_us' then alloc_amt_usd else 0 end),2) as bar_amt_usd_GTS_US
--from 	tmp_rsa_output_gap_summarized_allocated
--order by 1
--;
	
exception
when others then raise info 'exception occur while ingesting data in stage.sgm_allocated_data_rule_23';
end;
$_$
;

CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_26_c11(fmthid integer)
 LANGUAGE plpgsql
AS $_$
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
	
	
    /* create temp table variable */
    drop table if exists calendar_posting_week
    ;
    create temporary table calendar_posting_week as 
        select  dd.wk_begin_dte-1 as calendar_posting_week
        from    ref_data.calendar dd
        where dy_dte = cast(getdate() as date)
    ;
--select dd.wk_begin_dte-1 
--into calendar_posting_week
--from dw.dim_date dd
--where dy_dte = cast(getdate() as date);

---sts cost from bods GTS_NA: only exists for c11 data. 
drop table if exists stage_c11_amount_to_allocate_rule_26;
create temporary table stage_c11_amount_to_allocate_rule_26
as
 Select 	  audit_rec_src as source_system,
 		  bar_entity,
		  coalesce( mapped_bar_custno, 'ADJ_FOB') as mapped_bar_custno,
		  sum(bar_amt) as bar_amt,
		  sum(sales_volume) as sales_volume,
		  sum(tran_volume) as tran_volume,
		  bcta.bar_currtype,
		  bcta.fiscal_month_id 
from stage.bods_core_transaction_agg bcta
	inner join vtbl_date_range dt on dt.fiscal_month_id = bcta.fiscal_month_id 
LEFT JOIN ref_data.data_processing_rule dpr  on bcta.mapped_dataprocessing_ruleid = dpr.data_processing_ruleid 
where dpr.data_processing_ruleid =26
--	and bcta.fiscal_month_id = fmthid--fmthid
	and bcta.audit_rec_src in  ('sap_c11') 
	--and mapped_bar_custno <> 'Customer_None'
	and bcta.bar_acct = 'A60111'
	and bcta.posting_week_enddate <= (select calendar_posting_week from calendar_posting_week)
group by mapped_bar_custno,bar_currtype,audit_rec_src,bar_entity,bcta.fiscal_month_id
having sum(bar_amt)<>0;

--1. Pull all transactions A60111 out of BODS in which bar_bu = 'GTS" or Null (Total 60111). 
--Sum of $ by soldtocust and material combination (60111 Sum)
drop table if exists stdcost_from_manuf_site; 
create temporary table stdcost_from_manuf_site
as 
 Select 	  audit_rec_src as source_system,
 		  soldtocust,
		  material,
		  sum(bar_amt) as bar_amt,
		  sum(sales_volume) as sales_volume,
		  sum(tran_volume) as tran_volume,
		  bcta.bar_currtype
from stage.bods_core_transaction_agg bcta
	inner join vtbl_date_range dt on dt.fiscal_month_id = bcta.fiscal_month_id 
LEFT JOIN ref_data.data_processing_rule dpr  on bcta.mapped_dataprocessing_ruleid = dpr.data_processing_ruleid 
where dpr.data_processing_ruleid =26
--	and bcta.fiscal_month_id = fmthid--fmthid
	and bcta.audit_rec_src in  ('ext_c11std') 
	and bcta.bar_acct = 'A60111'
	and bcta.posting_week_enddate <= (select calendar_posting_week from calendar_posting_week)
group by audit_rec_src,soldtocust,material,bar_currtype
having sum(bar_amt)<>0;
---Pull transactions from A40111 BODS filtering on bar_bu = 'GTS' and a non-empty shipto, shito to US or CA 
--these are products shipto US/CA
---invoices sold from manuf sites, sent multiple shipto for given soldto, sku combination. calculate adj cost per shipto
drop table if exists adj_std_cost; 
create temporary table adj_std_cost
as 
with adj_std_cost as 
(
 Select 	  bcta.audit_rec_src as source_system,
 		  bcta.shiptocust,
 		  bcta.soldtocust,
		  bcta.material,
		  bcta.bar_amt as invoice_amt,
		  bcta.sales_volume as sales_volume,
		  bcta.tran_volume as tran_volume,
		  m.bar_amt as std_cost,
		  sum(bcta.bar_amt) over (partition by bcta.soldtocust,bcta.material,bcta.bar_currtype) as invoice_per_soldto_sku,
		  cast(bcta.bar_amt as numeric(19,6)) / sum(bcta.bar_amt) over (partition by bcta.soldtocust,bcta.material,bcta.bar_currtype) as wt_avg,
		  m.bar_amt*(cast(bcta.bar_amt as numeric(19,6)) / sum(bcta.bar_amt) over (partition by bcta.soldtocust,bcta.material,bcta.bar_currtype)) as adj_std_cost,
		  bcta.bar_currtype,
		  kc.land1 as shipto_location
from stage.bods_core_transaction_agg bcta
	inner join vtbl_date_range dt on dt.fiscal_month_id = bcta.fiscal_month_id 
LEFT JOIN  ref_data.data_processing_rule dpr  on bcta.mapped_dataprocessing_ruleid = dpr.data_processing_ruleid 
left join stdcost_from_manuf_site m on bcta.soldtocust = m.soldtocust
			  and bcta.material = m.material 
			  and bcta.bar_currtype = m.bar_currtype
left join sapc11.kna1_current kc 
	on lower(shiptocust) = lower(kc.kunnr) 
where dpr.data_processing_ruleid =21
--	and bcta.fiscal_month_id = fmthid--fmthid
	and bcta.audit_rec_src in  ('ext_c11fob')    --GTS filter applied during delta pull
	and bcta.bar_acct = 'A40111'
	and bcta.posting_week_enddate <= (select calendar_posting_week from calendar_posting_week)
	---and kc.land1 in ('CA','US')   ---shito to US or CA
	--and bcta.material = 'CMCF604AM'
	and shiptocust is not null   ----non-empty shipto
	and bcta.bar_amt <> 0
)
---exclude transactions thats does not belongs to US and CA 
Select 	shiptocust,
		soldtocust,
		material,
		bar_currtype,
		sum(adj_std_cost) as adj_std_cost,
		sum(invoice_amt) as invoice_amt
from adj_std_cost
where shipto_location in ('CA','US')
group by shiptocust,
		soldtocust,
		material,
		bar_currtype;
		
		
drop table if exists stage_base_allocation_rate_by_entity_26; 
create temporary table stage_base_allocation_rate_by_entity_26
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
 from stage_c11_amount_to_allocate_rule_26
)a WHERE total_amt_per_cust <> 0;		
		

drop table if exists manuf_adj_std_cost;
create temporary table manuf_adj_std_cost
as 
---pick first / min tran_agg_id for soldto, material combination
with min_tranagg_id as 
(
Select min(org_tranagg_id) as org_tranagg_id, bcta.soldtocust,bcta.material
from stage.bods_core_transaction_agg bcta
	inner join vtbl_date_range dt on dt.fiscal_month_id = bcta.fiscal_month_id 
 inner join adj_std_cost s on bcta.material = s.material and bcta.soldtocust = s.soldtocust
Where audit_rec_src = 'ext_c11std'  
--and fiscal_month_id = fmthid
and bar_acct = 'A60111'
group by bcta.soldtocust,bcta.material
)
Select  bcta.audit_rec_src as  source_system,
	   bcta.org_tranagg_id,
	   bcta.posting_week_enddate,
	   bcta.fiscal_month_id,
 	   bcta.bar_acct,
 	   bcta.org_bar_brand,
 	   bcta.org_bar_custno,
 	   bcta.org_bar_product,
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
 	   bcta.mapped_dataprocessing_ruleid, 
 	   s.bar_currtype,
 	   adj_std_cost as bar_amt,
 	   bcta.sales_volume,
  	   bcta.tran_volume,
        bcta.uom 
from stage.bods_core_transaction_agg bcta
	inner join vtbl_date_range dt on dt.fiscal_month_id = bcta.fiscal_month_id 
inner join min_tranagg_id mn on bcta.org_tranagg_id=mn.org_tranagg_id
---non US / CA will automatically 
 inner join adj_std_cost s on bcta.material = s.material and bcta.soldtocust = s.soldtocust
 left join ref_data.sku_gpp_mapping sgm on lower(s.material) = lower(sgm.material)  and sgm.current_flag =1
 left join ref_data.fob_soldto_barcust_mapping fsbm on lower(fsbm.soldtocust) = lower(s.soldtocust)
 left join ref_data.sku_barbrand_mapping sbm on lower(s.material) = lower(sbm.material)  and sbm.current_flag =1
Where audit_rec_src = 'ext_c11std'
and bcta.posting_week_enddate<=(select calendar_posting_week from calendar_posting_week)
--and fiscal_month_id = fmthid
and bar_acct = 'A60111';

    /* create temp table variable */
    drop table if exists current_posting_week
    ;
    create temporary table current_posting_week as 
        select max(posting_week_enddate) as current_posting_week
        from manuf_adj_std_cost
    ;

drop table if exists allocated_adj_stdcost_sales; 
create temporary table allocated_adj_stdcost_sales
as
Select *
from (
---union the gap between allocated data from GTS and unallocated from GTS_NA
with allocated_adj_stdcost as (
	Select isnull(r.bar_entity,'E2035') as bar_entity,s.fiscal_month_id,alloc_bar_custno, s.bar_currtype,
		  sum(cast(s.bar_amt as numeric(19,6)) * cast(isnull(r.wt_avg,1) as numeric(19,8)) )  as alloc_bar_amt, 
		  0 as alloc_sales_volume,
		  0 as alloc_tran_volume 
	from manuf_adj_std_cost s 
	left join  stage_base_allocation_rate_by_entity_26 r on s.bar_currtype = r.bar_currtype
			and s.alloc_bar_custno = r.mapped_bar_custno
	group by isnull(r.bar_entity,'E2035'),s.fiscal_month_id,alloc_bar_custno,s.bar_currtype
)
, tobe_allocated_adj_stdcost as (
	Select bar_entity,mapped_bar_custno,bar_currtype,fiscal_month_id,
		sum(bar_amt) as bar_amt, 
		sum(sales_volume) as sales_volume, 
		sum(tran_volume) as tran_volume
	from stage_c11_amount_to_allocate_rule_26
	group by bar_entity,mapped_bar_custno,bar_currtype,fiscal_month_id
	HAVING sum(bar_amt)<>0
)
Select cast('sap_c11' as varchar(10)) as  source_system,
	  -1 as org_tranagg_id, 
	  coalesce(
	       (select current_posting_week from current_posting_week),
	       d.range_end_date) as posting_week_enddate,
	  al.fiscal_month_id,
	  al.bar_entity,
	  'A60111' as bar_acct,
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
	  26 as org_dataprocessing_ruleid,
	  26 as mapped_dataprocessing_ruleid,  --will always be 21
	  2 as dataprocessing_outcome_id, --2 as not allocated
	  case when al.alloc_bar_custno= 'ADJ_FOB_NO_CUST' then 'phase 104' else  'phase 103' end as dataprocessing_phase,
	  (alloc_bar_amt - isnull(bar_amt,0))*-1 as alloc_bar_amt,
	  (alloc_sales_volume - isnull(sales_volume,0)) as alloc_sales_volume,
	  (alloc_tran_volume - isnull(tran_volume,0)) as alloc_tran_volume,
	  cast('ea' as varchar(10)) as uom,
	  cast(getdate() as timestamp) as audit_loadts
from allocated_adj_stdcost al
left join tobe_allocated_adj_stdcost tal on  alloc_bar_custno = mapped_bar_custno
and al.bar_currtype = tal.bar_currtype
and al.bar_entity = tal.bar_entity
left join vtbl_date_range d on al.fiscal_month_id = d.fiscal_month_id
union all 
-----customer none goes here
Select cast('sap_c11' as varchar(10)) as source_system,
	  -1 as org_tranagg_id, 
	  coalesce(
	       (select current_posting_week from current_posting_week),
	       d.range_end_date) as posting_week_enddate,
	  tal.fiscal_month_id,
	  tal.bar_entity,
	  'A60111' as bar_acct,
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
	  26 as org_dataprocessing_ruleid,
	  26 as mapped_dataprocessing_ruleid,  --will always be 21
	  2 as dataprocessing_outcome_id, --2 as not allocated
	  'phase 103' as dataprocessing_phase,
	  isnull(bar_amt,0) as alloc_bar_amt,
	  isnull(sales_volume,0) as alloc_sales_volume,
	  isnull(tran_volume,0) as alloc_tran_volume,
	  cast('ea' as varchar(10)) as uom,
	  cast(getdate() as timestamp) as audit_loadts
from tobe_allocated_adj_stdcost tal 
left join allocated_adj_stdcost al on  alloc_bar_custno = mapped_bar_custno
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
	  s.mapped_dataprocessing_ruleid,  --will always be 26
	  1 as dataprocessing_outcome_id, --1 as allocated
	  'phase 3' as dataprocessing_phase,
	  cast(s.bar_amt as numeric(19,6))*cast(isnull(r.wt_avg,1) as numeric(19,8)) as alloc_bar_amt,
	 -- s.bar_amt*isnull(r.wt_avg,1)  as alloc_bar_amt, 
	  s.sales_volume*isnull(r.wt_avg,1) as sales_volume,
	  s.tran_volume*isnull(r.wt_avg,1) as tran_volume,
	  s.uom,
	  cast(getdate() as timestamp) as audit_loadts
from manuf_adj_std_cost s 
left join  stage_base_allocation_rate_by_entity_26 r on s.bar_currtype = r.bar_currtype
		and s.alloc_bar_custno = r.mapped_bar_custno
) a;

    delete  
    from    stage.sgm_allocated_data_rule_26
    where   fiscal_month_id = (select fiscal_month_id from vtbl_date_range) and  
            source_system ='sap_c11'
    ;

    INSERT INTO stage.sgm_allocated_data_rule_26 (	
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
        Select  source_system, 
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
        from    allocated_adj_stdcost_sales
    ;
  
    exception when others then raise info 'exception occur while ingesting data in stage.sgm_allocated_data_rule_26_c11';
end
$_$
;

CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_26_hfm(fmthid integer)
 LANGUAGE plpgsql
AS $_$
--DECALRE Variables here
BEGIN 
	
---step 1 : 1. Keep all current transactions pulled for A60111 (transactions booked to a GTS-NA Entity). Do not apply allocation rule to the account. Sum up $ each month for each customer. Do not use these transactions in UMM
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
drop table if exists stage_hfm_amount_to_allocate_rule_26;
create temporary table stage_hfm_amount_to_allocate_rule_26
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
where dpr.data_processing_ruleid =26
	and bcta.fiscal_month_id = fmthid----fmthid
	and bcta.audit_rec_src in  ('hfm')  -----stored_proc : parameter
	and bcta.bar_acct = 'A60111'
	and bcta.bar_amt <> 0;


---IA_Tools is product division. hfm data is for bar_custno and product_division. 
--get product division from bods master table
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
from 	bods.drm_product_current
where 	loaddts = ( select max(loaddts) from bods.drm_product_current dpc );

--build_total_amount_for_rate_calculations
drop table if exists build_total_amount_for_rate_calculations_rule_26_p1;
create temporary table build_total_amount_for_rate_calculations_rule_26_p1
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
		 from stage_hfm_amount_to_allocate_rule_26 ) in_amt 
		 on  rb.bar_custno = in_amt.bar_custno 
			and lower(bpb.level07_bar) = lower(in_amt.bar_product) ---product division
group by 	rb.bar_custno,
		lower(bpb.level07_bar),
		rb.bar_entity,
		rb.bar_currtype,
		rb.source_system
order by lower(bpb.level07_bar);


drop table if exists build_rate_calculations_rule_26_p1;
----build averages now all combinations 
--Select bar_custno, bar_product,sum(weighted_avg)
--from (
create temporary table build_rate_calculations_rule_26_p1
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
from build_total_amount_for_rate_calculations_rule_26_p1 rc
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
	from (select distinct material from build_rate_calculations_rule_26_p1) rt
	inner join stage.bods_core_transaction_agg bcta  on rt.material = bcta.material 
	where bar_acct = 'A60110'
	group by bcta.audit_rec_src,bcta.material,mapped_bar_brand
) mat ;
	
	
drop table if exists sgm_hfm_allocated_data_rule_26_p1; 
create temporary table sgm_hfm_allocated_data_rule_26_p1
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
from build_rate_calculations_rule_26_p1 rt
inner join stage_hfm_amount_to_allocate_rule_26 in_amt on rt.bar_custno = in_amt.mapped_bar_custno 
		and lower(rt.bar_division) = lower(in_amt.mapped_bar_product)
left join build_p1_mapped_brand_for_material mbm on rt.source_system = mbm.source_system
		and rt.material = mbm.material 
		and rank_tran_cnt=1;	

delete from stage.sgm_allocated_data_rule_26
where fiscal_month_id = fmthid and source_system ='hfm';

INSERT INTO stage.sgm_allocated_data_rule_26
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
	allocated_amt as allocated_amt, 
	0 sales_volume, 
	0 tran_volume, 
	'unknown' as uom, 
	cast(getdate() as timestamp) audit_loadts
from sgm_hfm_allocated_data_rule_26_p1;
  
exception
when others then raise info 'exception occur while ingesting data in stage.sgm_allocated_data_rule_26_hmf';
end;
$_$
;

CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_27(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN   
	
	--TESTING
	--delete from stage.sgm_allocated_data_rule_27;
	--call stage.p_allocate_data_rule_27 (201903)
	--call stage.p_allocate_data_rule_27 (202003)
	--select count(*) from stage.sgm_allocated_data_rule_27
	-- select * from stage.sgm_allocated_data_rule_27
	--select fiscal_month_id, count(*) from stage.sgm_allocated_data_rule_27 group by fiscal_month_id order by 1
/*
 *	This procedure manages the allocations for Rule ID #22
 *
 *		Allocation Exception - Customer_None, Product_None based scenarios
 *
 * 		Final Table(s): 
 *			stage.sgm_allocated_data_rule_27
 *
 * 		Rule Logic:	
 * 			Org BAR_Product	Org SKU	Org BAR_Customer	Org SoldTo	Allocated SKU	Allocated SoldTo		Allocation Flag
			Product_None		unknown  	Customer_None		unknown		ADJ_NO_Prod		ADJ_NO_CUST		Allocated flag =1
			Product_None		unknown	Real Customer		unknown		ADJ_NO_Prod		ADJ_NO_CUST		Allocated flag =1
			Real Product		unknown	Customer_None		unknown		ADJ_NO_Prod		ADJ_NO_CUST		Allocated flag =1
			Product_None		Real SKU	Customer_None		unknown		(keep original)	ADJ_NO_CUST		Allocated flag =1
			Product_None		unknown  	Customer_None		Real Sold-to	ADJ_NO_Prod		(keep original)	Allocated flag =1
			Product_None		Real SKU	Customer_None		Real Sold-to	(keep original)	(keep original)	Allocated flag =1
 *
 */
	
--	
--	Select *
--from  stage.bods_core_transaction_agg bcta
--where mapped_bar_custno is null
--and mapped_dataprocessing_ruleid = 27;
	
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
				,case when tran.mapped_bar_product not in ('PRODUCT_NONE') then 'Real Product' else  tran.mapped_bar_product end as mapped_bar_product_for_27
				,case when (tran.material is not null or tran.material not in ('unknown')) then 'Real SKU' else isnull(tran.material, 'unknown') end as material_for_27
				,case when tran.mapped_bar_custno not in ('CUSTOMER_NONE') then  'Real Customer' else isnull(tran.mapped_bar_custno,'unknown') end as mapped_bar_custno_for_27
				,case when (tran.soldtocust is not null or tran.soldtocust not in ('unknown')) then 'Real Sold-to' else isnull(tran.soldtocust,'unknown') end as org_soldtocust_for_27 
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
			and dpr.data_processing_ruleid = 27
			and tran.audit_rec_src in  ('sap_c11', 'sap_lawson', 'sap_p10','hfm')
	;
--select count(*) 
--from 	_trans_unalloc
--where 	mapped_bar_custno = 'unknown'
	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	stage.sgm_allocated_data_rule_27
	where 	posting_week_enddate between 
			(select range_start_date from vtbl_date_range) and 
			(select range_end_date from vtbl_date_range)
	;
	/* load transactions */
	insert into stage.sgm_allocated_data_rule_27 (
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
--				tran.shiptocust as alloc_shiptocust,
				case when org_soldtocust_for_27 in ('Real Sold-to') then tran.shiptocust else 'ADJ_NO_CUST' end as alloc_shiptocust,
				case when org_soldtocust_for_27 in ('Real Sold-to') then tran.org_soldtocust else 'ADJ_NO_CUST' end as alloc_soldtocust,
				case when tran.material_for_27 in ('Real SKU') then tran.material else 'ADJ_NO_PROD' end as alloc_material,
				tran.mapped_bar_product as alloc_bar_product,
				bar_currtype,
				tran.org_dataprocessing_ruleid,
				tran.mapped_dataprocessing_ruleid,
				1 as dataprocessing_outcome_id,
				'phase 101' as dataprocessing_phase,
				tran.unallocated_bar_amt as allocated_amt,
				tran.sales_volume,
				tran.tran_volume,
				tran.uom,
				getdate() as audit_loadts
		from 	_trans_unalloc as tran
	;
	
exception
when others then raise info 'exception occur while ingesting data in stage.sgm_allocated_data_rule_27';
end;
$$
;

CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_28(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN   
	
	--TESTING
	--delete from stage.sgm_allocated_data_rule_28;
	--call stage.p_allocate_data_rule_28 (201903)
	--call stage.p_allocate_data_rule_27 (202003)
	--select count(*) from stage.sgm_allocated_data_rule_28
	-- select * from stage.sgm_allocated_data_rule_28
	--select fiscal_month_id, count(*) from stage.sgm_allocated_data_rule_28 group by fiscal_month_id order by 1
/*
 *	This procedure manages the allocations for Rule ID #22
 *
 *		Allocation Exception - Customer_None, Product_None based scenarios
 *
 * 		Final Table(s): 
 *			stage.sgm_allocated_data_rule_27
 *
 * 		Rule Logic:	
 * 			Org BAR_Product	Org SKU	Org BAR_Customer	Org SoldTo	Allocated SKU		Allocated SoldTo		Allocation Flag
				OTH_SERVICE	unknown 	PSD_Oth			unknown 		ADJ_SERVICE		ADJ_PSD
				OTH_SERVICE	unknown 	PSD_Oth			Real Sold-to	ADJ_SERVICE		(keep original)
				OTH_SERVICE	Real SKU	PSD_Oth			unknown 		(keep original)	ADJ_PSD
				OTH_SERVICE	Real SKU	PSD_Oth			Real Sold-to	(keep original)	(keep original)
				OTH_SERVICE	unknown 	Real Customer		Real Sold-to	ADJ_SERVICE		(keep original)
				OTH_SERVICE	Real SKU	Real Customer		unknown 		(keep original)	ADJ_PSD
				P60999		unknown 	PSD_Oth			unknown 		ADJ_REBUILD		ADJ_PSD
				P60999		unknown 	PSD_Oth			Real Sold-to	ADJ_REBUILD		(keep original)
				P60999		Real SKU	PSD_Oth			unknown 		(keep original)	ADJ_PSD
				P60999		Real SKU	PSD_Oth			Real Sold-to	(keep original)	(keep original)
				P60999		unknown 	Real Customer		Real Sold-to	ADJ_REBUILD		(keep original)
				P60999		Real SKU	Real Customer		unknown 		(keep original)	ADJ_PSD
				Real Product	unknown 	PSD_Oth			unknown 		ADJ_SERVICE		ADJ_PSD
				Real Product	Real SKU	PSD_Oth			unknown 		(keep original)	ADJ_PSD
 *
 */
	
--	
--	Select distinct 
--			case when tran.mapped_bar_product not in ('OTH_SERVICE','P60999') then 'Real Product' else tran.mapped_bar_product end as mapped_bar_product_for_28,
--			case when (tran.material is null or tran.material = 'unknown') then 'unknown' else 'Real SKU' end as material_for_28,
--			case when tran.mapped_bar_custno not in ('PSD_Oth') then 'Real Customer' else mapped_bar_custno end as mapped_bar_custno_for_28,
--			case when (tran.soldtocust is null or tran.soldtocust = 'unknown') then 'unknown' else 'Real Sold-to' end as soldtocust_for_28
--from  stage.bods_core_transaction_agg tran
--where mapped_dataprocessing_ruleid = 28;
	
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
				,case when tran.mapped_bar_product not in ('OTH_SERVICE','P60999') then 'Real Product' else tran.mapped_bar_product end as mapped_bar_product_for_28
				,case when (tran.material is null or tran.material = 'unknown') then 'unknown' else 'Real SKU' end as material_for_28
				,case when tran.mapped_bar_custno not in ('PSD_Oth') then 'Real Customer' else mapped_bar_custno end as mapped_bar_custno_for_28
				,case when (tran.soldtocust is null or tran.soldtocust = 'unknown') then 'unknown' else 'Real Sold-to' end as soldtocust_for_28				
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
			and dpr.data_processing_ruleid = 28
			and tran.audit_rec_src in  ('sap_c11', 'sap_lawson', 'sap_p10','hfm')
	;
	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	stage.sgm_allocated_data_rule_28
	where 	posting_week_enddate between 
			(select range_start_date from vtbl_date_range) and 
			(select range_end_date from vtbl_date_range)
	;
	/* load transactions */
	insert into stage.sgm_allocated_data_rule_28 (
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
				case when soldtocust_for_28 in ('Real Sold-to') then tran.shiptocust else 'ADJ_PSD' end as alloc_shiptocust,
				case when soldtocust_for_28 in ('Real Sold-to') then tran.org_soldtocust else 'ADJ_PSD' end as alloc_soldtocust,
				case when material_for_28 in ('Real SKU') then tran.material else 'ADJ_REBUILD' end as alloc_material,
				tran.mapped_bar_product as alloc_bar_product,
				bar_currtype,
				tran.org_dataprocessing_ruleid,
				tran.mapped_dataprocessing_ruleid,
				1 as dataprocessing_outcome_id,
				'phase 102' as dataprocessing_phase,
				tran.unallocated_bar_amt as allocated_amt,
				tran.sales_volume,
				tran.tran_volume,
				tran.uom,
				getdate() as audit_loadts
		from 	_trans_unalloc as tran
	;
	
exception
when others then raise info 'exception occur while ingesting data in stage.sgm_allocated_data_rule_28';
end;
$$
;

CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_agm_100(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN   
	
/*
 * 		truncate table stage.agm_allocated_data_rule_100;
 * 		call stage.p_allocate_data_rule_agm_100(202101)
 * 		select count(*) from stage.agm_allocated_data_rule_100;
 * 		grant execute on procedure stage.p_allocate_data_rule_agm_100(fmthid integer) to group "g-ada-rsabible-sb-ro";
 */
/*
 *	This procedure manages the logic for AGM Rule ID #100
 *		Allocation of costs associated w/ Reported Inventory Adjustment 
 *
 * 		Final Table(s): 
 *			stage.agm_allocated_data_rule_100
 *
 * 		Rule Logic:	
 *			Step 1: Extract cost for allocation at SBU level
 *			Step 2: Build allocation rate table (% of COGS) for P&L cost assignment
 *			Step 3: Allocate cost to SKU transactions
 *			Step 4: Allocate BOD gap to BA&R hyperion cost (at super SBU level)
 *		
 *		New Logic (7/6/2021): allocate hyp amount by COGS
 *			
 */
	
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				max(dt.fmth_id) AS fiscal_month_id
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	/* create temp table for exchange_rate */
	drop table if exists vtbl_exchange_rate
	;
	create temporary table vtbl_exchange_rate as 
		select 	rt.fiscal_month_id, 
				rt.from_currtype,
				rt.fxrate
		from 	ref_data.hfmfxrates_current rt
				inner join vtbl_date_range dt
					on 	dt.fiscal_month_id = rt.fiscal_month_id 
		where 	lower(rt.to_currtype) = 'usd'
	;
	
	/* Step 1: Hyperion Amounts for month */
	drop table if exists _hyp_amt
	;
	create temporary table _hyp_amt as 
		select 	bar.fiscal_month_id,
				sum(bar.amt_reported * agm_acct.multiplication_factor) as amt_usd
		from 	ref_data.agm_bnr_financials_extract bar
				inner join ref_data.pnl_acct_agm as agm_acct 
					on 	agm_acct.bar_acct = bar.account 
				inner join vtbl_date_range as dt
					on 	bar.fiscal_month_id = dt.fiscal_month_id
				inner join (
					/* this table is no longer distinct on entity_name */
					select 	distinct name, level4
					from 	ref_data.entity
				) as rbh
					on 	bar.entity = rbh.name
		where 	agm_acct.acct_category = 'Reported Inventory Adjustment' and 
				bar.scenario = 'Actual_Ledger' and
				rbh.level4 = 'GTS_NA'
		group by bar.fiscal_month_id
	;
	/* Step 2: sku/cust/entity with postive net sales (A40110) in current month */
	drop table if exists sku_positive_sales
	;
	create temporary table sku_positive_sales as 
		select 	dp.material
		from 	dw.fact_pnl_commercial_stacked as f
				inner join dw.dim_product dp on dp.product_key = f.product_key 
				inner join vtbl_date_range as dt_rng
					on  dt_rng.fiscal_month_id = f.fiscal_month_id 
		where 	0=0
			and f.bar_acct = 'A40110' 
		group by dp.material
		having 	sum(f.amt_usd) > 0
	;
	/* Step 2: create rate table based on standard cost */
	drop table if exists rate_base_cogs_pct_of_total
	;
	create temporary table rate_base_cogs_pct_of_total as 
		WITH
			cte_base AS (
				select 	rb_cogs.fiscal_month_id,
						rb_cogs.material,
						rb_cogs.audit_rec_src,
						rb_cogs.bar_entity,
						rb_cogs.bar_currtype,
						rb_cogs.soldtocust,
						rb_cogs.shiptocust,
						rb_cogs.bar_custno,
						rb_cogs.bar_product,
						rb_cogs.bar_brand,
						rb_cogs.cost_pool,
						sum(rb_cogs.total_bar_amt_usd) as total_bar_amt_usd
				from 	stage.rate_base_cogs as rb_cogs
						inner join vtbl_date_range as dt_rng
							on  dt_rng.fiscal_month_id = rb_cogs.fiscal_month_id 
						inner join sku_positive_sales sku_list
							on 	lower(sku_list.material) = lower(rb_cogs.material)
				group by rb_cogs.fiscal_month_id,
						rb_cogs.material,
						rb_cogs.audit_rec_src,
						rb_cogs.bar_entity,
						rb_cogs.bar_currtype,
						rb_cogs.soldtocust,
						rb_cogs.shiptocust,
						rb_cogs.bar_custno,
						rb_cogs.bar_product,
						rb_cogs.bar_brand,
						rb_cogs.cost_pool
				having 	sum(rb_cogs.total_bar_amt_usd) < 0
			),
			cte_rate_base_cogs as (
				select 	rb.fiscal_month_id,
						rb.material,
						rb.audit_rec_src,
						rb.bar_entity,
						rb.bar_currtype,
						rb.soldtocust,
						rb.shiptocust,
						rb.bar_custno,
						rb.bar_product,
						rb.bar_brand,
						rb.cost_pool,
						rb.total_bar_amt_usd,
						sum(rb.total_bar_amt_usd) over( partition by rb.fiscal_month_id ) as total_bar_amt_usd_partition
				from 	cte_base as rb
			)
		select 	cte_rb.fiscal_month_id,
				cte_rb.material,
				cte_rb.audit_rec_src,
				cte_rb.bar_entity,
				cte_rb.bar_currtype,
				cte_rb.soldtocust,
				cte_rb.shiptocust,
				cte_rb.bar_custno,
				cte_rb.bar_product,
				cte_rb.bar_brand,
				cte_rb.cost_pool,
				cte_rb.total_bar_amt_usd,
				cte_rb.total_bar_amt_usd_partition,
				CAST(cte_rb.total_bar_amt_usd as decimal(20,8))
					/ CAST(cte_rb.total_bar_amt_usd_partition as decimal(20,8)) as pct_of_total
		from 	cte_rate_base_cogs cte_rb
		where 	total_bar_amt_usd_partition != 0
	;

/* DEBUG: clusters with cumulative pct of total != 100% */
--select 	rt.fiscal_month_id, lower(rt.super_sbu), sum(rt.pct_of_total)
--from 	rate_base_cogs_pct_of_total rt
--group by rt.fiscal_month_id, lower(rt.super_sbu)
--having round(sum(rt.pct_of_total),4) != 1
--order by 3 asc
--;
	/* Step 2: allocate to full transaction level via COGS Rate */
	drop table if exists _hyp_allocated
	;
	create temporary table _hyp_allocated as 
		select 	rt.fiscal_month_id,
				rt.audit_rec_src,
				rt.bar_entity,
				rt.bar_currtype,
				rt.soldtocust,
				rt.shiptocust,
				rt.bar_custno,
				rt.material,
				rt.bar_product,
				rt.bar_brand,
				rt.cost_pool,
				tran.amt_usd,
				rt.pct_of_total,
				rt.pct_of_total * tran.amt_usd as allocated_amt_usd
		from 	_hyp_amt as tran
				inner join rate_base_cogs_pct_of_total as rt
					on 	rt.fiscal_month_id = tran.fiscal_month_id
	;

/* DEBUG: compare input / output*/
--select 	1 as ord, 'Input-HYP' as category, 
--		sum(round(amt_usd,4)) as amt_usd, count(*)
--from 	_hyp_amt
--union all
--select 	2 as ord, 'Output-Cost2SSBU' as category, 
--		sum(round(allocated_amt_usd,4)) as amt_usd, count(*)
--from 	_hyp_allocated
--order by 1
--;

	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	stage.agm_allocated_data_rule_100
	where 	fiscal_month_id = (select fiscal_month_id from vtbl_date_range)
	;
	/* load to final transaction table (AGM: Inv Adj) */
	INSERT INTO stage.agm_allocated_data_rule_100 (
				source_system,
				fiscal_month_id,
				posting_week_enddate,
			
				bar_entity,
				bar_acct,
				
				material,
				bar_product,
				bar_brand,
				
				soldtocust,
				shiptocust,
				bar_custno,
				
				dataprocessing_ruleid,
				dataprocessing_outcome_id,
				dataprocessing_phase,
				
				cost_pool,
				super_sbu,
				
				bar_currtype,
				allocated_amt,
				allocated_amt_usd,
				
				audit_loadts
		)
		select 	stg.audit_rec_src as source_system,
				stg.fiscal_month_id,
				dt.range_end_date as posting_week_enddate,
			
				stg.bar_entity,
				'AGM_ADJ_INV' as bar_acct,
				
				stg.material,
				stg.bar_product,
				stg.bar_brand as bar_brand,
				
				stg.soldtocust,
				'unknown' as shiptocust,
				stg.bar_custno,
				
				100 as dataprocessing_ruleid,
				1 as dataprocessing_outcome_id,
				'phase 20' as dataprocessing_phase,
				
				/* these are N/A because they are no longer factors in the allocation logic */
				'n/a' as cost_pool,
				'n/a' as super_sbu,
				
				stg.bar_currtype,
				case 
					when fx.from_currtype is null then stg.allocated_amt_usd
					else CAST(stg.allocated_amt_usd as decimal(20,8))
						/ CAST(fx.fxrate as decimal(20,8))
				end as allocated_amt,
				stg.allocated_amt_usd,
				
				getdate() audit_loadts
		from 	_hyp_allocated as stg
				inner join vtbl_date_range as dt
					on 	dt.fiscal_month_id = stg.fiscal_month_id				
				left outer join vtbl_exchange_rate as fx
					on 	fx.fiscal_month_id = stg.fiscal_month_id and 
						lower(fx.from_currtype) = lower(stg.bar_currtype)
	;
/* DEBUG: compare Hyp Input */
--select 	1 as ord, 'Input-BODS' as category, 
--		sum(round(allocated_amt_usd,4)) as amt,
--		count(*)
--from 	_hyp_allocated
--union all
--select 	2 as ord, 'Output-StageAGM' as category, 
--		sum(round(allocated_amt_usd,4)) as amt,
--		count(*)
--from 	stage.agm_allocated_data_rule_100
--where 	fiscal_month_id = 202008
--order by ord
--;
exception
when others then raise info 'exception occur while ingesting data in stage.p_allocate_data_rule_agm_100';
end;
$$
;

CREATE OR REPLACE PROCEDURE stage.p_allocate_data_rule_agm_105(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN   
	
/*
 * 		truncate table stage.agm_allocated_data_rule_105;
 * 		call stage.p_allocate_data_rule_agm_105(202101)
 * 		select count(*) from stage.agm_allocated_data_rule_105;
 * 		grant execute on procedure stage.p_allocate_data_rule_agm_105(fmthid integer) to group "g-ada-rsabible-sb-ro";
 */
/*
 *		Description: logic for AGM Rule ID #105 (Reported Labor/OH)
 *
 * 		Final Table(s): 
 *			stage.agm_allocated_data_rule_105
 *
 * 		Rule Logic:	
 *			Step 1: Extract cost for allocation at SBU level
 *			Step 2: Build allocation rate table (% of COGS) for P&L cost assignment
 *			Step 3: Allocate cost to SKU transactions
 *			Step 4: Allocate BOD gap to BA&R hyperion cost (at super SBU level)
 *
 *		TODO:
 *			test w/ real data
 *			
 */
	
	/*
	 * 	201901: Target
	 * 			-10,105,649.6
	 * 			Actual
	 * 			
	 * 
	 */
	
	
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				max(dt.fmth_id) AS fiscal_month_id
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	/* create temp table for exchange_rate */
	drop table if exists vtbl_exchange_rate
	;
	create temporary table vtbl_exchange_rate as 
		select 	rt.fiscal_month_id, 
				rt.from_currtype,
				rt.fxrate
		from 	ref_data.hfmfxrates_current rt
				inner join vtbl_date_range dt
					on 	dt.fiscal_month_id = rt.fiscal_month_id 
		where 	lower(rt.to_currtype) = 'usd'
	;
	
	/* Step 1: Hyperion Amounts for month */
	drop table if exists _hyp_amt
	;
	create temporary table _hyp_amt as 
		select 	bar.fiscal_month_id,
				sum(bar.amt_reported * agm_acct.multiplication_factor) as amt_usd
		from 	ref_data.agm_bnr_financials_extract bar
				inner join ref_data.pnl_acct_agm as agm_acct 
					on 	agm_acct.bar_acct = bar.account 
				inner join vtbl_date_range as dt
					on 	bar.fiscal_month_id = dt.fiscal_month_id
				inner join (
					/* this table is no longer distinct on entity_name */
					select 	distinct name, level4
					from 	ref_data.entity
				) as rbh
					on 	bar.entity = rbh.name
		where 	agm_acct.acct_category = 'Reported Labor / OH' and 
				bar.scenario = 'Actual_Ledger' and
				rbh.level4 = 'GTS_NA'
		group by bar.fiscal_month_id
	;

	/* Step 2: sku/cust/entity with postive net sales (A40110) in current month */
	drop table if exists sku_positive_sales
	;
	create temporary table sku_positive_sales as 
		select 	dp.material
		from 	dw.fact_pnl_commercial_stacked as f
				inner join dw.dim_product dp on dp.product_key = f.product_key 
				inner join vtbl_date_range as dt_rng
					on  dt_rng.fiscal_month_id = f.fiscal_month_id 
		where 	0=0
			and f.bar_acct = 'A40110' 
		group by dp.material
		having 	sum(f.amt_usd) > 0
	;
	/* Step 2: create rate table based on standard cost */
	drop table if exists rate_base_cogs_pct_of_total
	;
	create temporary table rate_base_cogs_pct_of_total as 
		WITH
			cte_base AS (
				select 	rb_cogs.fiscal_month_id,
						rb_cogs.material,
						rb_cogs.audit_rec_src,
						rb_cogs.bar_entity,
						rb_cogs.bar_currtype,
						rb_cogs.soldtocust,
						rb_cogs.shiptocust,
						rb_cogs.bar_custno,
						rb_cogs.bar_product,
						rb_cogs.bar_brand,
						rb_cogs.cost_pool,
						sum(rb_cogs.total_bar_amt_usd) as total_bar_amt_usd
				from 	stage.rate_base_cogs as rb_cogs
						inner join vtbl_date_range as dt_rng
							on  dt_rng.fiscal_month_id = rb_cogs.fiscal_month_id 
						inner join sku_positive_sales sku_list
							on 	lower(sku_list.material) = lower(rb_cogs.material)
				group by rb_cogs.fiscal_month_id,
						rb_cogs.material,
						rb_cogs.audit_rec_src,
						rb_cogs.bar_entity,
						rb_cogs.bar_currtype,
						rb_cogs.soldtocust,
						rb_cogs.shiptocust,
						rb_cogs.bar_custno,
						rb_cogs.bar_product,
						rb_cogs.bar_brand,
						rb_cogs.cost_pool
				having 	sum(rb_cogs.total_bar_amt_usd) < 0
			),
			cte_rate_base_cogs as (
				select 	rb.fiscal_month_id,
						rb.material,
						rb.audit_rec_src,
						rb.bar_entity,
						rb.bar_currtype,
						rb.soldtocust,
						rb.shiptocust,
						rb.bar_custno,
						rb.bar_product,
						rb.bar_brand,
						rb.cost_pool,
						rb.total_bar_amt_usd,
						sum(rb.total_bar_amt_usd) over( partition by rb.fiscal_month_id ) as total_bar_amt_usd_partition
				from 	cte_base as rb
			)
		select 	cte_rb.fiscal_month_id,
				cte_rb.material,
				cte_rb.audit_rec_src,
				cte_rb.bar_entity,
				cte_rb.bar_currtype,
				cte_rb.soldtocust,
				cte_rb.shiptocust,
				cte_rb.bar_custno,
				cte_rb.bar_product,
				cte_rb.bar_brand,
				cte_rb.cost_pool,
				cte_rb.total_bar_amt_usd,
				cte_rb.total_bar_amt_usd_partition,
				CAST(cte_rb.total_bar_amt_usd as decimal(20,8))
					/ CAST(cte_rb.total_bar_amt_usd_partition as decimal(20,8)) as pct_of_total
		from 	cte_rate_base_cogs cte_rb
		where 	total_bar_amt_usd_partition != 0
	;

/* DEBUG: clusters with cumulative pct of total != 100% */
--select 	rt.fiscal_month_id, sum(rt.pct_of_total)
--from 	rate_base_cogs_pct_of_total rt
--group by rt.fiscal_month_id
--having round(sum(rt.pct_of_total),4) != 1
--order by 2 asc
--;
	/* Step 2: allocate to full transaction level via COGS Rate */
	drop table if exists _hyp_allocated
	;
	create temporary table _hyp_allocated as 
		select 	rt.fiscal_month_id,
				rt.audit_rec_src,
				rt.bar_entity,
				rt.bar_currtype,
				rt.soldtocust,
				rt.shiptocust,
				rt.bar_custno,
				rt.material,
				rt.bar_product,
				rt.bar_brand,
				rt.cost_pool,
				tran.amt_usd,
				rt.pct_of_total,
				rt.pct_of_total * tran.amt_usd as allocated_amt_usd
		from 	_hyp_amt as tran
				inner join rate_base_cogs_pct_of_total as rt
					on 	rt.fiscal_month_id = tran.fiscal_month_id
	;
	

/* DEBUG: compare input / output*/
--select 	1 as ord, 'Input-HYP' as category, 
--		sum(round(amt_usd,4)) as amt_usd, count(*)
--from 	_hyp_amt
--union all
--select 	2 as ord, 'Output-Cost2SSBU' as category, 
--		sum(round(allocated_amt_usd,4)) as amt_usd, count(*)
--from 	_hyp_allocated
--order by 1
--;

	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	stage.agm_allocated_data_rule_105
	where 	fiscal_month_id = (select fiscal_month_id from vtbl_date_range)
	;
	/* load to final transaction table (AGM: Labor/OH Adj) */
	INSERT INTO stage.agm_allocated_data_rule_105 (
				source_system,
				fiscal_month_id,
				posting_week_enddate,
			
				bar_entity,
				bar_acct,
				
				material,
				bar_product,
				bar_brand,
				
				soldtocust,
				shiptocust,
				bar_custno,
				
				dataprocessing_ruleid,
				dataprocessing_outcome_id,
				dataprocessing_phase,
				
				cost_pool,
				super_sbu,
				
				bar_currtype,
				allocated_amt,
				allocated_amt_usd,
				
				audit_loadts
		)
		select 	stg.audit_rec_src as source_system,
				stg.fiscal_month_id,
				dt.range_end_date as posting_week_enddate,
			
				stg.bar_entity,
				'AGM_ADJ_LABOH' as bar_acct,
				
				stg.material,
				stg.bar_product,
				stg.bar_brand as bar_brand,
				
				stg.soldtocust,
				'unknown' as shiptocust,
				stg.bar_custno,
				
				105 as dataprocessing_ruleid,
				1 as dataprocessing_outcome_id,
				'phase 25' as dataprocessing_phase,
				
				/* these are N/A because they are no longer factors in the allocation logic */
				'n/a' as cost_pool,
				'n/a' as super_sbu,
				
				stg.bar_currtype,
				case 
					when fx.from_currtype is null then stg.allocated_amt_usd
					else CAST(stg.allocated_amt_usd as decimal(20,8))
						/ CAST(fx.fxrate as decimal(20,8))
				end as allocated_amt,
				stg.allocated_amt_usd,
				
				getdate() audit_loadts
		from 	_hyp_allocated as stg
				inner join vtbl_date_range as dt
					on 	dt.fiscal_month_id = stg.fiscal_month_id				
				left outer join vtbl_exchange_rate as fx
					on 	fx.fiscal_month_id = stg.fiscal_month_id and 
						lower(fx.from_currtype) = lower(stg.bar_currtype)
	;
/* DEBUG: compare BODS Input vs BAR Input */
--select 	1 as ord, 'Input-BODS' as category, 
--		sum(round(allocated_amt_usd,4)) as amt,
--		count(*)
--from 	_hyp_allocated
--union all
--select 	2 as ord, 'Output-StageAGM' as category, 
--		sum(round(allocated_amt_usd,4)) as amt,
--		count(*)
--from 	stage.agm_allocated_data_rule_105
--where 	fiscal_month_id = 202101
--order by ord
--;

exception
when others then raise info 'exception occur while ingesting data in stage.p_allocate_data_rule_agm_105';
end;
$$
;

CREATE OR REPLACE PROCEDURE stage.p_build_allocation_rule_102_104(fmthid integer)
 LANGUAGE plpgsql
AS $$
declare
current_posting_week date;
calendar_posting_week date;
begin
	
---
/*
 
 Get records from agg table, rolled up to super SBU, Sku, Week
 
DONE
 
 insert records from this result joined with costing table into results
 
 
 Calculate the average % of net sales for PPV, Duty and Freight, by super sbu
 
 apply these averages to records that do not join to the costing table
 
 insert these records into results table 
 
 allocate from there
 
 * */	 
	 
	 
	/* create temp table for selected period */
	drop table if exists vtbl_date_range ;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				max(dt.fmth_id) AS fiscal_month_id
		from 	ref_data.calendar dt
	where 	dt.fmth_id = fmthid
	--where dt.fmth_id = 202101
	
	;
	 
 ---map _dim_prod_sku_to_super_sbu_map
     drop table if exists _dim_prod_sku_to_super_sbu_map
    ;
    create temporary table _dim_prod_sku_to_super_sbu_map as
    with
        cte_base as (
            select     dp.material,
                    dp.level04_bar as super_sbu,
                    sum(f.amt_usd) as amt_usd
            from     dw.fact_pnl_commercial_stacked f
                    inner join dw.dim_product dp on dp.product_key = f.product_key
            where     f.bar_acct = 'A40110' and
                    lower(dp.level04_bar) != 'unknown'
            group by dp.material,
                    dp.level04_bar
        ),
        cte_rnk as (
            select     base.material,
                    base.super_sbu,
                    base.amt_usd,
                    rank() over(partition by material order by amt_usd desc) as rnk
            from     cte_base as base
        )
        select     rnk.material,
                rnk.super_sbu
        from     cte_rnk as rnk
        where     rnk.rnk = 1
    ;
    
   
    drop table if exists invoice_mat; 
   
create temporary table invoice_mat as (
 select 	a.fiscal_month_id , 
 		max(a.posting_week_enddate) as posting_week_enddate , 
 		a.alloc_material as material, 
 		COALESCE (sbu.super_sbu,'unknown') as super_sbu,
	    sum(a.tran_volume  )  as total_qty, 
	    sum(a.amt_usd) as amt_usd
 from dw.fact_pnl_commercial_stacked a
 inner join _dim_prod_sku_to_super_sbu_map sbu on lower(sbu.material) = lower(a.alloc_material)  
	 inner join vtbl_date_range dt on a.fiscal_month_id  = dt.fiscal_month_id
	 inner join dw.dim_business_unit bu on a.business_unit_key  = bu.business_unit_key 
	 inner join ref_data.data_processing_rule as dpr on dpr.data_processing_ruleid = a.mapped_dataprocessing_ruleid
	 inner join dw.dim_source_system dss on a.source_system_id  = dss.source_system_id 
	where
	  a.bar_acct   = 'A40110' -- Sales Invoice
	  and dpr.dataprocessing_group = 'perfect-data'
	 group by a.fiscal_month_id, a.alloc_material, COALESCE (sbu.super_sbu,'unknown')
	 having sum(a.tran_volume) > 0 
	 	   and sum(amt_usd)>0 
	 );
	

delete from  stage.agm_costing_variance where fiscal_month_id  in (select fiscal_month_id from vtbl_date_range);
insert into stage.agm_costing_variance (sku_match, fiscal_month_id,posting_week_enddate, material, super_sbu, total_qty, ppv_var, duty_var, frght_var )
 select 1 as sku_match,  
	i.fiscal_month_id, 
	i.posting_week_enddate, 
	i.material, 
	i.super_sbu, 
	i.total_qty, 
	i.total_qty * avg_ppv_var as ppv_var,
	i.total_qty * avg_duty_var as duty_var,
	i.total_qty * avg_fgt_abs_var frght_var
from invoice_mat i inner join stage.agm_1070_costing c on i.material = c.matnr and i.fiscal_month_id = c.fiscal_month_id
;


--
--
--
--Select sum(total_qty) volume, sum(ppv_var) ppv_var, material
--from stage.agm_costing_variance
----where lower(material) = 'dck277c2'
--group by material
-- select     dp.material,
--            sum(f.invoice_sales) as  invoice_sales, 
--            sum(net_sales) as net_sales, 
--            sum(tran_volume*-1) as volume
--  from     dw.fact_pnl_commercial f
--  inner join dw.dim_product dp on dp.product_key = f.product_key
--where 	lower(material) = 'dck277c2'
--and fiscal_month_id = 202101 
--group by dp.material
	--Calc Percent of net sales
	--cost/bar_amt by super SBU
	
	drop table if exists pct_net_sales; 
	
 	create temporary table pct_net_sales as 
 	(
	SELECT cv.super_sbu, inv.total_amt, cv.total_ppv, cv.total_duty, cv.total_frght,total_qty,
		cv.total_ppv/inv.total_amt as ppv_pct_net_sales,
		cv.total_duty/inv.total_amt as duty_pct_net_sales,
		cv.total_frght/inv.total_amt as frght_pct_net_sales
	from 
	(
	select fiscal_month_id,super_sbu, sum(ppv_var) total_ppv, sum(duty_var) as total_duty, sum(frght_var) as total_frght,sum(total_qty) as total_qty
	from stage.agm_costing_variance
	group by super_sbu, fiscal_month_id
	) cv inner JOIN 
	(
	SELECT fiscal_month_id,super_sbu, sum( amt_usd) as total_amt
	from  invoice_mat i
	where exists (select 1 from stage.agm_costing_variance cv where  i.material = cv.material and sku_match=1 and i.fiscal_month_id = cv.fiscal_month_id)
	 group by super_sbu, fiscal_month_id
	 ) inv on cv.super_sbu = inv.super_sbu and cv.fiscal_month_id = inv.fiscal_month_id
	cross join vtbl_date_range dt 
	where cv.fiscal_month_id = dt.fiscal_month_id and inv.fiscal_month_id = dt.fiscal_month_id
	);

--
--
--select *
--from pct_net_sales
--Select *
--from stage.agm_costing_variance
--limit 10;
--
--
--Select sum(ppv_var), sum(total_qty),sku_match,super_sbu
--from stage.agm_costing_variance 
--group by sku_match,super_sbu
--order by sku_match
--
--Select sum(ppv_var),sum(ppv_var_1),super_sbu,sum(total_qty),avg(ppv_pct_net_sales)
--from (	select 0 as sku_match,  
--	i.fiscal_month_id, i.posting_week_enddate, i.material, i.super_sbu, i.total_qty, ppv_pct_net_sales,
--	i.total_qty * avg_ppv_per_qty as ppv_var,
--	i.amt_usd * ppv_pct_net_sales as ppv_var_1
----	i.total_qty * duty_pct_net_sales as duty_var,
----	i.total_qty * frght_pct_net_sales as frght_var
--	from invoice_mat i 
--	left join stage.agm_1070_costing c on i.material = c.matnr 
--	left join pct_net_sales r on i.super_sbu = r.super_sbu
--	where c.matnr  is null 
--	) 
--group by super_sbu
----SK chnaged to i.amt_usd * ppv_pct_net_sales as ppv_var
	delete from  stage.agm_costing_variance where sku_match = 0 and fiscal_month_id  in (select fiscal_month_id from vtbl_date_range);

	insert into stage.agm_costing_variance (sku_match,fiscal_month_id,posting_week_enddate,material,super_sbu,total_qty, ppv_var, duty_var,frght_var)
	select 0 as sku_match,  i.fiscal_month_id, i.posting_week_enddate, i.material, i.super_sbu, i.total_qty, 
	i.amt_usd * ppv_pct_net_sales as ppv_var,
	i.amt_usd * duty_pct_net_sales as duty_var,
	i.amt_usd * frght_pct_net_sales as frght_var
	from 
	invoice_mat i 
	left join (
			select distinct material from  stage.agm_costing_variance  c
									inner join vtbl_date_range dt on c.fiscal_month_id  = dt.fiscal_month_id
									where sku_match  =1
									)  c on i.material = c.material 
	left join pct_net_sales r on i.super_sbu = r.super_sbu 
	where c.material  is null 
	;

/*
--debug
	select super_sbu , sku_match,  count(*) as rec_count , sum(total_qty) total_qty,  sum(ppv_var) total_ppv, sum(frght_var) total_frght, sum(duty_var) total_duty
	from stage.agm_costing_variance
	group by super_sbu, sku_match
	order by super_sbu, sku_match
Select sum(ppv_var), sum(duty_var), sum(frght_var) 
from stage.agm_costing_variance
select sku_match, count(*) 
from  stage.agm_costing_variance
group by  sku_match

select *
from stage.agm_costing_variance
limit 10

	*/
----allocate data at transaction level : has leakage issues
/************************************************************************************************************************************/

  /* +ve sales with negative cogs for processing month */ 
       drop table if exists stage_sales_by_sku;
       create temporary table stage_sales_by_sku 
       as 
       Select sum(amt_usd) as invoice_sales, dp.material,acv.super_sbu 
       from dw.fact_pnl_commercial_stacked fpcs 
       inner join dw.dim_product dp on fpcs.product_key = dp.product_key 
       inner join ( select sum(total_bar_amt) as total_bar_amt, material
                           from stage.rate_base_cogs rb 
                           cross join vtbl_date_range dt 
                           where rb.fiscal_month_id = dt.fiscal_month_id 
                    group by material
                    having sum(total_bar_amt) < 0
                    ) rb on dp.material = rb.material 
       inner join stage.agm_costing_variance acv on fpcs.alloc_material = acv.material and fpcs.fiscal_month_id = acv.fiscal_month_id 
       cross join  vtbl_date_range dt 
       where fpcs.fiscal_month_id = dt.fiscal_month_id 
       and bar_acct  in ('A40110')
       ---and lower(material) = '00 20 06 us2'
       group by dp.material,acv.super_sbu
       having sum(amt) > 0;
---502361.74717423
-- Select *
-- from stage_sales_by_sku
-- where material = '3007272L';
--
--Select *
--from stage.agm_costing_variance
--where material = '3007272L'--and fiscal_month_id  = 202101;
--drop table if exists stage.rate_base_cogs_pct_of_total_test; 
--
--create table stage.rate_base_cogs_pct_of_total_test 
--as 
--select *
--from rate_base_cogs_pct_of_total;

       /* rate table based on standard cost */
       drop table if exists rate_base_cogs_pct_of_total;
       create temporary table rate_base_cogs_pct_of_total as 
             with
                    cte_rate_base_cogs as (
                           select       rb.audit_rec_src,
                           			rb.fiscal_month_id,
                                        rb.bar_entity,
                                        rb.soldtocust,
                                        rb.shiptocust,
                                        rb.bar_custno,
                                        rb.material,
                                        rb.bar_product,
                                        rb.bar_brand,
                                        rb.super_sbu,
                                        cast(rb.total_bar_amt as decimal(38,12)) as total_bar_amt,
                                        rb.cost_pool,
                                        rb.bar_currtype,
                                        cast(sum(rb.total_bar_amt) over( partition by rb.material,rb.fiscal_month_id, rb.super_sbu ) as decimal(38,18)) as total_bar_amt_partition,
                                        cast(rb.total_bar_amt as decimal(38,12)) / cast(sum(rb.total_bar_amt) over( partition by rb.material,rb.fiscal_month_id, rb.super_sbu ) as decimal(38,18)) 
                                        as pct_of_total
                           from   stage.rate_base_cogs rb
                           cross join vtbl_date_range dt 
                           inner join stage_sales_by_sku s on rb.material = s.material and s.super_sbu =rb.super_sbu 
                           where rb.fiscal_month_id = dt.fiscal_month_id
                           and rb.total_bar_amt < 0
                    ),cte_rate_base_cogs_1 as (                    
             			select  cte_rb.audit_rec_src,
             			  cte_rb.fiscal_month_id,
                           cte_rb.bar_entity,
                           cte_rb.soldtocust,
                           cte_rb.shiptocust,
                           cte_rb.bar_custno,
                           cte_rb.material,
                           cte_rb.bar_product,
                           cte_rb.bar_brand,
                           cte_rb.super_sbu,
                           cte_rb.total_bar_amt,
                           cte_rb.total_bar_amt_partition,
                           cte_rb.cost_pool,
                           cte_rb.bar_currtype,
                           cte_rb.pct_of_total*cte_rb.total_bar_amt as total_bar_amt_1,
                           cast(sum(cte_rb.pct_of_total*cte_rb.total_bar_amt) over( partition by cte_rb.material,cte_rb.fiscal_month_id, cte_rb.super_sbu ) as decimal(38,18)) as total_bar_amt_partition_1
           	from   cte_rate_base_cogs cte_rb
             where total_bar_amt_partition != 0
             )select        cte_rb.audit_rec_src,
             			  cte_rb.fiscal_month_id,
                           cte_rb.bar_entity,
                           cte_rb.soldtocust,
                           cte_rb.shiptocust,
                           cte_rb.bar_custno,
                           cte_rb.material,
                           cte_rb.bar_product,
                           cte_rb.bar_brand,
                           cte_rb.super_sbu,
                           cte_rb.total_bar_amt_1 as total_bar_amt,
                           cte_rb.cost_pool,
                           cte_rb.bar_currtype,
                           total_bar_amt_partition_1,
                           cast(cte_rb.total_bar_amt_1
                                 / cast(total_bar_amt_partition_1 as decimal(38,8)) as decimal(38,8)) as pct_of_total
             from   cte_rate_base_cogs_1 cte_rb
             where total_bar_amt_partition != 0 ;
          
--Select sum(pct_of_total)
--from rate_base_cogs_pct_of_total
--where material ='20566618R';
--
--select sum(pct_of_total)
--from stage.rate_base_cogs_pct_of_total_test 
--where material ='20566618R';
            
/* debug : should be 1 */ 
            /*
Select sum(pct_of_total),material,fiscal_month_id, super_sbu,avg(total_bar_amt)
from rate_base_cogs_pct_of_total
group by material,fiscal_month_id, super_sbu
having sum(pct_of_total)<1
order by 1
*/
       /* create temp table for exchange_rate */
       drop table if exists vtbl_exchange_rate
       ;
       create temporary table vtbl_exchange_rate as 
             select       rt.fiscal_month_id, 
                           rt.from_currtype,
                           rt.fxrate
             from   ref_data.hfmfxrates_current rt
                           inner join vtbl_date_range dt
                                 on     dt.fiscal_month_id = rt.fiscal_month_id 
             where lower(rt.to_currtype) = 'usd'
       ;   

delete from stage.agm_allocated_data_rule_102_104 where fiscal_month_id  = (select fiscal_month_id from vtbl_date_range)
--and bar_acct  in ('AGM-ADJ-DUTY', 'AGM-ADJ-FRGHT','AGM-ADJ-PPV')
and dataprocessing_phase in ('phase 22', 'phase 23', 'phase 24', 'phase 32', 'phase 33', 'phase 34');

/* if dd.range_end_date > calendar_posting_week, then (processing for current month) 
 * posting week = calendar_posting_week - 7 days else 
 * dd.range_end_date  (processing for previous months) 
 */
select distinct cast(wk_end_dte as date) 
into calendar_posting_week
from ref_data.calendar c 
where dy_dte = cast(getdate() as date);
select dd.range_end_date
into current_posting_week
from vtbl_date_range dd;
if current_posting_week >= calendar_posting_week
then 
   current_posting_week = calendar_posting_week - 7; 
end if;
INSERT INTO stage.agm_allocated_data_rule_102_104
(
  source_system,
  fiscal_month_id,
  posting_week_enddate,
  bar_entity,
  bar_acct,
  material,
  bar_product,
  bar_brand,
  soldtocust,
  shiptocust,
  bar_custno,
  dataprocessing_ruleid,
  dataprocessing_outcome_id,
  dataprocessing_phase,
  bar_currtype,
  super_sbu,
  cost_pool,
  allocated_amt,
  allocated_amt_usd,
  audit_loadts
)
SELECT stg.audit_rec_src as source_system,
       stg.fiscal_month_id,
       current_posting_week as posting_week_enddate,
       stg.bar_entity,
       cast('AGM-ADJ-DUTY' as varchar(20)) as bar_acct,
       stg.material,
       stg.bar_product,
       COALESCE(stg.bar_brand, 'unknown') as bar_brand,
       stg.soldtocust,
       stg.shiptocust,
       stg.bar_custno,
       cast(102 as integer) dataprocessing_ruleid,
       cast(1 as integer) dataprocessing_outcome_id,
        case when acv.sku_match = 1 then  cast('phase 22' as varchar(10)) else cast('phase 32' as varchar(10)) end  as dataprocessing_phase,
       stg.bar_currtype,
       stg.super_sbu,
       case when lower(stg.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(acv.duty_var as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12)))* (1/CAST(fx.fxrate as decimal(10,6))) 
       end allocated_amt,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(acv.duty_var as numeric(38,20)) / (1 / cast(stg.pct_of_total as numeric(38,20)))
       end allocated_amt_usd,
       cast(getdate() as timestamp) as audit_loadts
FROM rate_base_cogs_pct_of_total stg 
cross join vtbl_date_range dd 
inner join stage.agm_costing_variance acv 
                                        on  lower(stg.super_sbu) = lower(acv.super_sbu)
                                        and lower(stg.material) = lower(acv.material)
                                        and stg.fiscal_month_id = acv.fiscal_month_id
left outer join vtbl_exchange_rate as fx
                                 on     fx.fiscal_month_id = stg.fiscal_month_id and 
                                        lower(fx.from_currtype) = lower(stg.bar_currtype)
where cast(stg.pct_of_total as numeric(38,12)) !=0
union all 
SELECT stg.audit_rec_src as source_system,
       stg.fiscal_month_id,
       current_posting_week as posting_week_enddate,
       stg.bar_entity,
       cast('AGM-ADJ-FRGHT' as varchar(20)) as bar_acct,
       stg.material,
       stg.bar_product,
       COALESCE(stg.bar_brand, 'unknown') as bar_brand,
       stg.soldtocust,
       stg.shiptocust,
       stg.bar_custno,
       cast(103 as integer) dataprocessing_ruleid,
       cast(1 as integer) dataprocessing_outcome_id,
        case when acv.sku_match = 1 then  cast('phase 23' as varchar(10)) else cast('phase 33' as varchar(10)) end  as dataprocessing_phase,
       stg.bar_currtype,
       stg.super_sbu,
         case when lower(stg.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(acv.frght_var as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12)))* (1/CAST(fx.fxrate as decimal(10,6))) 
       end allocated_amt,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(acv.frght_var as numeric(38,20)) / (1 / cast(stg.pct_of_total as numeric(38,20)))
       end allocated_amt_usd,
       cast(getdate() as timestamp) as audit_loadts
FROM rate_base_cogs_pct_of_total stg 
cross join vtbl_date_range dd 
inner join stage.agm_costing_variance acv 
                                        on  lower(stg.super_sbu) = lower(acv.super_sbu)
                                        and lower(stg.material) = lower(acv.material)
                                        and stg.fiscal_month_id = acv.fiscal_month_id
left outer join vtbl_exchange_rate as fx
                                 on     fx.fiscal_month_id = stg.fiscal_month_id and 
                                        lower(fx.from_currtype) = lower(stg.bar_currtype)
where cast(stg.pct_of_total as numeric(38,12)) !=0
union all 
SELECT stg.audit_rec_src as source_system,
       stg.fiscal_month_id,
       current_posting_week as posting_week_enddate,
       stg.bar_entity,
       cast('AGM-ADJ-PPV' as varchar(20)) as bar_acct,
       stg.material,
       stg.bar_product,
       COALESCE(stg.bar_brand, 'unknown') as bar_brand,
       stg.soldtocust,
       stg.shiptocust,
       stg.bar_custno,
       cast(104  as integer) dataprocessing_ruleid,
       cast(1 as integer) dataprocessing_outcome_id,
       case when acv.sku_match = 1 then  cast('phase 24' as varchar(10)) else cast('phase 34' as varchar(10)) end  as dataprocessing_phase,
       stg.bar_currtype,
       stg.super_sbu,
       case when lower(stg.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(acv.ppv_var as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12)))* (1/CAST(fx.fxrate as decimal(10,6))) 
       end allocated_amt,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(acv.ppv_var as numeric(38,20)) / (1 / cast(stg.pct_of_total as numeric(38,20)))
       
       end allocated_amt_usd,
       cast(getdate() as timestamp) as audit_loadts
FROM rate_base_cogs_pct_of_total stg 
cross join vtbl_date_range dd 
inner join stage.agm_costing_variance acv 
                                        on  lower(stg.super_sbu) = lower(acv.super_sbu)
                                        and lower(stg.material) = lower(acv.material)
                                        and stg.fiscal_month_id = acv.fiscal_month_id
left outer join vtbl_exchange_rate as fx
                                 on     fx.fiscal_month_id = stg.fiscal_month_id and 
                                        lower(fx.from_currtype) = lower(stg.bar_currtype)
where cast(stg.pct_of_total as numeric(38,12)) !=0
;


	end;
  $$
;

CREATE OR REPLACE PROCEDURE stage.p_build_allocation_rule_102_104_gap(fmthid integer)
 LANGUAGE plpgsql
AS $$
 BEGIN 
	 
	 
	 /*
	  * Step 1  Bods transactions
	  * Some bods transactions do not have material.. 
	  		spread these values portortionally based on bods transactiosn that do have material
	  * 
	  * call stage.p_build_allocation_rule_102_104_gap (202101)
	  */
	 
	 

	 	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				max(dt.fmth_id) AS fiscal_month_id
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
		--where dt.fmth_id  = 202101
	;
	 
	 
	 drop table if exists _dim_prod_sku_to_super_sbu_map ;
	
    create temporary table _dim_prod_sku_to_super_sbu_map as
    with
        cte_base as (
            select     dp.material,
                    dp.level04_bar as super_sbu,
                    sum(f.amt_usd) as amt_usd
            from     dw.fact_pnl_commercial_stacked f
                    inner join dw.dim_product dp on dp.product_key = f.product_key
            where     f.bar_acct = 'A40110' and
                    lower(dp.level04_bar) != 'unknown'
            group by dp.material,
                    dp.level04_bar
        ),
        cte_rnk as (
            select     base.material,
                    base.super_sbu,
                    base.amt_usd,
                    rank() over(partition by material order by amt_usd desc) as rnk
            from     cte_base as base
        )
        select     rnk.material,
                rnk.super_sbu
        from     cte_rnk as rnk
        where     rnk.rnk = 1;
    
	
       
      drop  table if exists calculate_costs ;
      create temporary table calculate_costs 
      as (
	
		Select super_sbu,
			  case when bar_acct = 'AGM-ADJ-DUTY' then 'Reported Duty / Tariffs'
			  	  when  bar_acct = 'AGM-ADJ-FRGHT' then 'Reported Freight'
			  	  when  bar_acct = 'AGM-ADJ-PPV' then 'Reported PPV'
			  end as acct_category, 
			  sum(allocated_amt_usd) cost_var
		from stage.agm_allocated_data_rule_102_104 aadr 
		inner join vtbl_date_range  dd on 	dd.fiscal_month_id = aadr.fiscal_month_id
		--where aadr.dataprocessing_phase in ('phase 22','phase 23','phase 24')
		where aadr.dataprocessing_phase in ('phase 22','phase 23','phase 24', 'phase 32', 'phase 33', 'phase 34')
		group by super_sbu,
			  case when bar_acct = 'AGM-ADJ-DUTY' then 'Reported Duty / Tariffs'
			  	  when  bar_acct = 'AGM-ADJ-FRGHT' then 'Reported Freight'
			  	  when  bar_acct = 'AGM-ADJ-PPV' then 'Reported PPV'
			  end
	
	) ;

	/*--
	select * from calculate_costs
	*/

 -- Now get Percent of variance by Super SBU
	drop table if exists bods_calc_rate;
	create temporary table  bods_calc_rate as
	(
	SELECT 
		a.acct_category, a.super_sbu, a.cost_var, a.cost_var/ b.bods_calc_var_category prcnt_of_gap
	from 
	calculate_costs a 
	inner join 
	(select acct_category, sum(cost_var) as bods_calc_var_category from calculate_costs group by acct_category) b on a.acct_category = b.acct_category
	);
	
/*
	-- should equal 100%..
	select acct_category, sum(prcnt_of_gap) from bods_calc_rate group by acct_category
	select acct_category, sum(prcnt_of_gap), super_sbu from bods_calc_rate group by acct_category, super_sbu
	
	Reported Duty / Tariffs	0.9998
	Reported PPV	1.0000
	Reported Freight	0.9999
*/
	drop table if exists bar_bods_var;
	-- get the BAR and Bods VARIANCE(
	create TEMPORARY table bar_bods_var as 
	(
	select bar.acct_category, bar.bar_cost,  bods.calc_cost,  bar.bar_cost -  COALESCE(bods.calc_cost,0) as bar_calc_var
	from 
	(
	
	select b.acct_category , 
		  sum(a.amt_reported*multiplication_factor) bar_cost
	from ref_data.agm_bnr_financials_extract a 
	inner join ref_data.pnl_acct_agm b on a.account = b.bar_acct
	inner join vtbl_date_range  dd on 	dd.fiscal_month_id = a.fiscal_month_id
	inner join (select distinct name, level4 from ref_data.entity) as rbh on   a.entity = rbh.name
	WHERE 	b.acct_category  in ('Reported Duty / Tariffs', 'Reported Freight','Reported PPV')
		and  a.scenario = 'Actual_Ledger'
    	and rbh.level4 = 'GTS_NA'
	group by  b.acct_category
	) bar
	LEFT JOIN 
	(
	select acct_category, sum(cost_var) calc_cost
	from calculate_costs
	group by acct_category
	) bods on bods.acct_category = bar.acct_category
	
	);
  /*
   select * from bar_bods_var
   */
  
	drop table if exists final_gap;
	create temporary table  final_gap as (
	SELECT b.acct_category, r.super_sbu, b.bar_cost,  b.calc_cost,   cast(b.bar_calc_var as decimal(20,8))  as bar_bods , r.prcnt_of_gap, 
	( cast(b.bar_calc_var as decimal(20,8))  *  r.prcnt_of_gap) gap_to_alloc
	from 
	bar_bods_var b 
	inner join bods_calc_rate r on b.acct_category = r.acct_category 
	);
	delete from  stage.agm_cost_variance_gap_final_gap_transient;
	insert into stage.agm_cost_variance_gap_final_gap_transient(acct_category, super_sbu, bar_cost, calc_cost, bar_bods, prcnt_of_gap, gap_to_alloc)
	select acct_category, super_sbu, bar_cost, calc_cost, bar_bods, prcnt_of_gap, gap_to_alloc
	from final_gap;

/*
	select *
	from final_gap
	where acct_category = 'Reported PPV'
	group by acct_category
	
	select * 
	from stage.agm_cost_variance_gap_final_gap_transient
	order by acct_category, super_sbu
	
	select acct_category,sum(prcnt_of_gap)
	from final_gap
	group by acct_category
	
*/
/*  BEGIN ALLOCATION BY % Cogs  
 */

   /* +ve sales with negative cogs for processing month */ 
       drop table if exists stage_sales_by_sku;
       create temporary table stage_sales_by_sku 
       as 
       Select sum(amt_usd) as invoice_sales, dp.material
       from dw.fact_pnl_commercial_stacked fpcs 
       inner join dw.dim_product dp on fpcs.product_key = dp.product_key 
       inner join ( select sum(total_bar_amt) as total_bar_amt, material
                           from stage.rate_base_cogs rb 
                           cross join vtbl_date_range dt 
                           where rb.fiscal_month_id = dt.fiscal_month_id 
                    group by material
                    having sum(total_bar_amt) < 0
                    ) rb on dp.material = rb.material 
       cross join  vtbl_date_range dt 
       where fpcs.fiscal_month_id = dt.fiscal_month_id 
       and bar_acct  in ('A40110')
       ---and lower(material) = '00 20 06 us2'
       group by dp.material
       having sum(amt) > 0;
      
       /* rate table based on standard cost */
       drop table if exists rate_base_cogs_pct_of_total;
       create temporary table rate_base_cogs_pct_of_total as 
             with
                    cte_rate_base_cogs as (
                           select       rb.fiscal_month_id,
                                        rb.bar_entity,
                                        rb.soldtocust,
                                        rb.shiptocust,
                                        rb.bar_custno,
                                        rb.material,
                                        rb.bar_product,
                                        rb.bar_brand,
                                        rb.super_sbu,
                                        rb.total_bar_amt,
                                        rb.cost_pool,
                                        rb.bar_currtype,
                                        sum(rb.total_bar_amt) over( partition by rb.fiscal_month_id, rb.super_sbu ) as total_bar_amt_partition
                           from   stage.rate_base_cogs rb
                           cross join vtbl_date_range dt 
                           
                           inner join stage_sales_by_sku s on rb.material = s.material 
                           
                           where rb.fiscal_month_id = dt.fiscal_month_id
                    )
             select       cte_rb.fiscal_month_id,
                           cte_rb.bar_entity,
                           cte_rb.soldtocust,
                           cte_rb.shiptocust,
                           cte_rb.bar_custno,
                           cte_rb.material,
                           cte_rb.bar_product,
                           cte_rb.bar_brand,
                           cte_rb.super_sbu,
                           cte_rb.total_bar_amt,
                           cte_rb.total_bar_amt_partition,
                           cte_rb.cost_pool,
                           cte_rb.bar_currtype,
                           CAST(cte_rb.total_bar_amt as decimal(20,8))
                                 / CAST(cte_rb.total_bar_amt_partition as decimal(20,8)) as pct_of_total
             from   cte_rate_base_cogs cte_rb
             where total_bar_amt_partition != 0
       ;
      /*
  --DEBUG
Select sum(pct_of_total), super_sbu
from rate_base_cogs_pct_of_total
group by super_sbu
select *
from rate_base_cogs_pct_of_total 
where super_sbu = 'PTG'
limit 100

*/
      
drop table if exists bnr_gap_to_allocate_for_cv; 
create temporary table bnr_gap_to_allocate_for_cv 
diststyle all 
as 
select super_sbu, 
             sum(case when acct_category = 'Reported Duty / Tariffs' then  gap_to_alloc else 0 end) as gap_to_allocate_duty,
             sum(case when acct_category = 'Reported Freight' then  gap_to_alloc else 0 end) as gap_to_allocate_freight,
             sum(case when acct_category = 'Reported PPV' then  gap_to_alloc else 0 end) as gap_to_allocate_ppv
    from stage.agm_cost_variance_gap_final_gap_transient
    group by  super_sbu;
   
   
   
--select * from  bnr_gap_to_allocate_for_cv
       /* create temp table for exchange_rate */
       drop table if exists vtbl_exchange_rate
       ;
       create temporary table vtbl_exchange_rate as 
             select       rt.fiscal_month_id, 
                           rt.from_currtype,
                           rt.fxrate
             from   ref_data.hfmfxrates_current rt
                           inner join vtbl_date_range dt
                                 on     dt.fiscal_month_id = rt.fiscal_month_id 
             where lower(rt.to_currtype) = 'usd'
       ;   
   

      
delete from stage.agm_allocated_data_rule_102_104 where fiscal_month_id  = (select fiscal_month_id from vtbl_date_range)
	and dataprocessing_phase in  ('phase 92', 'phase 93', 'phase 94') ;

INSERT INTO stage.agm_allocated_data_rule_102_104
(
  source_system,
  fiscal_month_id,
  posting_week_enddate,
  bar_entity,
  bar_acct,
  material,
  bar_product,
  bar_brand,
  soldtocust,
  shiptocust,
  bar_custno,
  dataprocessing_ruleid,
  dataprocessing_outcome_id,
  dataprocessing_phase,
  bar_currtype,
  super_sbu,
  cost_pool,
  allocated_amt,
  allocated_amt_usd,
  audit_loadts
)
SELECT cast('adj-cv-tran-gap' as varchar(20)) as source_system,
       dd.fiscal_month_id,
       dd.range_end_date as posting_week_enddate,
       stg.bar_entity,
       cast('AGM-ADJ-DUTY-GAP' as varchar(20)) as bar_acct,
       stg.material,
       stg.bar_product,
       COALESCE( stg.bar_brand, 'unknown') as bar_brand,
       stg.soldtocust,
       stg.shiptocust,
       stg.bar_custno,
       cast(102 as integer) dataprocessing_ruleid,
       cast(1 as integer) dataprocessing_outcome_id,
       cast('phase 92' as varchar(10)) as dataprocessing_phase,
       stg.bar_currtype,
       stg.super_sbu,
      -- cast(stg.pct_of_total as numeric(38,12)) as pct_of_total,
       case when lower(stg.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(cv.gap_to_allocate_duty as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12)))* (1/CAST(fx.fxrate as decimal(10,6))) 
       end allocated_amt,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(cv.gap_to_allocate_duty as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12))) 
       end allocated_amt_usd,
       cast(getdate() as timestamp) as audit_loadts
FROM rate_base_cogs_pct_of_total stg 
cross join vtbl_date_range dd 
inner join bnr_gap_to_allocate_for_cv cv on  lower(stg.super_sbu) = lower(cv.super_sbu)
left outer join vtbl_exchange_rate as fx
                                 on     fx.fiscal_month_id = stg.fiscal_month_id and 
                                        lower(fx.from_currtype) = lower(stg.bar_currtype)
where cast(stg.pct_of_total as numeric(38,12)) !=0
;
INSERT INTO stage.agm_allocated_data_rule_102_104
(
  source_system,
  fiscal_month_id,
  posting_week_enddate,
  bar_entity,
  bar_acct,
  material,
  bar_product,
  bar_brand,
  soldtocust,
  shiptocust,
  bar_custno,
  dataprocessing_ruleid,
  dataprocessing_outcome_id,
  dataprocessing_phase,
  bar_currtype,
  super_sbu,
  cost_pool,
  allocated_amt,
  allocated_amt_usd,
  audit_loadts
)
SELECT cast('adj-cv-tran-gap' as varchar(20)) as source_system,
        dd.fiscal_month_id,
       dd.range_end_date as posting_week_enddate,
       stg.bar_entity,
       cast('AGM-ADJ-PPV-GAP' as varchar(20)) as bar_acct,
       stg.material,
       stg.bar_product,
       COALESCE( stg.bar_brand, 'unknown') as bar_brand,
       stg.soldtocust,
       stg.shiptocust,
       stg.bar_custno,
       cast(104 as integer) dataprocessing_ruleid,
       cast(1 as integer) dataprocessing_outcome_id,
       cast('phase 94' as varchar(10)) as dataprocessing_phase,
       stg.bar_currtype,
       stg.super_sbu,
       --cast(stg.pct_of_total as numeric(38,12)) as pct_of_total,
       case when lower(stg.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(cv.gap_to_allocate_ppv as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12)))* (1/CAST(fx.fxrate as decimal(10,6))) 
       end allocated_amt,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(cv.gap_to_allocate_ppv as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12))) 
       end allocated_amt_usd,
       cast(getdate() as timestamp) as audit_loadts
FROM rate_base_cogs_pct_of_total stg 
cross join vtbl_date_range dd
inner join bnr_gap_to_allocate_for_cv cv on  lower(stg.super_sbu) = lower(cv.super_sbu)
left outer join vtbl_exchange_rate as fx
                                 on     fx.fiscal_month_id = stg.fiscal_month_id and 
                                        lower(fx.from_currtype) = lower(stg.bar_currtype)
where cast(stg.pct_of_total as numeric(38,12)) !=0
;
INSERT INTO stage.agm_allocated_data_rule_102_104
(
  source_system,
  fiscal_month_id,
  posting_week_enddate,
  bar_entity,
  bar_acct,
  material,
  bar_product,
  bar_brand,
  soldtocust,
  shiptocust,
  bar_custno,
  dataprocessing_ruleid,
  dataprocessing_outcome_id,
  dataprocessing_phase,
  bar_currtype,
  super_sbu,
  cost_pool,
  allocated_amt,
  allocated_amt_usd,
  audit_loadts
)
SELECT cast('adj-cv-tran-gap' as varchar(20)) as source_system,
       dd.fiscal_month_id,
       dd.range_end_date as posting_week_enddate,
       stg.bar_entity,
       cast('AGM-ADJ-FRGHT-GAP' as varchar(20)) as bar_acct,
       stg.material,
       stg.bar_product,
       COALESCE( stg.bar_brand, 'unknown') as bar_brand,
       stg.soldtocust,
       stg.shiptocust,
       stg.bar_custno,
       cast(103 as integer) dataprocessing_ruleid,
       cast(1 as integer) dataprocessing_outcome_id,
       cast('phase 93' as varchar(10)) as dataprocessing_phase,
       stg.bar_currtype,
       stg.super_sbu,
       --cast(stg.pct_of_total as numeric(38,12)) as pct_of_total,
       case when lower(stg.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(cv.gap_to_allocate_freight as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12)))* (1/CAST(fx.fxrate as decimal(10,6))) 
       end allocated_amt,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(cv.gap_to_allocate_freight as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12))) 
       end allocated_amt_usd,
       cast(getdate() as timestamp) as audit_loadts
FROM rate_base_cogs_pct_of_total stg 
cross join vtbl_date_range dd
inner join bnr_gap_to_allocate_for_cv cv
                                        on  lower(stg.super_sbu) = lower(cv.super_sbu)
left outer join vtbl_exchange_rate as fx
                                 on     fx.fiscal_month_id = stg.fiscal_month_id and 
                                        lower(fx.from_currtype) = lower(stg.bar_currtype)
where cast(stg.pct_of_total as numeric(38,12)) !=0
;
	
END; 
  $$
;

CREATE OR REPLACE PROCEDURE stage.p_build_source_1070_costing(fmthid integer)
 LANGUAGE plpgsql
AS $$
 BEGIN 
	
/*
	 --Join for date Context
Invoices
inner join standard on material = material and period_enddate between FromDate and ToDate
inner join current on standard.material = current.material and current.fromdate between standard.fromdate and Todate
WHERE 
current.fromdate is the latest before i.period_endDate
	 
	 
*/
	 
--Invoices are used to limit the list of sku's needed for costing. 
	/* create temp table for selected period */
	drop table if exists vtbl_date_range ;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				cast(max(dt.fmth_begin_dte) as date) as first_of_month,
				max(dt.fmth_id) AS fiscal_month_id
		from 	ref_data.calendar dt
	where 	dt.fmth_id = fmthid
	
;
	
 ---map _dim_prod_sku_to_super_sbu_map
     drop table if exists _dim_prod_sku_to_super_sbu_map
    ;
    create temporary table _dim_prod_sku_to_super_sbu_map as
    with
        cte_base as (
            select     dp.material,
                    dp.level04_bar as super_sbu,
                    sum(f.amt_usd) as amt_usd
            from     dw.fact_pnl_commercial_stacked f
                    inner join dw.dim_product dp on dp.product_key = f.product_key
            where     f.bar_acct = 'A40110' and
                    lower(dp.level04_bar) != 'unknown'
            group by dp.material,
                    dp.level04_bar
        ),
        cte_rnk as (
            select     base.material,
                    base.super_sbu,
                    base.amt_usd,
                    rank() over(partition by material order by amt_usd desc) as rnk
            from     cte_base as base
        )
        select     rnk.material,
                rnk.super_sbu
        from     cte_rnk as rnk
        where     rnk.rnk = 1
    ;
    
   
    drop table if exists invoice; 
   
create temporary table invoice as (
 select 	a.fiscal_month_id , 
 		max(a.posting_week_enddate) as posting_week_enddate , 
 		a.alloc_material as material, 
 		COALESCE (sbu.super_sbu,'unknown') as super_sbu,
	    sum(a.tran_volume  )  as total_qty, 
	    sum(a.amt_usd) as amt_usd
 from dw.fact_pnl_commercial_stacked a
 inner join _dim_prod_sku_to_super_sbu_map sbu on lower(sbu.material) = lower(a.alloc_material)  
	 inner join vtbl_date_range dt on a.fiscal_month_id  = dt.fiscal_month_id
	 inner join dw.dim_business_unit bu on a.business_unit_key  = bu.business_unit_key 
	 inner join ref_data.data_processing_rule as dpr on dpr.data_processing_ruleid = a.mapped_dataprocessing_ruleid
	 inner join dw.dim_source_system dss on a.source_system_id  = dss.source_system_id 
	where
	  a.bar_acct   = 'A40110' -- Sales Invoice
	  and dpr.dataprocessing_group = 'perfect-data'
	    and sbu.super_sbu <> 'Product_None'
	 group by a.fiscal_month_id, a.alloc_material, COALESCE (sbu.super_sbu,'unknown')
	 having sum(a.tran_volume) > 0 
	 	   and sum(amt_usd)>0 
	 );
	

	 --standard Costs
	 drop table if exists vtbl_keko_standard;
	 
	--125 sec was  Now is 2 minutes
	 create temporary table vtbl_keko_standard  as (
	
	with vkeko as (
	 Select kalnr, matnr, bwkey
	,werks, klvar, kadat as fromdate , bidat as todate,  tvers, feh_sta  ,losgr 
	,bzobj
	,kalka, kadky
	 ,kkzma
	 ,bwvar
	from sapc11.keko_current k
	inner join vtbl_date_range dt on dt.first_of_month between cast(k.kadat as date) and cast(k.bidat as date)
	where 
		 --ZPC7 Std cost   
		klvar = 'ZPC7' 
		and feh_sta = 'FR'
	    and tvers  = '01' -- harded coded value should always be 1 for ZPC7 Records
       and trim(kkzma) = ''   -- Extended manually
       and bzobj = 0  -- Ref. object
      and k.matnr  in (select distinct material from invoice)  -- limit to materials with sales
	)	
        
   	select  a.matnr , b.klvar , a.bwkey as plant,  b.kalka, b.bwvar ,b.losgr,  cast(b.fromdate as date) fromdate, cast(b.todate as date) todate, g.waers as from_currtype,
	        	
	       ((c.kst001 + c.kst003 + c.kst005 + c.kst007 + c.kst009 + c.kst025 + c.kst027 + c.kst029+ c.kst031 + c.kst035) - (c.kst002 + c.kst004 + c.kst006 + c.kst008 + c.kst010 + c.kst026 + c.kst028 +  c.kst030+ c.kst032 + c.kst036) )/b.losgr as standard_tot_matl,
        	( c.kst005 - c.kst006)/ b.losgr as standard_fgt_abs,
	        ( c.kst035 - c.kst036)/ b.losgr as standard_duty
	        -- ,row_number() over (partition by a.matnr, a.bwkey order by b.fromdate desc  ) as row_nbr
       	FROM sapc11.mbew_current  a
		    inner JOIN vkeko  b ON  a.kaln1 = cast(b.kalnr as int) AND a.matnr = b.matnr AND  a.bwkey = b.werks 
		   left join sapc11.keph_current c on b.bzobj = coalesce(c.bzobj,'') and b.kalnr = c.kalnr and b.kalka = c.kalka and b.kadky = c.kadky and b.tvers = c.tvers  and b.kkzma = c.kkzma and b.bwvar = c.bwvar 
	       left join  sapc11.t001k_current f on a.bwkey = f.bwkey 
			left join sapc11.t001_current g on f.bukrs = g.bukrs 
    	where    
	        --these fields defaulted from ABAP program -- keeping them to keep logic same
	       COALESCE (trim(c.kkzst),'')  = '' -- Lower levels
        	and COALESCE (trim(c.losfx),'') = ''
	     	and COALESCE (trim(c.kkzmm),'') = ''
	        and coalesce (a.matnr,'') <> ''       
);  

-- WE now have standard costs for the materials of the month
-- get Current Costs
     --2m 1
     drop table if exists vtbl_keko_current;
	 
	 create temporary table vtbl_keko_current  as (  
     
     with vkeko as 
     	(
	    Select c.kalnr, c.matnr, c.bwkey
			,c.werks, c.klvar, c.kadat as fromdate , c.bidat as todate,  c.tvers, c.feh_sta, c.losgr 
			,c.bzobj
			,c.kalka
			,c.kadky
			 ,c.kkzma
			 ,c.bwvar
		 from sapc11.keko_current c
	 --	inner join vtbl_keko_standard s on c.matnr = s.matnr and c.werks = s.plant and cast(c.kadat as date) between s.fromdate and s.todate
	 	--Check this Join with Stan and Ken -- do we want the current cssts that belong to this Standard cost.. if so keep this join
	  --	inner join vtbl_date_range dt on  dt.first_of_month >= cast(c.kadat as date) 
	  	inner join vtbl_date_range dt on dt.first_of_month between cast(c.kadat as date) and cast(c.bidat as date)
	  -- Limit to CC's with start date prior to first day of month
	  	where 
			 --ZPC8 Current cost   
			c.klvar = 'ZPC8' 
			and c.feh_sta = 'KA'
	        and trim(c.kkzma) = ''   -- Extended manually
	        and c.bzobj = 0  -- Ref. object   
	        and cast(c.kadat as date) >= dateadd(year,-2, dt.first_of_month)
	  		and c.matnr in (select matnr from vtbl_keko_standard group by  matnr) -- limit to materials that have a Standard Cost
   )
    select 
    a.matnr , b.klvar , a.bwkey as plant,  b.kalka, b.bwvar ,b.losgr,  cast(b.fromdate as date) fromdate, cast(b.todate as date) todate, g.waers as from_currtype,
	        	
	       ((c.kst001 + c.kst003 + c.kst005 + c.kst007 + c.kst009 + c.kst025 + c.kst027 + c.kst029+ c.kst031 + c.kst035) - (c.kst002 + c.kst004 + c.kst006 + c.kst008 + c.kst010 + c.kst026 + c.kst028 +  c.kst030+ c.kst032 + c.kst036) )/b.losgr as current_tot_matl,
        	( c.kst005 - c.kst006)/ b.losgr as current_fgt_abs,
	        ( c.kst035 - c.kst036)/ b.losgr as current_duty
   -- ,row_number() over (partition by a.matnr, a.bwkey order by b.fromdate desc) as row_count
    FROM sapc11.mbew_current  a
	inner JOIN vkeko  b ON  a.kaln1 = cast(b.kalnr as int) AND a.matnr = b.matnr AND  a.bwkey = b.werks 
	left join sapc11.keph_current c on b.bzobj = coalesce(c.bzobj,'') and b.kalnr = c.kalnr and b.kalka = c.kalka and b.kadky = c.kadky and b.tvers = c.tvers  and b.kkzma = c.kkzma and b.bwvar = c.bwvar 
	left join  sapc11.t001k_current f on a.bwkey = f.bwkey 
	left join sapc11.t001_current g on f.bukrs = g.bukrs    	
    where    
        --these fields defaulted from ABAP program -- keeping them to keep logic same
         COALESCE (trim(c.kkzst),'')  = '' -- Lower levels
    	and COALESCE (trim(c.losfx),'') = ''
        and COALESCE (trim(c.kkzmm),'') = ''
        and coalesce (a.matnr,'') <> ''  
    
    );

   
	    /* create temp table for exchange_rate */
       drop table if exists vtbl_exchange_rate
       ;
       create temporary table vtbl_exchange_rate as 
             select       rt.fiscal_month_id, 
                           rt.from_currtype,
                           rt.fxrate
             from   ref_data.hfmfxrates_current rt
                           inner join vtbl_date_range dt
                                 on     dt.fiscal_month_id = rt.fiscal_month_id 
             where lower(rt.to_currtype) = 'usd'
       ;   
  
 delete from stage.agm_1070_costing_base where fiscal_month_id = fmthid;     
      
insert into stage.agm_1070_costing_base (
	matnr, plant, st_fromdate, st_todate, from_currtype,standard_tot_matl, standard_fgt_abs, standard_duty,
	  cc_fromdate, cc_todate,  current_tot_matl,  current_fgt_abs,  current_duty,
   	standard_pp, current_pp, fgt_abs_var, duty_var, ppv_var,fiscal_month_id
    )
 --create transient table     
with cc_base as (
	select  matnr,plant, fromdate, todate, from_currtype, current_tot_matl, current_fgt_abs, current_duty
	, ROW_NUMBER () over (Partition by matnr, plant order by fromdate desc) as row_nbr
	from vtbl_keko_current
	)
	
   select 
   	s.matnr, s.plant, s.fromdate st_fromdate, s.todate st_todate, s.from_currtype,
	  standard_tot_matl *  coalesce(fx.fxrate,1) as standard_tot_matl,
	  standard_fgt_abs *  coalesce(fx.fxrate,1) as standard_fgt_abs,
	  standard_duty *  coalesce(fx.fxrate,1) as standard_duty,
	  c.fromdate as cc_fromdate, c.todate as cc_todate,
	  current_tot_matl *  coalesce(fx.fxrate,1) as current_tot_matl,
	  current_fgt_abs * coalesce(fx.fxrate,1) as current_fgt_abs,
	  current_duty *  coalesce(fx.fxrate,1) as current_duty,
   	(standard_tot_matl - (standard_fgt_abs + standard_duty)) * coalesce(fx.fxrate,1) as standard_pp,
   	(current_tot_matl - (current_fgt_abs + current_duty)) * coalesce(fx.fxrate,1) as current_pp,
   	(standard_fgt_abs -  current_fgt_abs) * coalesce(fx.fxrate,1) as fgt_abs_var,
   	(standard_duty  - current_duty) *  coalesce(fx.fxrate,1) as duty_var,
   	((standard_tot_matl - (standard_fgt_abs + standard_duty))  - (current_tot_matl - (current_fgt_abs + current_duty)))   * coalesce(fx.fxrate,1) ppv_var,
    fmthid as fiscal_month_id
   	
    from vtbl_keko_standard s 
    inner join cc_base c on s.matnr = c.matnr and s.plant = c.plant 
    	--WARD   this is the line to decide on
    	--and c.fromdate >= s.fromdate
	left join vtbl_exchange_rate as fx on  lower(fx.from_currtype) = lower(s.from_currtype)
	where c.row_nbr = 1
--order by s.matnr , s.plant
;

delete from stage.agm_1070_costing where fiscal_month_id = fmthid;
insert into stage.agm_1070_costing(
	matnr, fiscal_month_id,
	avg_standard_tot_matl,
	avg_standard_fgt_abs,
	avg_standard_duty,
	avg_current_tot_matl,
	avg_current_fgt_abs,
	avg_current_duty,
	avg_standard_pp, 
	avg_current_pp,
	avg_fgt_abs_var, 
	avg_duty_var, 
	avg_ppv_var
)
select matnr, fiscal_month_id,
	avg(standard_tot_matl) avg_standard_tot_matl,
	avg(standard_fgt_abs) avg_standard_fgt_abs,
	avg(standard_duty) avg_standard_duty,
	avg(current_tot_matl ) avg_current_tot_matl,
	avg(current_fgt_abs) avg_current_fgt_abs,
	avg(current_duty) avg_current_duty,
	avg(standard_pp) avg_standard_pp, 
	avg(current_pp) avg_current_pp,
	avg(fgt_abs_var) avg_fgt_abs_var, 
	avg(duty_var) avg_duty_var, 
	avg(ppv_var) avg_ppv_var
from stage.agm_1070_costing_base
where fiscal_month_id = fmthid
group by matnr, fiscal_month_id
;
end;
$$
;

CREATE OR REPLACE PROCEDURE stage.p_build_source_core_tran_delta_agg(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN   
	
/* delete from bods_core_transaction_agg */
delete 
from stage.bods_core_transaction_agg
where 	0=0
--AND audit_rec_src = 'sap_c11'
and fiscal_month_id = fmthid;
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
from 	bods.drm_product_current
where 	loaddts = ( select max(loaddts) from bods.drm_product_current dpc );		
	/* create current version of bar_customer hierarchy */
	drop table if exists tmp_customer_bar_hierarchy
	;
	create temporary table tmp_customer_bar_hierarchy 
	DISTSTYLE ALL
	as
	with
		cte_cust_current as (
			select 	name as leaf,
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
					description, 
					membertype,
					cast(generation as int) as generation,
					level1 as level01,
					level2 as level02,
					level3 as level03,
					level4 as level04,
					level5 as level05,
					level6 as level06,
					level7 as level07,
					level8 as level08,
					level9 as level09,
					level10,
					level11
			from 	bods.drm_customer_current
			where 	loaddts = (select max(loaddts) from bods.drm_customer_current)
				and membertype != 'Parent'
		)
		select 	leaf as bar_custno,
				description as bar_customer_desc,
				level01 as bar_customer_level01,
				case when generation <= 2  then case when leaf = 'Customer_None' then leaf else parent end else level02 end as bar_customer_level02,
				case when generation <= 3  then case when leaf = 'Customer_None' then leaf else parent end else level03 end as bar_customer_level03,
				case when generation <= 4  then case when leaf = 'Customer_None' then leaf else parent end else level04 end as bar_customer_level04,
				case when generation <= 5  then case when leaf = 'Customer_None' then leaf else parent end else level05 end as bar_customer_level05,
				case when generation <= 6  then case when leaf = 'Customer_None' then leaf else parent end else level06 end as bar_customer_level06,
				case when generation <= 7  then case when leaf = 'Customer_None' then leaf else parent end else level07 end as bar_customer_level07,
				case when generation <= 8  then case when leaf = 'Customer_None' then leaf else parent end else level08 end as bar_customer_level08,
				case when generation <= 9  then case when leaf = 'Customer_None' then leaf else parent end else level09 end as bar_customer_level09,
				case when generation <= 10 then case when leaf = 'Customer_None' then leaf else parent end else level10 end as bar_customer_level10,
				leaf as bar_customer_level11, 
				membertype,
				generation,
				level01 as ragged_level01,
				level02 as ragged_level02,
				level03 as ragged_level03,
				level04 as ragged_level04,
				level05 as ragged_level05,
				level06 as ragged_level06,
				level07 as ragged_level07,
				level08 as ragged_level08,
				level09 as ragged_level09,
				level10 as ragged_level10,
				level11 as ragged_level11
		from 	cte_cust_current
	;
drop table if exists stage_core_transaction_agg;
--	/* insert from delta into bods_core_transaction_agg */
create temporary table stage_core_transaction_agg
diststyle even 
sortkey (posting_week_enddate)
as 
with marm_volume_conversion as (
Select matnr as material, 
	  max(ea) as ea, 
	  max(mp_factor) as mp,
	  max(loadtime) as loadtime
from (
		 Select mc_max.matnr,
			   loaddts as loadtime, 
			   case when mc.meinh = 'EA' then umrez else null end as ea,
			   case when mc.meinh = 'MP' then umrez else null end / case when mc.meinh = 'MP' then umren else null end as mp_factor
		from sapc11.marm_current mc 
		inner join (
		Select matnr ,meinh,max(loaddts) as max_loadtime
		from sapc11.marm_current
		where meinh in ('EA', 'MP')
		--and matnr in ('50310-PWR')
		group by matnr,meinh
		) mc_max on mc.loaddts = mc_max.max_loadtime 
		and mc.matnr = mc_max.matnr
		and mc.meinh = mc_max.meinh
	) 
group by matnr
)	SELECT 	audit_rec_src,
				ctdc.bar_year,
				bar_period, 
				bar_entity,
				ctdc.bar_acct, 
				/* standardizing string format trimmed + lowercase */
				shiptocust AS shiptocust, 
				ctdc.soldtocust AS soldtocust, 
				ctdc.org_bar_custno AS org_bar_custno, 
				ctdc.mapped_bar_custno,
				ctdc.material AS material, 
				ctdc.org_bar_product,
				ctdc.mapped_bar_product,
				ctdc.org_bar_brand,
				ctdc.mapped_bar_brand,
				sum(case when audit_rec_src = 'hfm' 
						  then cast(bar_amt as numeric(19,6))  
						  else cast(bar_amt as numeric(19,6))*-1
						  end) as bar_amt,
				ctdc.bar_currtype,
				sum(case when ctdc.bar_acct in ('A40110', 'A40116','A40210', 'A40111','A40310','A40120', 'A40410','A40510', 'A40610', 'A40710', 'A40910','A41110', 'A41210')
							and lower(ctdc.material) = lower(sku)
							and lower(ctdc.quanunit) = 'ea'
						then cast(isnull(quantity,0) as decimal(38,8))  / (case when ConversionRate is null then 1 else ConversionRate end)
						when ctdc.bar_acct in ('A40110', 'A40116','A40210', 'A40111','A40310','A40120', 'A40410','A40510', 'A40610', 'A40710', 'A40910','A41110', 'A41210')
							and lower(level07_bar) = 'anf_div' 
							and lower(level08_bar) not LIKE '%chem_fast%'
							and lower(ctdc.quanunit) = 'ea'
						then cast(isnull(quantity,0) as decimal(38,8))  / (case when mp is null then 1 else mp end)
						when ctdc.bar_acct in ('A40110', 'A40116','A40210', 'A40111','A40310','A40120', 'A40410','A40510', 'A40610', 'A40710', 'A40910','A41110', 'A41210')
							and lower(level07_bar) = 'anf_div' 
							and lower(level08_bar) LIKE '%chem_fast%'
							and lower(ctdc.quanunit) = 'ea'
						then cast(isnull(quantity,0) as decimal(38,8)) 
						when  ctdc.bar_acct in ('A40110', 'A40116','A40210', 'A40111','A40310','A40120', 'A40410','A40510', 'A40610', 'A40710', 'A40910','A41110', 'A41210')
							and lower(level07_bar) != 'anf_div' 
						then cast(isnull(quantity,0) as decimal(38,8)) 
						else cast(0 as decimal(38,0))
					end) as sales_volume,
  	 			sum(case when ctdc.bar_acct in ('A40110', 'A40116','A40210', 'A40111','A40310','A40120', 'A40410','A40510', 'A40610', 'A40710', 'A40910','A41110', 'A41210')
                			then cast(isnull(quantity,0) as decimal(38,8)) 
                			else cast(0 as decimal(38,0))
         		 		end) as tran_volume,
         		 	lower(ctdc.quanunit) as uom,
				posting_week_enddate, 
				fiscal_month_id,
				org_dataprocessing_ruleid,
				--rsa account logic for mapping rule id
				CASE
					WHEN	ctdc.bar_acct = 'A40115' and ctdc.audit_rec_src = 'sap_c11' and bcb.ragged_level06 = 'Retail'
						then 	acct_except.data_processing_ruleid 
					WHEN ctdc.bar_acct in ('A40111', 'A60111')  
					     THEN COALESCE (acct_except.data_processing_ruleid,rar_m.data_processing_ruleid)
					WHEN 	lower(ctdc.org_bar_product) = 'product_none' OR 
							lower(ctdc.org_bar_custno) = 'customer_none'
						THEN (
							SELECT 	max(data_processing_ruleid)
							FROM 	ref_data.data_processing_rule
							WHERE 	dataprocessing_group = 'cleansing - Product_None / Customer_None'
						)
					WHEN 	lower(ctdc.org_bar_product) in ('oth_service','p60999') OR 
							lower(ctdc.org_bar_custno) = 'psd_oth'
						THEN (
							SELECT 	max(data_processing_ruleid)
							FROM 	ref_data.data_processing_rule
							WHERE 	dataprocessing_group = 'cleansing - OTH / PSD_Oth'
						)
					when 	ctdc.bar_acct = 'A40115' and 
							( ctdc.audit_rec_src not in  ('sap_c11') or bcb.ragged_level06 not in ('Retail') or bcb.ragged_level06 is null )
						then 	org_dataprocessing_ruleid
					else 
						COALESCE (acct_except.data_processing_ruleid,rar_m.data_processing_ruleid) 
				end as mapped_dataprocessing_ruleid ,
				getdate() as audit_loadts
		From 
		(		
			Select ctdc.*,rar_o.data_processing_ruleid as org_dataprocessing_ruleid
			from stage.core_tran_delta_cleansed ctdc
			LEFT JOIN ref_data.data_processing_rule rar_o  on ctdc.org_dataprocessing_hash = rar_o.dataprocessing_hash
		) ctdc
		LEFT JOIN ref_data.data_processing_rule acct_except on ctdc.bar_acct = acct_except.bar_acct and acct_except.dataprocessing_group LIKE 'acct exception%'
		LEFT JOIN ref_data.data_processing_rule rar_m on ctdc.mapped_dataprocessing_hash = rar_m.dataprocessing_hash 	
		LEFT join marm_volume_conversion v on ctdc.material  = v.material 
		LEFT join bar_product_base bpb on lower(ctdc.mapped_bar_product) = lower(bpb.bar_product)
		LEFT JOIN ref_data.volume_conv_sku vol_sku on lower(ctdc.material) = lower(sku)
		LEFT join tmp_customer_bar_hierarchy bcb on lower(ctdc.mapped_bar_custno) = lower(bcb.bar_custno) 
		where ctdc.fiscal_month_id = fmthid  ---input fiscal month
		group by audit_rec_src,
				ctdc.bar_year,
				bar_period, 
				bar_entity,
				ctdc.bar_acct, 
				shiptocust, 
				soldtocust, 
				org_bar_custno,
				mapped_bar_custno,
				ctdc.material, 
				org_bar_product, 
				mapped_bar_product,
				org_bar_brand,
				mapped_bar_brand,
				lower(ctdc.quanunit),
				bar_currtype,
				posting_week_enddate, 
				fiscal_month_id,
				org_dataprocessing_ruleid,
				--rsa account logic for mapping rule id
				CASE
					WHEN	ctdc.bar_acct = 'A40115' and ctdc.audit_rec_src = 'sap_c11' and bcb.ragged_level06 = 'Retail'
						then 	acct_except.data_processing_ruleid 
					WHEN ctdc.bar_acct in ('A40111', 'A60111')  
					     THEN COALESCE (acct_except.data_processing_ruleid,rar_m.data_processing_ruleid)
					WHEN 	lower(ctdc.org_bar_product) = 'product_none' OR 
							lower(ctdc.org_bar_custno) = 'customer_none'
						THEN (
							SELECT 	max(data_processing_ruleid)
							FROM 	ref_data.data_processing_rule
							WHERE 	dataprocessing_group = 'cleansing - Product_None / Customer_None'
						)
					WHEN 	lower(ctdc.org_bar_product) in ('oth_service','p60999') OR 
							lower(ctdc.org_bar_custno) = 'psd_oth'
						THEN (
							SELECT 	max(data_processing_ruleid)
							FROM 	ref_data.data_processing_rule
							WHERE 	dataprocessing_group = 'cleansing - OTH / PSD_Oth'
						)
					when 	ctdc.bar_acct = 'A40115' and 
							( ctdc.audit_rec_src not in  ('sap_c11') or bcb.ragged_level06 not in ('Retail') or bcb.ragged_level06 is null )
						then 	org_dataprocessing_ruleid
					else 
						COALESCE (acct_except.data_processing_ruleid,rar_m.data_processing_ruleid) 
				end;

INSERT INTO stage.bods_core_transaction_agg
(
  audit_rec_src,
  bar_year,
  bar_period,
  bar_entity,
  bar_acct,
  shiptocust,
  soldtocust,
  org_bar_custno,
  mapped_bar_custno,
  material,
  org_bar_product,
  mapped_bar_product,
  org_bar_brand,
  mapped_bar_brand,
  bar_amt,
  bar_currtype,
  sales_volume,
  tran_volume,
  uom,
  posting_week_enddate,
  fiscal_month_id,
  org_dataprocessing_ruleid,
  mapped_dataprocessing_ruleid,
  audit_loadts
)
Select  
  audit_rec_src,
  bar_year,
  bar_period,
  bar_entity,
  bar_acct,
  shiptocust,
  soldtocust,
  org_bar_custno,
  mapped_bar_custno,
  material,
  org_bar_product,
  mapped_bar_product,
  org_bar_brand,
  mapped_bar_brand,
  bar_amt,
  bar_currtype,
  (case when audit_rec_src = 'sap_lawson' then 1 else -1 end) * sales_volume as sales_volume,
  (case when audit_rec_src = 'sap_lawson' then 1 else -1 end) * tran_volume as tran_volume,
  uom,
  posting_week_enddate,
  fiscal_month_id,
  org_dataprocessing_ruleid,
  mapped_dataprocessing_ruleid,
  audit_loadts
from stage_core_transaction_agg;

exception
when others then raise info 'exception occur while ingesting data in bods_core_transaction_agg_c11';
end
$$
;

CREATE OR REPLACE PROCEDURE stage.p_build_source_core_tran_delta_agg_agm(fmthid integer)
 LANGUAGE plpgsql
AS $$
	/*
	 * 		
	 *		call stage.p_build_source_core_tran_delta_agg_agm (202101)
	 * 		select count(*) from stage.bods_core_transaction_agg_agm;
	 * 		select 	dataprocessing_ruleid, count(*) from stage.bods_core_transaction_agg_agm group by dataprocessing_ruleid
	 * 		grant execute on procedure stage.p_build_source_core_tran_delta_agg_agm(fmthid integer) to group "g-ada-rsabible-sb-ro";
	 * 
	 * 		TODO:
	 * 			Super Accounts (ref_data.pnl_acct_agm)
	 *        06/02 : sk : added bar_amt_usd column
	 * 
	 */
BEGIN   
	
	
	DROP TABLE IF EXISTS stage_bods_core_transaction_agg_agm
	;
	CREATE TEMPORARY TABLE stage_bods_core_transaction_agg_agm
	(
		org_tranagg_agm_id 		bigint NOT NULL DEFAULT "identity"(200247, 0, '1,1'::character varying::text),
		audit_rec_src 			varchar(10) NOT NULL,
		fiscal_month_id 		int4 NOT NULL,
		bar_entity 				varchar(5) NOT NULL,
		bar_acct_category		varchar(50) NULL,
		bar_acct 				varchar(10) NOT NULL,
		shiptocust 				varchar(50) NULL,
		soldtocust 				varchar(50) NULL,
		bar_custno 				varchar(50) NULL,
		material 				varchar(50) NULL,
		bar_product 			varchar(50) NULL,
		bar_brand 				varchar(50) NULL,
		bar_amt 				numeric(38,8) NOT NULL,
		bar_amt_usd				numeric(38,8) NOT NULL,
		bar_currtype 			varchar(10) NOT NULL,
		tran_volume 			numeric(38,8) NOT NULL,
		uom 					varchar(20) NULL,
		posting_week_enddate 	date NOT NULL,
		dataprocessing_ruleid 	int4 NOT NULL,
		audit_loadts 			date NOT NULL DEFAULT getdate()
	)
	DISTSTYLE KEY
	DISTKEY (org_tranagg_agm_id)
	SORTKEY (posting_week_enddate)
	;

	/* create temp table for exchange_rate */
	drop table if exists vtbl_exchange_rate
	;
	create temporary table vtbl_exchange_rate as 
		select 	rt.fiscal_month_id, 
				rt.from_currtype,
				rt.fxrate
		from 	ref_data.hfmfxrates_current rt
		where 	lower(rt.to_currtype) = 'usd'
				AND fiscal_month_id = fmthid ;
	INSERT INTO stage_bods_core_transaction_agg_agm ( 
				audit_rec_src,
				fiscal_month_id,
				posting_week_enddate,
				bar_entity,
				bar_acct_category,
				bar_acct,
				shiptocust,
				soldtocust,
				bar_custno,
				material,
				bar_product,
				bar_brand,
				bar_currtype,
				uom,
				bar_amt,
				bar_amt_usd,
				tran_volume,
				dataprocessing_ruleid,
				audit_loadts
		)
		SELECT	src.audit_rec_src,
				src.fiscal_month_id,
				src.posting_week_enddate, 
				src.bar_entity,
				agm_acct.acct_category as bar_acct_category,
				src.bar_acct,
				src.shiptocust,
				src.soldtocust, 
				src.bar_custno,
				src.material, 
				src.bar_product,
				src.bar_brand,
				src.bar_currtype,
	   		 	lower(src.quanunit) as uom,
				sum(cast(src.bar_amt as numeric(19,6))) * -1 as bar_amt,
				sum(case 
					when rt.fxrate is not null then rt.fxrate * cast(src.bar_amt as numeric(19,6)) 
					else cast(src.bar_amt as numeric(19,6))
				end) * -1 as bar_amt_usd,
	 			sum(cast(isnull(src.quantity,0) as decimal(38,8))) as tran_volume,
				dpr_agm.data_processing_ruleid AS dataprocessing_ruleid,
				getdate() as audit_loadts
		from 	stage.core_tran_delta_agm as src
				inner join ref_data.pnl_acct_agm as agm_acct on agm_acct.bar_acct = src.bar_acct 
				inner join ref_data.data_processing_rule_agm as dpr_agm on dpr_agm.bar_acct_category = agm_acct.acct_category
				left join vtbl_exchange_rate rt on src.fiscal_month_id = rt.fiscal_month_id and 
						lower(rt.from_currtype) = lower(src.bar_currtype)
		where 	src.fiscal_month_id = fmthid
		group by src.audit_rec_src,
				src.fiscal_month_id,
				src.posting_week_enddate, 
				src.bar_entity,
				agm_acct.acct_category,
				src.bar_acct,
				src.shiptocust,
				src.soldtocust, 
				src.bar_custno,
				src.material, 
				src.bar_product,
				src.bar_brand,
				src.bar_currtype,
	   		 	lower(src.quanunit),
	   		 	dpr_agm.data_processing_ruleid
	;
	/* delete from bods_core_transaction_agg_agm */
	delete 
	from 	stage.bods_core_transaction_agg_agm
	where 	0=0 and 
			fiscal_month_id = fmthid
	;
	INSERT INTO stage.bods_core_transaction_agg_agm ( 
				audit_rec_src,
				fiscal_month_id,
				posting_week_enddate,
				bar_acct_category,
				bar_entity,
				bar_acct,
				shiptocust,
				soldtocust,
				bar_custno,
				material,
				bar_product,
				bar_brand,
				bar_currtype,
				uom,
				bar_amt,
				bar_amt_usd,
				tran_volume,
				dataprocessing_ruleid,
				audit_loadts
		)
		SELECT	stg.audit_rec_src,
				stg.fiscal_month_id,
				stg.posting_week_enddate, 
				stg.bar_acct_category,
				stg.bar_entity,
				stg.bar_acct,
				stg.shiptocust,
				stg.soldtocust, 
				stg.bar_custno,
				stg.material, 
				stg.bar_product,
				stg.bar_brand,
				stg.bar_currtype,
	   		 	stg.uom,
				stg.bar_amt,
				stg.bar_amt_usd,
	 			(case when stg.audit_rec_src = 'sap_lawson' then 1 else -1 end) * stg.tran_volume as tran_volume,
				stg.dataprocessing_ruleid,
				stg.audit_loadts
		from 	stage_bods_core_transaction_agg_agm stg
	;
exception
when others then raise info 'exception occur while ingesting data in stage.bods_core_transaction_agg_agm';
end;
$$
;

CREATE OR REPLACE PROCEDURE stage.p_build_source_core_tran_delta_c11(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN  
raise info 'delete c11 core bods transactions for fiscal month : % data from table',fmthid ;	
delete from stage.core_tran_delta where fiscal_month_id = fmthid and audit_rec_src='sap_c11';

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
	--fmthid
	--202007
	--cast('2020-08-01' as date)
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
            cast('sap_c11' as varchar(10)) as audit_rec_src ,
			cast(s.docno as varchar(10)) as document_no, 
			cast(s.docline as varchar(3)) as document_line,
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
	from 			
			bods.c11_0ec_pca3_current s
			left join ref_data.calendar dd on
			cast((case when s.postdate = '' then null else postdate end) as date) = cast(dd.dy_dte as date)
		inner join ref_data.entity rbh on s.bar_entity = rbh.name
		---only accounts thats contributes to sgm pnl structure
		inner join (select distinct bar_acct from ref_data.pnl_acct) acct on lower(s.bar_acct) = lower(acct.bar_acct) 
		cross join dates d
		where
			s.bar_acct is not null 
			and s.bar_entity is not null 
			and s.bar_acct <> ''
			and rbh.level4 = 'GTS_NA'
			---and s.bar_currtype in ('USD' ,'CAD')
			and cast((case when s.postdate = '' then null else postdate end) as date)  
			between d.fmth_begin_dte and d.fmth_end_dte 
			--and s.rec_src <> ''
			--cast('2020-06-28' as date) and cast('2020-08-01' as date)
			--between dateadd(week, -4, fiscal_week_endate) and fiscal_week_endate
) t;

exception
when others then raise exception 'exception occur while ingesting data for fiscal month : % in stage.core_tran_delta', fmthid;
end;
$$
;

CREATE OR REPLACE PROCEDURE stage.p_build_source_core_tran_delta_c11_agg(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN   
	
/* delete from bods_core_transaction_agg */
delete 
from stage.bods_core_transaction_agg
where 	0=0
AND audit_rec_src = 'sap_c11'
and fiscal_month_id = fmthid;
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
from 	bods.drm_product_current
where 	loaddts = ( select max(loaddts) from bods.drm_product_current dpc );		

drop table if exists stage_core_transaction_agg;
--	/* insert from delta into bods_core_transaction_agg */
create temporary table stage_core_transaction_agg
diststyle even 
sortkey (posting_week_enddate)
as 
with marm_volume_conversion as (
Select matnr as material, 
	  max(ea) as ea, 
	  max(mp_factor) as mp_factor, 
	  max(row_sqn) as row_sql
from (
		Select mc_max.matnr,max_row_sqn as row_sqn, 
			   case when meinh = 'EA' then umrez else null end as ea,
			   case when meinh = 'MP' then umrez else null end / case when meinh = 'MP' then umren else null end as mp_factor
		from sapc11.marm_current mc 
		inner join (
		Select matnr ,max(row_sqn) as max_row_sqn
		from sapc11.marm_current
		where meinh in ('EA', 'MP')
		--and matnr in ('64-100-A')
		group by matnr,meinh
		) mc_max on mc.row_sqn = mc_max.max_row_sqn
	)m 
group by matnr
),hfm_curreancy_rates as (
Select  distinct fyr_id as bar_year, 
			lower(SUBSTRING(fmth_name,1,3)) as fmonth_short_name, 
			fxrate,
			c.fmth_id,
			from_currtype,
			to_currtype
from ref_data.calendar c 
inner join ref_data.hfmfxrates_current cr on cast(cr.bar_year as integer) = c.fyr_id 
		and cr.bar_period = lower(SUBSTRING(fmth_name,1,3))
)
		SELECT 	audit_rec_src,
				ctdc.bar_year,
				bar_period, 
				bar_entity,
				bar_acct, 
				/* standardizing string format trimmed + lowercase */
				shiptocust AS shiptocust, 
				ctdc.soldtocust AS soldtocust, 
				ctdc.org_bar_custno AS org_bar_custno, 
				ctdc.mapped_bar_custno,
				ctdc.material AS material, 
				ctdc.org_bar_product,
				ctdc.mapped_bar_product,
				ctdc.org_bar_brand,
				ctdc.mapped_bar_brand,
				sum(cast(bar_amt as numeric(19,6)) * isnull(cast(hcr.fxrate as numeric(19,6)),1)*-1) as bar_amt, 
				isnull(lower(to_currtype),lower(bar_currtype)) as bar_currtype,
				sum(case when bar_acct in ('A40110', 'A40116','A40210', 'A40111','A40310','A40120', 'A40410','A40510', 'A40610', 'A40710', 'A40910')
						     and lower(level07_bar) = 'anf_div'
						     and lower(ctdc.quanunit) = 'ea'
                			then cast(quantity as decimal(38,8)) / (case when mp_factor is null then 1 else cast(mp_factor as decimal) end)
                			when bar_acct in ('A40110', 'A40116','A40210', 'A40111','A40310','A40120', 'A40410','A40510', 'A40610', 'A40710', 'A40910')
						     and lower(level07_bar) not in  ('anf_div')
                			then cast(quantity as decimal(38,8))
			     		else cast(0 as decimal(38,0)) 
         		 	end) as sales_volume,
  	 			sum(case when bar_acct in ('A40110', 'A40116','A40210', 'A40111','A40310','A40120', 'A40410','A40510', 'A40610', 'A40710', 'A40910')
                			then cast(quantity as decimal(38,8)) 
                			else cast(0 as decimal(38,0))
         		 	end) as tran_volume,
         		 	lower(ctdc.quanunit) as uom,
				posting_week_enddate, 
				fiscal_month_id,
				rar_o.data_processing_ruleid as org_dataprocessing_ruleid,
				rar_m.data_processing_ruleid as mapped_dataprocessing_ruleid,
				getdate() as audit_loadts
		FROM 	stage.core_tran_delta_cleansed ctdc
		LEFT JOIN ref_data.data_processing_rule rar_o  on ctdc.org_dataprocessing_hash = rar_o.dataprocessing_hash 
		LEFT JOIN ref_data.data_processing_rule rar_m on ctdc.mapped_dataprocessing_hash = rar_m.dataprocessing_hash 
		left join hfm_curreancy_rates hcr on ctdc.bar_year = hcr.bar_year and ctdc.fiscal_month_id = hcr.fmth_id
					and lower(ctdc.bar_currtype) = lower(hcr.from_currtype)
		--where audit_rec_src = data_source
		LEFT join marm_volume_conversion v on ctdc.material  = v.material 
		LEFT join bar_product_base bpb on ctdc.mapped_bar_product = bpb.bar_product 
		where ctdc.fiscal_month_id = fmthid  ---input fiscal month
		group by audit_rec_src,
				ctdc.bar_year,
				bar_period, 
				bar_entity,
				bar_acct, 
				shiptocust, 
				soldtocust, 
				org_bar_custno,
				mapped_bar_custno,
				ctdc.material, 
				org_bar_product, 
				mapped_bar_product,
				org_bar_brand,
				mapped_bar_brand,
				lower(ctdc.quanunit),
				isnull(lower(to_currtype),lower(bar_currtype)),
				hcr.fxrate,
				posting_week_enddate, 
				fiscal_month_id,
				rar_o.data_processing_ruleid,
				rar_m.data_processing_ruleid;

INSERT INTO stage.bods_core_transaction_agg
(
  audit_rec_src,
  bar_year,
  bar_period,
  bar_entity,
  bar_acct,
  shiptocust,
  soldtocust,
  org_bar_custno,
  mapped_bar_custno,
  material,
  org_bar_product,
  mapped_bar_product,
  org_bar_brand,
  mapped_bar_brand,
  bar_amt,
  bar_currtype,
  sales_volume,
  tran_volume,
  uom,
  posting_week_enddate,
  fiscal_month_id,
  org_dataprocessing_ruleid,
  mapped_dataprocessing_ruleid,
  audit_loadts
)
Select  
  audit_rec_src,
  bar_year,
  bar_period,
  bar_entity,
  bar_acct,
  shiptocust,
  soldtocust,
  org_bar_custno,
  mapped_bar_custno,
  material,
  org_bar_product,
  mapped_bar_product,
  org_bar_brand,
  mapped_bar_brand,
  bar_amt,
  bar_currtype,
  sales_volume,
  tran_volume,
  uom,
  posting_week_enddate,
  fiscal_month_id,
  org_dataprocessing_ruleid,
  mapped_dataprocessing_ruleid,
  audit_loadts
from stage_core_transaction_agg;

----compare csv vs marm 
--select material, max(mp) as mp 
--from source_poc.volume_conversion 
--where material in ('00391SD-PWR',
--					  '00397SD-PWR',
--					  '00410SD-PWR')
--group by material
--order by 1;
--
--
--
--Select matnr as material, 
--	  max(ea) as ea, 
--	  max(mp) as mp, 
--	  max(row_sqn) as row_sql
--from (
--		Select mc_max.matnr,max_row_sqn as row_sqn, 
--			   case when meinh = 'EA' then umrez else null end as ea,
--			   case when meinh = 'MP' then umrez else null end mp
--		from sapc11.marm_current mc 
--		inner join (
--		Select matnr ,max(row_sqn) as max_row_sqn
--		from sapc11.marm_current
--		where meinh in ('EA', 'MP')
--		and matnr in ('00391SD-PWR',
--					  '00397SD-PWR',
--					  '00410SD-PWR')
--		group by matnr,meinh
--		) mc_max on mc.row_sqn = mc_max.max_row_sqn
--	)m 
--group by matnr;

			
	--commit; 

exception
when others then raise info 'exception occur while ingesting data in bods_core_transaction_agg_c11';
end;
$$
;

CREATE OR REPLACE PROCEDURE stage.p_build_source_core_tran_delta_c11_agm(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN  
raise info 'delete c11 core bods transactions for fiscal month : % data from table',fmthid ;	
delete from stage.core_tran_delta_agm where fiscal_month_id = fmthid and audit_rec_src='sap_c11';

insert into stage.core_tran_delta_agm
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
	--fmthid
	--202007
	--cast('2020-08-01' as date)
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
            
            cast('sap_c11' as varchar(10)) as audit_rec_src ,
			cast(docno as varchar(10)) as document_no, 
			cast(docline as varchar(3)) as document_line,
			cast(s.year as varchar(4)) as bar_year ,
			cast(s.period as varchar(10)) as bar_period ,
			cast(s.bar_entity as varchar(5)) as bar_entity,
			cast(s.bar_acct as varchar(10)) as bar_acct,
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
			cast(bar_amt as numeric(38, 8)) as bar_amt,
			cast(quantity as decimal(38, 8)) as quantity,
			case when cast(s.quanunit as varchar(10)) = '' then null else cast(s.quanunit as varchar(10)) end as quanunit
		from 			
			bods.c11_0ec_pca3_current s
			left join ref_data.calendar dd on cast((case when s.postdate = '' then null else postdate end) as date) = cast(dd.dy_dte as date)
			inner join ref_data.entity rbh on s.bar_entity = rbh.name
			---only accounts thats contributes to sgm pnl structure
			inner join ref_data.pnl_acct_agm acct on lower(s.bar_acct) = lower(acct.bar_acct) 
		cross join dates d
		where
			s.bar_acct is not null 
			and s.bar_entity is not null 
			and s.bar_acct <> ''
			and rbh.level4 = 'GTS_NA'
			and cast((case when s.postdate = '' then null else postdate end) as date)  between d.fmth_begin_dte and d.fmth_end_dte 
			--cast('2020-06-28' as date) and cast('2020-08-01' as date)
			--between dateadd(week, -4, fiscal_week_endate) and fiscal_week_endate
) t;

exception
when others then raise exception 'exception occur while ingesting data for fiscal month : % in stage.core_tran_delta_agm', fmthid;
end;
$$
;

CREATE OR REPLACE PROCEDURE stage.p_build_source_core_tran_delta_c11_fob(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN  
raise info 'delete c11 core bods transactions for fiscal month : % data from table',fmthid ;	
delete from stage.core_tran_delta where fiscal_month_id = fmthid and audit_rec_src='ext_c11fob';

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
            
            cast('ext_c11fob' as varchar(10)) as audit_rec_src ,
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
	from bods.c11_0ec_pca3_current s
		left join ref_data.calendar dd on
			cast((case when s.postdate = '' then null else postdate end) as date) = cast(dd.dy_dte as date)
		inner join (select distinct bar_acct from ref_data.pnl_acct) acct on lower(s.bar_acct) = lower(acct.bar_acct) 
		left join sapc11.kna1_current kc 
				on lower(case when cast(shiptocust as varchar(10)) = ''  then null else cast(shiptocust as varchar(10)) end) = lower(kc.kunnr) 
		cross join dates d
		WHERE		
			s.bar_acct = 'A40111'
			and cast((case when s.postdate = '' then null else postdate end) as date)  
			--between cast('2020-06-28' as date) and cast('2020-08-01' as date)
			between d.fmth_begin_dte and d.fmth_end_dte 
			and bar_bu='GTS'
			---and kc.land1 in ('CA','US')
) t;

exception
when others then raise exception 'exception occur while ingesting data for fiscal month : % in stage.core_tran_delta for acct : A40111 - acct exception', fmthid;
end;
$$
;

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
	from bods.c11_0ec_pca3_current s
		left join ref_data.calendar dd on
			cast((case when s.postdate = '' then null else postdate end) as date) = cast(dd.dy_dte as date)
		inner join (select distinct bar_acct from ref_data.pnl_acct) acct on lower(s.bar_acct) = lower(acct.bar_acct) 
		left join sapc11.kna1_current kc 
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
        from    bods.hfm_vw_hfm_actual_trans_current as s
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
from bods.hfm_vw_hfm_actual_trans_current s 
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

CREATE OR REPLACE PROCEDURE stage.p_build_source_core_tran_delta_hfm_agm(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variablesstage.core_tran_delta
BEGIN  
raise info 'delete c11 core bods transactions for fiscal month : % data from table',fmthid ;	
delete from stage.core_tran_delta_agm where fiscal_month_id = fmthid AND audit_rec_src='hfm';

raise info 'Insert one month of c11 core bods transactions : % data into stage.core_tran_delta',fmthid;
insert into stage.core_tran_delta_agm 
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
from bods.hfm_vw_hfm_actual_trans_current s 
inner join ref_data.pnl_acct_agm acct on lower(s.bar_acct) = lower(acct.bar_acct) 
inner join ref_data.entity rbh on s.bar_entity = rbh.name
inner join (select fyr_id, lower(SUBSTRING(fmth_name,1,3)) as bar_period,
			   fmth_id as fiscal_month_id,
			   min(cast(fmth_begin_dte as date)) as fiscal_month_begin_date,
			   min(cast(fmth_end_dte as date)) as fiscal_month_end_date
		  from ref_data.calendar 
		  group by fyr_id, lower(SUBSTRING(fmth_name,1,3)), fmth_id
		  ) c on cast(s."year" as integer) = c.fyr_id and lower(s."period") = c.bar_period
where rectype = 'Actual'
and s.bar_bu = 'GTS'
and rbh.level4 = 'GTS_NA'
and fiscal_month_id=fmthid
;

exception
when others then raise exception 'exception occur while ingesting data for fiscal month : % in stage.core_tran_delta', fmthid;
end;
$$
;

CREATE OR REPLACE PROCEDURE stage.p_build_source_core_tran_delta_lawson(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN  
	
	delete from stage.core_tran_delta where fiscal_month_id = fmthid and audit_rec_src = 'sap_lawson';
	
	
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
	SELECT  DISTINCT
	 		cast('sap_lawson' as varchar(10)) as audit_rec_src ,
			cast(a.post_doc_ref_nbr as varchar(30)) as document_no, 
			cast(a.post_doc_ref_ln_nbr as varchar(30)) as document_line,
			cast(a.bar_year as varchar(4)) as bar_year ,
			cast(a.bar_period as varchar(10)) as bar_period ,
			cast(a.bar_entity as varchar(5)) as bar_entity,
			cast(a.bar_acct as varchar(6)) as bar_acct,
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
			cast(bar_amt as numeric(38, 8))bar_amt,
			cast(quantity as decimal(38, 8)) as quantity,
			null as quanunit
		from
			bods.lawson_mac_pl_trans_current a
			left join ref_data.calendar dd on cast((case when a.post_dte = '' then null else a.post_dte end) as date) = cast(dd.dy_dte as date)
			inner join ref_data.entity rbh on a.bar_entity = rbh.name
			---only accounts thats contributes to sgm pnl structure
			inner join (select distinct bar_acct from ref_data.pnl_acct) acct on lower(a.bar_acct) = lower(acct.bar_acct) 
			
			cross join (Select distinct fmth_begin_dte, fmth_end_dte,fmth_cd,fmth_id
						from ref_data.calendar c 
						where fmth_id = fmthid) dates
	
		WHERE 
			a.bar_acct is not null 
			and a.bar_entity is not null 
			and rbh.level4 = 'GTS_NA'
			--and a.bar_currtype in ('USD' ,'CAD')
			and cast((case when a.post_dte = '' then null else a.post_dte end) as date) between dates.fmth_begin_dte and dates.fmth_end_dte 
	) r;
	
END
$$
;

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
			bods.lawson_mac_pl_trans_current a
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
		from bods.p10_0ec_pca_3_trans_current a
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

CREATE OR REPLACE PROCEDURE stage.p_build_source_core_tran_delta_p10_agm(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN  
	
	
	delete from stage.core_tran_delta_agm where fiscal_month_id = fmthid and audit_rec_src = 'sap_p10';
	
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
	 		cast('sap_p10' as varchar(10)) as audit_rec_src ,
			cast(docnr as varchar(10)) as document_no, 
			cast(docln as varchar(3)) as document_line,
			cast(a.bar_year as varchar(4)) as bar_year ,
			cast(a.bar_period as varchar(10)) as bar_period ,
			cast(a.bar_entity as varchar(5)) as bar_entity,
			cast(a.bar_acct as varchar(10)) as bar_acct,
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
			cast(bar_amt as numeric(38, 8)) as bar_amt,
			cast(quantity as decimal(38, 8)) as quantity,
			case when cast(a.quanunit as varchar(10)) = '' then null else cast(a.quanunit as varchar(10)) end as quanunit
		from
			bods.p10_0ec_pca_3_trans_current a
			left join ref_data.calendar dd on cast((case when a.cpudt = '' then null else a.cpudt end) as date) = cast(dd.dy_dte as date)
			inner join ref_data.entity rbh on a.bar_entity = rbh.name
			---only accounts thats contributes to sgm pnl structure
			inner join ref_data.pnl_acct_agm acct on lower(a.bar_acct) = lower(acct.bar_acct) 
			
			cross join (Select distinct fmth_begin_dte, fmth_end_dte,fmth_cd,fyr_id
						from ref_data.calendar c 
						where fmth_id = fmthid) dates
	
		WHERE 
			a.bar_acct is not null 
			and a.bar_entity is not null 
			and rbh.level4 = 'GTS_NA'
			and cast((case when a.cpudt = '' then null else a.cpudt end) as date) between dates.fmth_begin_dte and dates.fmth_end_dte 
	) r;
	
END;
$$
;

CREATE OR REPLACE PROCEDURE stage.p_build_stage_currency_exchange_rate()
 LANGUAGE plpgsql
AS $$
BEGIN 
	truncate table stage.currency_exchange_rate;
	insert into stage.currency_exchange_rate (
			    YearMonthID,
				FromCurrencyCode,
				ToCurrencyCode,
				Rate
		)
		select 	DISTINCT 
				(CAST(rt."year" as int) * 100) +
					CASE rt."period"
						WHEN 'Jan' THEN 1
						WHEN 'Feb' THEN 2
						WHEN 'Mar' THEN 3
						WHEN 'Apr' THEN 4
						WHEN 'May' THEN 5
						WHEN 'Jun' THEN 6
						WHEN 'Jul' THEN 7
						WHEN 'Aug' THEN 8
						WHEN 'Sep' THEN 9
						WHEN 'Oct' THEN 10
						WHEN 'Nov' THEN 11
						WHEN 'Dec' THEN 12
					END as YearMonthID,
				rt.custom1 as FromCurrencyCode,
				rt.custom2 as ToCurrencyCode,
				rt.amt 
		from 	source_poc.hfmfxrates_current as rt
	;
	
exception
when others then raise info 'exception occur while ingesting data in stage.currency_exchange_rate';
end;
$$
;

CREATE OR REPLACE PROCEDURE stage.p_build_stage_rate_base(p_fmth_id integer)
 LANGUAGE plpgsql
AS $$
BEGIN
/*
 * 	TODO:
 * 		Handle Currency Conversion for non USD transactions
 * 			Use stage.currency_exchange_rate
 * 		
 */	
	
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
		from 	ref_data.calendar dt
		where 	dt.fmth_id = p_fmth_id
	;
	/* delete rate_base table for selected period */
	delete 	
	from 	stage.rate_base
	where 	0=0
		and range_start_date <= (select range_start_date from vtbl_date_range)
		and range_end_date = (select range_end_date from vtbl_date_range)
	;
	/* insert into rate_base table for selected period */
	INSERT INTO stage.rate_base (
				range_start_date, 
				range_end_date,
				Source_system,
				bar_entity,
				soldtocust, 
				bar_custno, 
				material,
				bar_product, 
				bar_currtype,
				total_bar_amt
		)
		select 	
				dt_rng.range_start_date,
				dt_rng.range_end_date,
				tran.audit_rec_src,
				tran.bar_entity,
				tran.soldtocust as soldtocust,
				tran.mapped_bar_custno as bar_custno,
				tran.material as material,
				tran.mapped_bar_product as bar_product,
				tran.bar_currtype,
				SUM(tran.bar_amt) as total_bar_amt
			
		from 	stage.bods_core_transaction_agg as tran
				inner join ref_data.data_processing_rule as dpr 
					on  dpr.data_processing_ruleid = tran.mapped_dataprocessing_ruleid 
				inner join vtbl_date_range as dt_rng
					on  dt_rng.range_start_date <= tran.posting_week_enddate and 
						dt_rng.range_end_date >= tran.posting_week_enddate
				
		where 	dpr.dataprocessing_group = 'perfect-data' and 
				dpr.soldtoflag = '1' and 
				dpr.skuflag = '1' and
				---remove mgsv sku's from allocation
				lower(tran.material) not like 'mgsv%' and 
				/* exclude transactions with zero sales amt */
				tran.bar_amt != 0 and
				/* sales invoice */
				tran.bar_acct = 'A40110'
		group by  tran.audit_rec_src,
				dt_rng.range_start_date,
				dt_rng.range_end_date,
				
				tran.bar_entity,
				tran.soldtocust,
				tran.mapped_bar_custno,
				tran.material,
				tran.mapped_bar_product,
				tran.bar_currtype
				
		/* exclude combinations with net-zero sales amt */
		having 	SUM(tran.bar_amt) != 0
	;

exception
when others then raise info 'exception occur while ingesting data in stage.rate_base';
end;
$$
;

CREATE OR REPLACE PROCEDURE stage.p_build_stage_rate_base_cogs(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN
	/*
	 * 		call stage.p_build_stage_rate_base_cogs (202101)
	 * 		select count(*) from stage.rate_base_cogs;
	 * 
	 */
	
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				max(dt.fmth_id) AS fiscal_month_id
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
/* mapping gpp portfolio to super-SBU */
	drop table if exists map_gpp_portfolio_to_supersbu
	;
	create temporary table map_gpp_portfolio_to_supersbu as 
	with
		cte_base as (
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
					case when bar_product is null then 'unknown' else bar_product end as portfolio,
					cast(generation as int) as generation,
					case when level4 is null then 'unknown' else level4 end as level04_bar,
					case when level7 is null then 'unknown' else level7 end as level07_bar
			from 	bods.drm_product_current
			where 	loaddts = ( select max(loaddts) from bods.drm_product_current dpc )
				and membertype != 'Parent'
		)
		select 	portfolio as gpp_portfolio,
				case when generation <= 4  then case when bar_product = 'Product_None' then bar_product else parent end else level04_bar end as super_sbu,
				case when generation <= 7  then case when bar_product = 'Product_None' then bar_product else parent end else level07_bar end as division
		from 	cte_base 
	;
	delete from stage.rate_base_cogs where fiscal_month_id = fmthid; 
	/* rate table based on standard cost */
	insert into stage.rate_base_cogs (
			 audit_rec_src
			 
			,fiscal_month_id
		
			,bar_entity
			,bar_currtype
			
			,soldtocust
			,shiptocust
			,bar_custno
			
			,material
			,bar_product
			,bar_brand
			
			,super_sbu
			,cost_pool
			
			,total_bar_amt
			,total_bar_amt_usd
	
	)
	
		select 	dss.source_system as audit_rec_src,
				tran.fiscal_month_id,
		
				dbu.bar_entity,
				tran.bar_currtype,
				
				dc.soldto_number as soldtocust,
				dc.shipto_number as shiptocust,
				dc.base_customer as bar_custno,
				
				dp.material,
				dp.bar_product,
				dp.product_brand as bar_brand,
				
				dp.level04_bar as super_sbu,
				case when lower(dp.level04_bar) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pools,
				
				SUM(tran.amt) as  total_bar_amt,
				SUM(tran.amt_usd) as total_bar_amt_usd
		from 	dw.fact_pnl_commercial_stacked as tran
				inner join dw.dim_customer dc on dc.customer_key = tran.customer_key 
				inner join dw.dim_product dp on dp.product_key = tran.product_key 
				inner join dw.dim_source_system dss on dss.source_system_id = tran.source_system_id 
				inner join dw.dim_business_unit dbu on dbu.business_unit_key = tran.business_unit_key 
				inner join ref_data.data_processing_rule as dpr 
					on  dpr.data_processing_ruleid = tran.mapped_dataprocessing_ruleid 
				inner join vtbl_date_range as dt_rng
					on  dt_rng.fiscal_month_id = tran.fiscal_month_id
					
		where 	dpr.dataprocessing_group = 'perfect-data' and 
				--removing mgsv sku's from allocation
				lower(dp.material) not like 'mgsv%' and 
				tran.amt_usd != 0 and
				tran.bar_acct in (
					'A60110','A60111','A60112','A60113',
					'A60114','A60115','A60116','A60210',
					'A61110','A61210','A60410','A60510',
					'A60610','A60612','A60613','A62612',
					'A62613','A62210','A60710','A60310'
				)
		group by dss.source_system,
				tran.fiscal_month_id,
		
				dbu.bar_entity,
				tran.bar_currtype,
				
				dc.soldto_number,
				dc.shipto_number,
				dc.base_customer,
				
				dp.material,
				dp.bar_product,
				dp.product_brand,
				
				dp.level04_bar,
				case when lower(dp.level04_bar) = 'ptg' then 'PTG' else 'Non-PTG' end
		/* exclude combinations with net-zero sales amt */
		having 	SUM(tran.amt_usd) != 0
	;
	
exception
when others then raise info 'exception occur while ingesting data in stage.rate_base_cogs';
end;
$$
;