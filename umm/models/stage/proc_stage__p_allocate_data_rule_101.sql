
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
			from 	{{ source('bods', 'drm_product') }}
			where 	loaddts = ( select max(loaddts) from {{ source('bods', 'drm_product') }} dpc )
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
		from 	{{ source('ref_data', 'hfmfxrates') }} rt
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
	left join {{ source('ref_data', 'hfmfxrates') }} hc on ctda.fiscal_month_id = hc.fiscal_month_id and lower(ctda.bar_currtype) = lower(hc.from_currtype)
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
from {{ source('bods', 'c11_0ec_pca3') }} s
left join ref_data.calendar dd on cast((case when s.postdate = '' then null else postdate end) as date) = cast(dd.dy_dte as date)
left join {{ source('ref_data', 'hfmfxrates') }} hc on dd.fmth_id = hc.fiscal_month_id and lower(s.currkey) = lower(hc.from_currtype)
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