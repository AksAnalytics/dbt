
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
	left join {{ source('ref_data', 'hfmfxrates') }} hc on aadr.fiscal_month_id = hc.fiscal_month_id and lower(aadr.bar_currtype) = lower(hc.from_currtype)
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