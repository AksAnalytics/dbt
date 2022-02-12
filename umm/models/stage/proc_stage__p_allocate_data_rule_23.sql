
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
		FROM 	{{ source('bods', 'drm_customer') }}
		WHERE 	0=0 
			AND loaddts = ( SELECT max(loaddts) FROM {{ source('bods', 'drm_customer') }} )
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
			from 	{{ source('bods', 'drm_product') }}
			where 	loaddts = ( select max(loaddts) from {{ source('bods', 'drm_product') }} dpc )
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
		from 	{{ source('ref_data', 'hfmfxrates') }} rt
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
		from 	{{ source('sapc11', 'kna1') }} as dmd
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
--from 	{{ source('sapc11', 'kna1') }} as dmd 
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
--from 	{{ source('sapc11', 'kna1') }} as dmd
--		right outer join cte_NonMGSV_demandgroups as dg 
--			on	lower(dg.demand_group) = lower(dmd.bran1)
--where 	dmd.bran1 is null
--;
/* DEBUG: demand group is sourced from {{ source('sapc11', 'kna1') }} 
 * 
 * the following demand groups are found in RSA bible but not source table
 * 		'HDYOW','LOWESFOB','CTC'
 */
--select 	distinct bran1
--from 	{{ source('sapc11', 'kna1') }}
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
				inner join {{ source('sapc11', 'kna1') }} as dmd
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
left join {{ source('sapc11', 'kna1') }} kc 
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
from 	{{ source('bods', 'drm_product') }}
where 	loaddts = ( select max(loaddts) from {{ source('bods', 'drm_product') }} dpc );

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