
CREATE OR REPLACE PROCEDURE dw.p_build_dim_customer(flag_reload integer)
 LANGUAGE plpgsql
AS $$
BEGIN
	/*
	 * 			call dw.p_build_dim_customer(0) -- incremental
	 * 			call dw.p_build_dim_customer(1) -- kill n fill
	 * 			select count(*) from dw.dim_customer
	 * 			select * from dw.dim_customer where customer_id like '%ADJ_ROYALTY%'
	 * 			select customer_id, count(*) from dw.dim_customer group by customer_id having count(*) > 1
	 */
	
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_customer;
	end if;

	drop table if exists tmp_customer_base
	;
	create temporary table tmp_customer_base 
	DISTSTYLE ALL
	as
		SELECT 	DISTINCT 
		      	lower(coalesce(tran.soldtocust, 'unknown')) as soldtocust,
		      	lower(coalesce(tran.shiptocust, 'unknown')) as shiptocust,
		      	lower(coalesce(tran.mapped_bar_custno, 'unknown')) as bar_custno
		FROM 	stage.bods_core_transaction_agg tran 
		where 	lower(coalesce(tran.shiptocust, 'unknown')) != 'unknown'
		UNION
		SELECT 	DISTINCT 
		      	lower(coalesce(soldtocust, 'unknown')) as soldtocust,
		      	lower(coalesce(shiptocust, 'unknown')) as shiptocust,
		      	lower(coalesce(bar_custno, 'unknown')) as bar_custno
		FROM 	ref_data.soldto_shipto_barcust_mapping 
		where 	lower(coalesce(shiptocust, 'unknown')) != 'unknown'
	;
		
	/* special members for Allocation Exception Royalty (A40910) */
	drop table if exists tmp_allocation_exception_royalty_shipto
	;
	create temporary table tmp_allocation_exception_royalty_shipto as 
		select 	distinct 
				'ADJ_ROYALTY' as soldtocust,
				'ADJ_ROYALTY' as shiptocust,
		      	lower(coalesce(bcta.mapped_bar_custno, 'unknown')) as bar_custno
		from 	stage.bods_core_transaction_agg bcta 
		where 	bcta.bar_acct = 'A40910'
	;
	/* special members for Allocation Exception CUSTOMER_NONE, PRODUCT_NONE (Rule 27) */
	drop table if exists tmp_allocation_exception_rule27_shipto
	;
	create temporary table tmp_allocation_exception_rule27_shipto as 
		select 	distinct 
				'ADJ_NO_CUST' as soldtocust,
				'ADJ_NO_CUST' as shiptocust,
		      	lower(coalesce(alloc.mapped_bar_custno, 'unknown')) as bar_custno
		from 	stage.sgm_allocated_data_rule_27 alloc
	;
	/* special members for Allocation Exception SERVICE (Rule 28) */
	drop table if exists tmp_allocation_exception_rule28_shipto
	;
	create temporary table tmp_allocation_exception_rule28_shipto as 
		select 	distinct 
				'ADJ_PSD' as soldtocust,
				'ADJ_PSD' as shiptocust,
		      	lower(coalesce(alloc.mapped_bar_custno, 'unknown')) as bar_custno
		from 	stage.sgm_allocated_data_rule_28 alloc
	;
	/* special members for Allocation Exception fob (A40111) */
	drop table if exists tmp_allocation_exception_fob_shipto
	;
	create temporary table tmp_allocation_exception_fob_shipto as 
		Select
			distinct 
			isnull(alloc_shiptocust, 'ADJ_FOB_NO_CUST') as shiptocust,
			isnull(alloc_soldtocust, 'ADJ_FOB_NO_CUST') as soldtocust,
			isnull(lower(mapped_bar_custno), 'ADJ_FOB_NO_CUST') as bar_custno
		from
			stage.sgm_allocated_data_rule_21 bcta
		where
			bcta.bar_acct = 'A40111'
	;
	/* special members for Allocation Exception fob std cos (A60111) */
	drop table if exists tmp_allocation_exception_fob_std_cos_shipto
	;
	create temporary table tmp_allocation_exception_fob_std_cos_shipto as 
		Select
			distinct 
			isnull(alloc_shiptocust, 'ADJ_FOB') as shiptocust,
			isnull(alloc_soldtocust, 'ADJ_FOB') as soldtocust,
			isnull(lower(mapped_bar_custno), 'ADJ_FOB') as bar_custno
		from
			stage.sgm_allocated_data_rule_26 bcta
		where
			bcta.bar_acct = 'A60111'
	;
	/* special members for Allocation Exception RSA (reconcile) */
	drop table if exists tmp_allocation_exception_rsa_reconcile
	;
	create temporary table tmp_allocation_exception_rsa_reconcile as 
		Select	distinct 
				'ADJ_RSA' as shiptocust,
				'ADJ_RSA' as soldtocust,
				COALESCE(lower(stg.mapped_bar_custno), 'unknown') as bar_custno
		from 	stage.sgm_allocated_data_rule_23 stg
		where	stg.source_system = 'rsa_bible' and 
				stg.dataprocessing_outcome_id = 2
	;
	/* 	create dummy records for combinations of soldTo + BarCustNo of the 
	 * 	w/ unknown shipto
	 */
	drop table if exists tmp_customer_base_unknown_shipto
	;
	create temporary table tmp_customer_base_unknown_shipto 
	DISTSTYLE ALL
	as
		SELECT 	DISTINCT 
		      	lower(coalesce(tran.soldtocust, 'unknown')) as soldtocust,
		      	lower(coalesce(tran.mapped_bar_custno, 'unknown')) as bar_custno
		FROM 	stage.bods_core_transaction_agg tran
		UNION
		SELECT 	DISTINCT 
		      	lower(coalesce(soldtocust, 'unknown')) as soldtocust,
		      	lower(coalesce(bar_custno, 'unknown')) as bar_custno
		FROM 	ref_data.soldto_shipto_barcust_mapping 
		UNION
		SELECT 	DISTINCT 
		      	lower(coalesce(stg.alloc_soldtocust, 'unknown')) as soldtocust,
		      	lower(coalesce(stg.mapped_bar_custno, 'unknown')) as bar_custno
		FROM 	stage.sgm_allocated_data_rule_23 stg
		where 	lower(coalesce(stg.alloc_shiptocust, 'unknown')) = 'unknown'
	;
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
			from 	{{ source('bods', 'drm_customer') }}
			where 	loaddts = (select max(loaddts) from {{ source('bods', 'drm_customer') }})
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
		where 	level02 != 'CSToBeRemoved'
	;
	/* Create mapping table for DemandGroup across c11, p10, lawson */
	drop table if exists tmp_demand_grp_mapping
	;
	create temporary table tmp_demand_grp_mapping
	DISTSTYLE ALL
	as
		select 	lower(c11.kunnr) as soldto_num,
				c11.bran1 AS demand_group
		from 	{{ source('sapc11', 'kna1') }} as c11
		where 	c11.kunnr is not null
	;
/* leaving P10 demand groups blank */
--	insert into tmp_demand_grp_mapping (soldto_num, demand_group)
--		select 	lower(p10.kunnr) as soldto_num,
--				p10.bran1 AS demand_group
--		from 	{{ source('sapp10', 'kna1') }} as p10
--				left outer join tmp_demand_grp_mapping as mapping
--					on 	lower(p10.kunnr) = lower(mapping.soldto_num)
--		where 	p10.kunnr is not null and
--				mapping.soldto_num is null
--	;

	/* combine customer base and bar_hierarchy */
	drop table if exists tmp_customer_core
	;
	create temporary table tmp_customer_core 
	DISTSTYLE ALL
	as
		SELECT 	COALESCE( base.soldtocust, 'unknown' ) || '|' ||
					COALESCE( base.shiptocust, 'unknown' ) || '|' ||
					COALESCE( base.bar_custno, 'unknown' ) as customer_id,
                base.soldtocust,
                base.shiptocust,
                COALESCE(hier.bar_custno, base.bar_custno) as bar_custno,
                COALESCE(hier.bar_customer_desc, 'unknown') AS bar_customer_desc,
                COALESCE(hier.bar_customer_level01, 'unknown') AS bar_customer_level01,
                COALESCE(hier.bar_customer_level02, 'unknown') AS bar_customer_level02,
                COALESCE(hier.bar_customer_level03, 'unknown') AS bar_customer_level03,
                COALESCE(hier.bar_customer_level04, 'unknown') AS bar_customer_level04,
                COALESCE(hier.bar_customer_level05, 'unknown') AS bar_customer_level05,
                COALESCE(hier.bar_customer_level06, 'unknown') AS bar_customer_level06,
                COALESCE(hier.bar_customer_level07, 'unknown') AS bar_customer_level07,
                COALESCE(hier.bar_customer_level08, 'unknown') AS bar_customer_level08,
                COALESCE(hier.bar_customer_level09, 'unknown') AS bar_customer_level09,
                COALESCE(hier.bar_customer_level10, 'unknown') AS bar_customer_level10,
                COALESCE(hier.bar_customer_level11, 'unknown') AS bar_customer_level11,
                hier.membertype,
                hier.generation,
                COALESCE(hier.ragged_level01, 'unknown') AS ragged_level01,
                COALESCE(hier.ragged_level02, 'unknown') AS ragged_level02,
                COALESCE(hier.ragged_level03, 'unknown') AS ragged_level03,
                COALESCE(hier.ragged_level04, 'unknown') AS ragged_level04,
                COALESCE(hier.ragged_level05, 'unknown') AS ragged_level05,
                COALESCE(hier.ragged_level06, 'unknown') AS ragged_level06,
                COALESCE(hier.ragged_level07, 'unknown') AS ragged_level07,
                COALESCE(hier.ragged_level08, 'unknown') AS ragged_level08,
                COALESCE(hier.ragged_level09, 'unknown') AS ragged_level09,
                COALESCE(hier.ragged_level10, 'unknown') AS ragged_level10,
                COALESCE(hier.ragged_level11, 'unknown') AS ragged_level11,
                demand.demand_group
		FROM	tmp_customer_base base
                left outer join tmp_customer_bar_hierarchy as hier 
                	ON 	lower(hier.bar_custno) = lower(base.bar_custno)
            	left outer join tmp_demand_grp_mapping demand 
            		on 	lower(demand.soldto_num) = lower(base.soldtocust)
        WHERE 	COALESCE( base.shiptocust, 'unknown' ) != 'unknown'
	;
	drop table if exists tmp_customer_core_unknown_shipto
	;
	create temporary table tmp_customer_core_unknown_shipto
	DISTSTYLE ALL
	as
		SELECT 	COALESCE( base.soldtocust, 'unknown' ) || 
					'|unknown|' ||
					COALESCE( base.bar_custno, 'unknown' ) as customer_id,
                base.soldtocust,
                'unknown' as shiptocust,
                COALESCE(hier.bar_custno, base.bar_custno) as bar_custno,
                COALESCE(hier.bar_customer_desc, 'unknown') AS bar_customer_desc,
                COALESCE(hier.bar_customer_level01, 'unknown') AS bar_customer_level01,
                COALESCE(hier.bar_customer_level02, 'unknown') AS bar_customer_level02,
                COALESCE(hier.bar_customer_level03, 'unknown') AS bar_customer_level03,
                COALESCE(hier.bar_customer_level04, 'unknown') AS bar_customer_level04,
                COALESCE(hier.bar_customer_level05, 'unknown') AS bar_customer_level05,
                COALESCE(hier.bar_customer_level06, 'unknown') AS bar_customer_level06,
                COALESCE(hier.bar_customer_level07, 'unknown') AS bar_customer_level07,
                COALESCE(hier.bar_customer_level08, 'unknown') AS bar_customer_level08,
                COALESCE(hier.bar_customer_level09, 'unknown') AS bar_customer_level09,
                COALESCE(hier.bar_customer_level10, 'unknown') AS bar_customer_level10,
                COALESCE(hier.bar_customer_level11, 'unknown') AS bar_customer_level11,
                hier.membertype,
                hier.generation,
                COALESCE(hier.ragged_level01, 'unknown') AS ragged_level01,
                COALESCE(hier.ragged_level02, 'unknown') AS ragged_level02,
                COALESCE(hier.ragged_level03, 'unknown') AS ragged_level03,
                COALESCE(hier.ragged_level04, 'unknown') AS ragged_level04,
                COALESCE(hier.ragged_level05, 'unknown') AS ragged_level05,
                COALESCE(hier.ragged_level06, 'unknown') AS ragged_level06,
                COALESCE(hier.ragged_level07, 'unknown') AS ragged_level07,
                COALESCE(hier.ragged_level08, 'unknown') AS ragged_level08,
                COALESCE(hier.ragged_level09, 'unknown') AS ragged_level09,
                COALESCE(hier.ragged_level10, 'unknown') AS ragged_level10,
                COALESCE(hier.ragged_level11, 'unknown') AS ragged_level11,
                demand.demand_group
		FROM	tmp_customer_base_unknown_shipto base
                left outer join tmp_customer_bar_hierarchy as hier 
                	ON 	lower(hier.bar_custno) = lower(base.bar_custno)
            	left outer join tmp_demand_grp_mapping demand 
            		on 	lower(demand.soldto_num) = lower(base.soldtocust)
	;
	drop table if exists tmp_customer_core_account_exception_royalty
	;
	create temporary table tmp_customer_core_account_exception_royalty
	DISTSTYLE ALL
	as
		SELECT 	base.soldtocust || '|' || base.shiptocust || '|' || base.bar_custno as customer_id,
                base.soldtocust,
                base.shiptocust,
                COALESCE(hier.bar_custno, base.bar_custno) as bar_custno,
                COALESCE(hier.bar_customer_desc, 'unknown') AS bar_customer_desc,
                COALESCE(hier.bar_customer_level01, 'unknown') AS bar_customer_level01,
                COALESCE(hier.bar_customer_level02, 'unknown') AS bar_customer_level02,
                COALESCE(hier.bar_customer_level03, 'unknown') AS bar_customer_level03,
                COALESCE(hier.bar_customer_level04, 'unknown') AS bar_customer_level04,
                COALESCE(hier.bar_customer_level05, 'unknown') AS bar_customer_level05,
                COALESCE(hier.bar_customer_level06, 'unknown') AS bar_customer_level06,
                COALESCE(hier.bar_customer_level07, 'unknown') AS bar_customer_level07,
                COALESCE(hier.bar_customer_level08, 'unknown') AS bar_customer_level08,
                COALESCE(hier.bar_customer_level09, 'unknown') AS bar_customer_level09,
                COALESCE(hier.bar_customer_level10, 'unknown') AS bar_customer_level10,
                COALESCE(hier.bar_customer_level11, 'unknown') AS bar_customer_level11,
                hier.membertype,
                hier.generation,
                '' AS ragged_level01,
                '' AS ragged_level02,
                '' AS ragged_level03,
                '' AS ragged_level04,
                '' AS ragged_level05,
                '' AS ragged_level06,
                '' AS ragged_level07,
                '' AS ragged_level08,
                '' AS ragged_level09,
                '' AS ragged_level10,
                '' AS ragged_level11,
                '' AS demand_group
		FROM	tmp_allocation_exception_royalty_shipto base
                left outer join tmp_customer_bar_hierarchy as hier 
                	ON 	lower(hier.bar_custno) = lower(base.bar_custno)
	;
	drop table if exists tmp_customer_core_account_exception_rule27
	;
	create temporary table tmp_customer_core_account_exception_rule27
	DISTSTYLE ALL
	as
		SELECT 	base.soldtocust || '|' || base.shiptocust || '|' || base.bar_custno as customer_id,
                base.soldtocust,
                base.shiptocust,
                COALESCE(hier.bar_custno, base.bar_custno) as bar_custno,
                COALESCE(hier.bar_customer_desc, 'unknown') AS bar_customer_desc,
                COALESCE(hier.bar_customer_level01, 'unknown') AS bar_customer_level01,
                COALESCE(hier.bar_customer_level02, 'unknown') AS bar_customer_level02,
                COALESCE(hier.bar_customer_level03, 'unknown') AS bar_customer_level03,
                COALESCE(hier.bar_customer_level04, 'unknown') AS bar_customer_level04,
                COALESCE(hier.bar_customer_level05, 'unknown') AS bar_customer_level05,
                COALESCE(hier.bar_customer_level06, 'unknown') AS bar_customer_level06,
                COALESCE(hier.bar_customer_level07, 'unknown') AS bar_customer_level07,
                COALESCE(hier.bar_customer_level08, 'unknown') AS bar_customer_level08,
                COALESCE(hier.bar_customer_level09, 'unknown') AS bar_customer_level09,
                COALESCE(hier.bar_customer_level10, 'unknown') AS bar_customer_level10,
                COALESCE(hier.bar_customer_level11, 'unknown') AS bar_customer_level11,
                hier.membertype,
                hier.generation,
                '' AS ragged_level01,
                '' AS ragged_level02,
                '' AS ragged_level03,
                '' AS ragged_level04,
                '' AS ragged_level05,
                '' AS ragged_level06,
                '' AS ragged_level07,
                '' AS ragged_level08,
                '' AS ragged_level09,
                '' AS ragged_level10,
                '' AS ragged_level11,
                '' AS demand_group
		FROM	tmp_allocation_exception_rule27_shipto base
                left outer join tmp_customer_bar_hierarchy as hier 
                	ON 	lower(hier.bar_custno) = lower(base.bar_custno)
	;
	drop table if exists tmp_customer_core_account_exception_rule28
	;
	create temporary table tmp_customer_core_account_exception_rule28
	DISTSTYLE ALL
	as
		SELECT 	base.soldtocust || '|' || base.shiptocust || '|' || base.bar_custno as customer_id,
                base.soldtocust,
                base.shiptocust,
                COALESCE(hier.bar_custno, base.bar_custno) as bar_custno,
                COALESCE(hier.bar_customer_desc, 'unknown') AS bar_customer_desc,
                COALESCE(hier.bar_customer_level01, 'unknown') AS bar_customer_level01,
                COALESCE(hier.bar_customer_level02, 'unknown') AS bar_customer_level02,
                COALESCE(hier.bar_customer_level03, 'unknown') AS bar_customer_level03,
                COALESCE(hier.bar_customer_level04, 'unknown') AS bar_customer_level04,
                COALESCE(hier.bar_customer_level05, 'unknown') AS bar_customer_level05,
                COALESCE(hier.bar_customer_level06, 'unknown') AS bar_customer_level06,
                COALESCE(hier.bar_customer_level07, 'unknown') AS bar_customer_level07,
                COALESCE(hier.bar_customer_level08, 'unknown') AS bar_customer_level08,
                COALESCE(hier.bar_customer_level09, 'unknown') AS bar_customer_level09,
                COALESCE(hier.bar_customer_level10, 'unknown') AS bar_customer_level10,
                COALESCE(hier.bar_customer_level11, 'unknown') AS bar_customer_level11,
                hier.membertype,
                hier.generation,
                '' AS ragged_level01,
                '' AS ragged_level02,
                '' AS ragged_level03,
                '' AS ragged_level04,
                '' AS ragged_level05,
                '' AS ragged_level06,
                '' AS ragged_level07,
                '' AS ragged_level08,
                '' AS ragged_level09,
                '' AS ragged_level10,
                '' AS ragged_level11,
                '' AS demand_group
		FROM	tmp_allocation_exception_rule28_shipto base
                left outer join tmp_customer_bar_hierarchy as hier 
                	ON 	lower(hier.bar_custno) = lower(base.bar_custno)
	;
	drop table if exists tmp_customer_core_account_exception_fob
	;
	create temporary table tmp_customer_core_account_exception_fob
	DISTSTYLE ALL
	as
		SELECT 	base.soldtocust || '|' || base.shiptocust || '|' || base.bar_custno as customer_id,
                base.soldtocust,
                base.shiptocust,
                COALESCE(hier.bar_custno, base.bar_custno) as bar_custno,
                COALESCE(hier.bar_customer_desc, 'unknown') AS bar_customer_desc,
                COALESCE(hier.bar_customer_level01, 'unknown') AS bar_customer_level01,
                COALESCE(hier.bar_customer_level02, 'unknown') AS bar_customer_level02,
                COALESCE(hier.bar_customer_level03, 'unknown') AS bar_customer_level03,
                COALESCE(hier.bar_customer_level04, 'unknown') AS bar_customer_level04,
                COALESCE(hier.bar_customer_level05, 'unknown') AS bar_customer_level05,
                COALESCE(hier.bar_customer_level06, 'unknown') AS bar_customer_level06,
                COALESCE(hier.bar_customer_level07, 'unknown') AS bar_customer_level07,
                COALESCE(hier.bar_customer_level08, 'unknown') AS bar_customer_level08,
                COALESCE(hier.bar_customer_level09, 'unknown') AS bar_customer_level09,
                COALESCE(hier.bar_customer_level10, 'unknown') AS bar_customer_level10,
                COALESCE(hier.bar_customer_level11, 'unknown') AS bar_customer_level11,
                hier.membertype,
                hier.generation,
                '' AS ragged_level01,
                '' AS ragged_level02,
                '' AS ragged_level03,
                '' AS ragged_level04,
                '' AS ragged_level05,
                '' AS ragged_level06,
                '' AS ragged_level07,
                '' AS ragged_level08,
                '' AS ragged_level09,
                '' AS ragged_level10,
                '' AS ragged_level11,
                '' AS demand_group
		FROM	tmp_allocation_exception_fob_shipto base
                left outer join tmp_customer_bar_hierarchy as hier 
                	ON 	lower(hier.bar_custno) = lower(base.bar_custno)
	;
	drop table if exists tmp_customer_core_account_exception_fob_std_cos
	;
	create temporary table tmp_customer_core_account_exception_fob_std_cos
	DISTSTYLE ALL
	as
		SELECT 	base.soldtocust || '|' || base.shiptocust || '|' || base.bar_custno as customer_id,
                base.soldtocust,
                base.shiptocust,
                COALESCE(hier.bar_custno, base.bar_custno) as bar_custno,
                COALESCE(hier.bar_customer_desc, 'unknown') AS bar_customer_desc,
                COALESCE(hier.bar_customer_level01, 'unknown') AS bar_customer_level01,
                COALESCE(hier.bar_customer_level02, 'unknown') AS bar_customer_level02,
                COALESCE(hier.bar_customer_level03, 'unknown') AS bar_customer_level03,
                COALESCE(hier.bar_customer_level04, 'unknown') AS bar_customer_level04,
                COALESCE(hier.bar_customer_level05, 'unknown') AS bar_customer_level05,
                COALESCE(hier.bar_customer_level06, 'unknown') AS bar_customer_level06,
                COALESCE(hier.bar_customer_level07, 'unknown') AS bar_customer_level07,
                COALESCE(hier.bar_customer_level08, 'unknown') AS bar_customer_level08,
                COALESCE(hier.bar_customer_level09, 'unknown') AS bar_customer_level09,
                COALESCE(hier.bar_customer_level10, 'unknown') AS bar_customer_level10,
                COALESCE(hier.bar_customer_level11, 'unknown') AS bar_customer_level11,
                hier.membertype,
                hier.generation,
                '' AS ragged_level01,
                '' AS ragged_level02,
                '' AS ragged_level03,
                '' AS ragged_level04,
                '' AS ragged_level05,
                '' AS ragged_level06,
                '' AS ragged_level07,
                '' AS ragged_level08,
                '' AS ragged_level09,
                '' AS ragged_level10,
                '' AS ragged_level11,
                '' AS demand_group
		FROM	tmp_allocation_exception_fob_std_cos_shipto base
                left outer join tmp_customer_bar_hierarchy as hier 
                	ON 	lower(hier.bar_custno) = lower(base.bar_custno)
	;
	drop table if exists tmp_customer_core_account_exception_rsa_reconcile
	;
	create temporary table tmp_customer_core_account_exception_rsa_reconcile
	DISTSTYLE ALL
	as
		SELECT 	base.soldtocust || '|' || base.shiptocust || '|' || base.bar_custno as customer_id,
                base.soldtocust,
                base.shiptocust,
                COALESCE(hier.bar_custno, base.bar_custno) as bar_custno,
                COALESCE(hier.bar_customer_desc, 'unknown') AS bar_customer_desc,
                COALESCE(hier.bar_customer_level01, 'unknown') AS bar_customer_level01,
                COALESCE(hier.bar_customer_level02, 'unknown') AS bar_customer_level02,
                COALESCE(hier.bar_customer_level03, 'unknown') AS bar_customer_level03,
                COALESCE(hier.bar_customer_level04, 'unknown') AS bar_customer_level04,
                COALESCE(hier.bar_customer_level05, 'unknown') AS bar_customer_level05,
                COALESCE(hier.bar_customer_level06, 'unknown') AS bar_customer_level06,
                COALESCE(hier.bar_customer_level07, 'unknown') AS bar_customer_level07,
                COALESCE(hier.bar_customer_level08, 'unknown') AS bar_customer_level08,
                COALESCE(hier.bar_customer_level09, 'unknown') AS bar_customer_level09,
                COALESCE(hier.bar_customer_level10, 'unknown') AS bar_customer_level10,
                COALESCE(hier.bar_customer_level11, 'unknown') AS bar_customer_level11,
                hier.membertype,
                hier.generation,
                '' AS ragged_level01,
                '' AS ragged_level02,
                '' AS ragged_level03,
                '' AS ragged_level04,
                '' AS ragged_level05,
                '' AS ragged_level06,
                '' AS ragged_level07,
                '' AS ragged_level08,
                '' AS ragged_level09,
                '' AS ragged_level10,
                '' AS ragged_level11,
                '' AS demand_group
		FROM	tmp_allocation_exception_rsa_reconcile base
                left outer join tmp_customer_bar_hierarchy as hier 
                	ON 	lower(hier.bar_custno) = lower(base.bar_custno)
	;


/*
	drop table if exists tmp_A1_hierarchy
	;
	create temporary table tmp_A1_hierarchy
	DISTSTYLE ALL
	as
		select 	HA1_id.soldto,
				HA1_id.hierarchy_a1_id,
				HA1_desc.hierarchy_a1_desc
		from 	(
					select 	distinct 
							knvh.kunnr as soldto,
							knvh.hkunnr as hierarchy_a1_id,
							row_number() over (
								partition by knvh.kunnr
								order by knvh.datab desc 
							) as rnk
					from 	{{ source('sapc11', 'knvh') }} knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA1_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a1_id,
							kna1.name1 as hierarchy_a1_desc
					from 	{{ source('sapc11', 'kna1') }} kna1
				) HA1_desc
					on 	HA1_desc.hierarchy_a1_id = HA1_id.hierarchy_a1_id
		where	HA1_id.rnk = 1
	;
	insert into tmp_A1_hierarchy (soldto, hierarchy_a1_id, hierarchy_a1_desc)
		select 	HA1_id.soldto,
				HA1_id.hierarchy_a1_id,
				HA1_desc.hierarchy_a1_desc
		from 	(
					select 	distinct 
							knvh.kunnr as soldto,
							knvh.hkunnr as hierarchy_a1_id,
							row_number() over (
								partition by knvh.kunnr
								order by knvh.datab desc 
							) as rnk
					from 	{{ source('sapp10', 'knvh') }} knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA1_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a1_id,
							kna1.name1 as hierarchy_a1_desc
					from 	{{ source('sapp10', 'kna1') }} kna1
				) HA1_desc
					on 	HA1_desc.hierarchy_a1_id = HA1_id.hierarchy_a1_id
				left outer join tmp_A1_hierarchy mapping
					on 	lower(mapping.soldto) = lower(HA1_id.soldto)
		where	HA1_id.rnk = 1 and 
				mapping.soldto is null
	;
	drop table if exists tmp_A2_hierarchy
	;
	create temporary table tmp_A2_hierarchy
	DISTSTYLE ALL
	as
		select 	HA2_id.soldto,
				HA2_id.hierarchy_a2_id,
				HA2_desc.hierarchy_a2_desc
		from 	(
					select 	distinct 
							knvh.kunnr as soldto,
							knvh.hkunnr as hierarchy_a2_id,
							row_number() over (
								partition by knvh.kunnr
								order by knvh.datab desc 
							) as rnk
					from 	{{ source('sapc11', 'knvh') }} knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA2_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a2_id,
							kna1.name1 as hierarchy_a2_desc
					from 	{{ source('sapc11', 'kna1') }} kna1
				) HA2_desc
					on 	HA2_desc.hierarchy_a2_id = HA2_id.hierarchy_a2_id
		where	HA2_id.rnk = 1
	;
	insert into tmp_A2_hierarchy (soldto, hierarchy_a2_id, hierarchy_a2_desc)
		select 	HA2_id.soldto,
				HA2_id.hierarchy_a2_id,
				HA2_desc.hierarchy_a2_desc
		from 	(
					select 	distinct 
							knvh.kunnr as soldto,
							knvh.kunnr as hierarchy_a2_id,
							row_number() over (
								partition by knvh.kunnr
								order by knvh.datab desc 
							) as rnk
					from 	{{ source('sapp10', 'knvh') }} knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA2_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a2_id,
							kna1.name1 as hierarchy_a2_desc
					from 	{{ source('sapp10', 'kna1') }} kna1
				) HA2_desc
					on 	HA2_desc.hierarchy_a2_id = HA2_id.hierarchy_a2_id
				left outer join tmp_A2_hierarchy mapping
					on 	lower(mapping.soldto) = lower(HA2_id.soldto)
		where	HA2_id.rnk = 1 and
			 	mapping.soldto is null
	;
	
	*/
/* 	temp table for A1/A2 hierarchies */
drop table if exists _H2_to_H1;
create table _H2_to_H1 
as (
select 	HA2_id.hierarchy_a2_id,
				HA2_id.hierarchy_a1_id,
				HA2_desc.hierarchy_a1_desc
		from 	(
					select 	distinct 
							knvh.kunnr as hierarchy_a2_id,
							knvh.hkunnr as hierarchy_a1_id,
							row_number() over (
								partition by knvh.kunnr
								order by knvh.datab desc 
							) as rnk
					from 	{{ source('sapc11', 'knvh') }} knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA2_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a1_id,
							kna1.name1 as hierarchy_a1_desc
					from 	{{ source('sapc11', 'kna1') }} kna1
				) HA2_desc
					on 	HA2_desc.hierarchy_a1_id = HA2_id.hierarchy_a1_id
		where	HA2_id.rnk = 1
		--and soldto = '0001710948'
		and hierarchy_a2_id like '00080%'
);
insert into _H2_to_H1 (hierarchy_a2_id, hierarchy_a1_id, hierarchy_a1_desc)
		select 	HA2_id.hierarchy_a2_id ,
				HA2_id.hierarchy_a1_id,
				HA2_desc.hierarchy_a1_desc
		from 	(
					select 	distinct 
							knvh.kunnr as hierarchy_a2_id,
							knvh.hkunnr as hierarchy_a1_id,
							row_number() over (
								partition by knvh.kunnr
								order by knvh.datab desc 
							) as rnk
					from 	{{ source('sapp10', 'knvh') }} knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA2_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a1_id,
							kna1.name1 as hierarchy_a1_desc
					from 	{{ source('sapp10', 'kna1') }} kna1
				) HA2_desc
					on 	HA2_desc.hierarchy_a1_id = HA2_id.hierarchy_a1_id
				left outer join _H2_to_H1 mapping
					on 	lower(mapping.hierarchy_a2_id) = lower(HA2_id.hierarchy_a2_id)
		where	HA2_id.rnk = 1  
				and HA2_id.hierarchy_a2_id like '00080%'
				and mapping.hierarchy_a2_id is null;
			
			
--select * from _H2
drop table if exists _soldto_h2;
create temporary table _soldto_h2 
as
(
	select 	HA1_id.soldto,
			HA1_id.hierarchy_a2_id,
			HA1_desc.hierarchy_a2_desc
		from 	(
					select 	distinct 
							knvh.kunnr as soldto,
							knvh.hkunnr as hierarchy_a2_id,
							row_number() over (
								partition by knvh.kunnr
								order by knvh.datab desc 
							) as rnk
					from 	{{ source('sapc11', 'knvh') }} knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA1_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a2_id,
							kna1.name1 as hierarchy_a2_desc
					from 	{{ source('sapc11', 'kna1') }} kna1
				) HA1_desc
					on 	HA1_desc.hierarchy_a2_id = HA1_id.hierarchy_a2_id
			
		where	HA1_id.rnk = 1
);
insert into _soldto_h2 (soldto,hierarchy_a2_id,hierarchy_a2_desc  )
select 	HA1_id.soldto,
			HA1_id.hierarchy_a2_id,
			HA1_desc.hierarchy_a2_desc
		from 	(
					select 	distinct 
							knvh.kunnr as soldto,
							knvh.hkunnr as hierarchy_a2_id,
							row_number() over (
								partition by knvh.kunnr
								order by knvh.datab desc 
							) as rnk
					from 	{{ source('sapp10', 'knvh') }} knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA1_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a2_id,
							kna1.name1 as hierarchy_a2_desc
					from 	{{ source('sapp10', 'kna1') }} kna1
				) HA1_desc
					on 	HA1_desc.hierarchy_a2_id = HA1_id.hierarchy_a2_id
			left join _soldto_h2 on HA1_id.soldto = _soldto_h2.soldto
		where	HA1_id.rnk = 1
		and _soldto_h2.soldto is null
;
drop table if exists tmp_A_hierarchy;
create temporary table tmp_A_hierarchy 
as (
select _soldto_h2.soldto, _soldto_h2.hierarchy_a2_id, _soldto_h2.hierarchy_a2_desc, _H2_to_H1.hierarchy_a1_id, _H2_to_H1.hierarchy_a1_desc
from _soldto_h2 
left join _H2_to_H1 on _soldto_h2.hierarchy_a2_id = _H2_to_H1.hierarchy_a2_id
);


	/* 	temp table for B & C hierarchies */
	drop table if exists tmp_B_hierarchy
	;
	create temporary table tmp_B_hierarchy
	DISTSTYLE ALL
	as
		select 	HB_id.shipto,
				HB_id.hierarchy_b_id,
				HB_desc.hierarchy_b_desc
		from 	(
					select 	distinct 
							knvh.kunnr as shipto,
							knvh.hkunnr as hierarchy_b_id
					from 	{{ source('sapc11', 'knvh') }} knvh
					where 	knvh.HITYP='B' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HB_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_b_id,
							kna1.name1 as hierarchy_b_desc
					from 	{{ source('sapc11', 'kna1') }} kna1
				) HB_desc
					on 	HB_desc.hierarchy_b_id = HB_id.hierarchy_b_id
	;
	insert into tmp_B_hierarchy (shipto, hierarchy_b_id, hierarchy_b_desc)
		select 	HB_id.shipto,
				HB_id.hierarchy_b_id,
				HB_desc.hierarchy_b_desc
		from 	(
					select 	distinct 
							knvh.kunnr as shipto,
							knvh.hkunnr as hierarchy_b_id
					from 	{{ source('sapp10', 'knvh') }} knvh
					where 	knvh.HITYP='B' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HB_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_b_id,
							kna1.name1 as hierarchy_b_desc
					from 	{{ source('sapp10', 'kna1') }} kna1
				) HB_desc
					on 	HB_desc.hierarchy_b_id = HB_id.hierarchy_b_id
				left outer join tmp_B_hierarchy mapping
					on 	lower(mapping.shipto) = lower(HB_id.shipto)
		where 	mapping.shipto is null
	;

	drop table if exists tmp_C_hierarchy
	;
	create temporary table tmp_C_hierarchy
	DISTSTYLE ALL
	as
		select 	HC_id.soldto,
				HC_id.hierarchy_c_id,
				HC_desc.hierarchy_c_desc
		from 	(
					select 	distinct 
							knvh.kunnr as soldto,
							knvh.hkunnr as hierarchy_c_id,
							row_number() over (
								partition by knvh.kunnr
								order by knvh.datab desc 
							) as rnk
					from 	{{ source('sapc11', 'knvh') }} knvh
					where 	knvh.HITYP='C' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HC_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_c_id,
							kna1.name1 as hierarchy_c_desc
					from 	{{ source('sapc11', 'kna1') }} kna1
				) HC_desc
					on 	HC_desc.hierarchy_c_id = HC_id.hierarchy_c_id
		where	HC_id.rnk = 1
	;
	insert into tmp_C_hierarchy (soldto, hierarchy_c_id, hierarchy_c_desc)
		select 	HC_id.soldto,
				HC_id.hierarchy_c_id,
				HC_desc.hierarchy_c_desc
		from 	(
					select 	distinct 
							knvh.kunnr as soldto,
							knvh.hkunnr as hierarchy_c_id
					from 	{{ source('sapp10', 'knvh') }} knvh
					where 	knvh.HITYP='C' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HC_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_c_id,
							kna1.name1 as hierarchy_c_desc
					from 	{{ source('sapp10', 'kna1') }} kna1
				) HC_desc
					on 	HC_desc.hierarchy_c_id = HC_id.hierarchy_c_id
				left outer join tmp_C_hierarchy mapping
					on 	lower(mapping.soldto) = lower(HC_id.soldto)
		where 	mapping.soldto is null
	;
		
	/* Create mapping table for SoldTo Name across c11, p10, lawson */
	drop table if exists tmp_soldto_name_mapping
	;
	create temporary table tmp_soldto_name_mapping
	DISTSTYLE ALL
	as
		select 	lower(c11.kunnr) as soldto_num,
				c11.name1 as soldto_name
		from 	{{ source('bods', 'c11_0customer_attr') }} as c11
		where 	c11.kunnr is not null
	;
	insert into tmp_soldto_name_mapping (soldto_num, soldto_name)
		select 	lower(p10.kunnr) as soldto_num,
				p10.name1 as soldto_name
		from 	{{ source('sapp10', 'kna1') }} as p10
				left outer join tmp_soldto_name_mapping as mapping
					on 	lower(p10.kunnr) = lower(mapping.soldto_num)
		where 	p10.kunnr is not null and
				mapping.soldto_num is null
	;
--	CONTAINS DUPLICATE
	insert into tmp_soldto_name_mapping (soldto_num, soldto_name)
		select 	lower(lawson.cust_nbr) as soldto_num,
				lawson.cust_name as soldto_name
		from 	{{ source('bods', 'extr_lawson_mac_cust') }} as lawson
				left outer join tmp_soldto_name_mapping as mapping
					on 	lower(lawson.cust_nbr) = lower(mapping.soldto_num)
		where 	lawson.cust_nbr is not null and
				mapping.soldto_num is null and 
				lawson.div_cd = 'USM'
	;
	/* ------------------------------------------------------------------ 
	 * 	Part 02: Create stage table to build from scratch
	 * ------------------------------------------------------------------
	 */
	drop table if exists stage_dim_customer
	;
	create temporary table stage_dim_customer (
		customer_id varchar(200) NULL,
		
		soldto varchar(50) NULL,
		shipto varchar(50) NULL,
		bar_customer varchar(50) NULL,
		
		bar_customer_desc varchar(100) NULL,
		bar_customer_level01 varchar(100) NULL,
		bar_customer_level02 varchar(100) NULL,
		bar_customer_level03 varchar(100) NULL,
		bar_customer_level04 varchar(100) NULL,
		bar_customer_level05 varchar(100) NULL,
		bar_customer_level06 varchar(100) NULL,
		bar_customer_level07 varchar(100) NULL,
		bar_customer_level08 varchar(100) NULL,
		bar_customer_level09 varchar(100) NULL,
		bar_customer_level10 varchar(100) NULL,
		bar_customer_level11 varchar(100) NULL,
		membertype varchar(10) NULL,
		generation int4 NULL,
		ragged_level01 varchar(100) NULL,
		ragged_level02 varchar(100) NULL,
		ragged_level03 varchar(100) NULL,
		ragged_level04 varchar(100) NULL,
		ragged_level05 varchar(100) NULL,
		ragged_level06 varchar(100) NULL,
		ragged_level07 varchar(100) NULL,
		ragged_level08 varchar(100) NULL,
		ragged_level09 varchar(100) NULL,
		ragged_level10 varchar(100) NULL,
		ragged_level11 varchar(100) NULL,
		demand_group varchar(100) NULL,
		a2 varchar(100) NULL,
		a1 varchar(100) NULL,
		a2_description varchar(100) NULL,
		a1_description varchar(100) NULL,
		soldto_name varchar(100) NULL,
		shipto_name varchar(100) NULL,
		level01_commercial varchar(100) NULL,
		level02_commercial varchar(100) NULL,
		level03_commercial varchar(100) NULL,
		level04_commercial varchar(100) NULL,
		level05_commercial varchar(100) NULL,
		level06_commercial varchar(100) NULL,
		hierarchy_b_id		varchar(100) NULL,
		hierarchy_b_desc	varchar(100) NULL,
		hierarchy_c_id		varchar(100) NULL,
		hierarchy_c_desc	varchar(100) NULL
	) DISTSTYLE ALL 
	;
	insert into stage_dim_customer (
				customer_id,
				
				soldto,
				shipto,
				bar_customer,
				
				bar_customer_desc,
		        bar_customer_level01,
		        bar_customer_level02,
		        bar_customer_level03,
		        bar_customer_level04,
		        bar_customer_level05,
		        bar_customer_level06,
		        bar_customer_level07,
		        bar_customer_level08,
		        bar_customer_level09,
		        bar_customer_level10,
		        bar_customer_level11,
		        membertype,
		        generation,
		        ragged_level01,
		        ragged_level02,
		        ragged_level03,
		        ragged_level04,
		        ragged_level05,
		        ragged_level06,
		        ragged_level07,
		        ragged_level08,
		        ragged_level09,
		        ragged_level10,
		        ragged_level11,
				demand_group,
				A2,
				A1,
				a2_description,
				a1_description,
				soldto_name,
				shipto_name,
				level01_commercial,
				level02_commercial,
				level03_commercial,
				level04_commercial,
				level05_commercial,
				level06_commercial,
				hierarchy_b_id,
				hierarchy_b_desc,
				hierarchy_c_id,
				hierarchy_c_desc
		)
		SELECT	cust.customer_id,
				
				cust.soldtocust,
				cust.shiptocust,
				cust.bar_custno as bar_customer,
				
				cust.bar_customer_desc,
		        cust.bar_customer_level01,
		        cust.bar_customer_level02,
		        cust.bar_customer_level03,
		        cust.bar_customer_level04,
		        cust.bar_customer_level05,
		        cust.bar_customer_level06,
		        cust.bar_customer_level07,
		        cust.bar_customer_level08,
		        cust.bar_customer_level09,
		        cust.bar_customer_level10,
		        cust.bar_customer_level11,
		        cust.membertype,
		        cust.generation,
		        cust.ragged_level01,
		        cust.ragged_level02,
		        cust.ragged_level03,
		        cust.ragged_level04,
		        cust.ragged_level05,
		        cust.ragged_level06,
		        cust.ragged_level07,
		        cust.ragged_level08,
		        cust.ragged_level09,
		        cust.ragged_level10,
		        cust.ragged_level11,
				cust.demand_group,
				COALESCE( ha.hierarchy_a2_id,'unknown') as A2,
				COALESCE( ha.hierarchy_a1_id,'unknown') as A1,
				COALESCE( ha.hierarchy_a2_desc,'unknown') as a2_description,
				COALESCE( ha.hierarchy_a1_desc,'unknown') as a1_description,
				COALESCE( ccac_soldto_name.soldto_name,'unknown') as SoldToName,
				COALESCE( ccac_shipto_name.soldto_name,'unknown') as ShipToName,
				COALESCE( cch.total_customer, 'unknown' ) as level01_commercial,
				COALESCE( cch.segment, 'unknown' ) as level02_commercial,
				COALESCE( cch.channel, 'unknown' ) as level03_commercial,
				COALESCE( cch.market, 'unknown' ) as level04_commercial,
				COALESCE( cch.major_customer, 'unknown' ) as level05_commercial,
				COALESCE( cch.base_customer, 'unknown' ) as level06_commercial,
				COALESCE( hb.hierarchy_b_id, 'unknown' ) as hierarchy_b_id,
				COALESCE( hb.hierarchy_b_desc, 'unknown' ) as hierarchy_b_desc,
				COALESCE( hc.hierarchy_c_id, 'unknown' ) as hierarchy_c_id,
				COALESCE( hc.hierarchy_c_desc, 'unknown' ) as hierarchy_c_desc
		FROM	tmp_customer_core as cust
		
				left join tmp_A_hierarchy ha on lower(ha.soldto) = lower(cust.soldtocust)
--				left outer join tmp_A1_hierarchy ha1 on lower(ha1.soldto) = lower(cust.soldtocust)
--				left outer join tmp_A2_hierarchy ha2 on lower(ha2.soldto) = lower(cust.soldtocust)
				left outer join tmp_B_hierarchy hb on lower(hb.shipto) = lower(cust.shiptocust)
				left outer join tmp_C_hierarchy hc on lower(hc.soldto) = lower(cust.soldtocust)
				
				left outer join ref_data.customer_commercial_hierarchy cch 
					on 	lower(cch.base_customer) = lower(cust.bar_custno)
			
				LEFT JOIN tmp_soldto_name_mapping as ccac_soldto_name 
					on lower(ccac_soldto_name.soldto_num) = lower(cust.soldtocust)
				LEFT JOIN tmp_soldto_name_mapping as ccac_shipto_name 
					on lower(ccac_shipto_name.soldto_num) = lower(cust.shiptocust)
	;

/* check for dups */
--select 	customer_id,  count(*)
--from 	stage_dim_customer
--group by customer_id
--having count(*) > 1
--order by 2 desc
--;

	/* 	unknown_shipto */
	insert into stage_dim_customer (
				customer_id,
				
				soldto,
				shipto,
				bar_customer,
				
				bar_customer_desc,
		        bar_customer_level01,
		        bar_customer_level02,
		        bar_customer_level03,
		        bar_customer_level04,
		        bar_customer_level05,
		        bar_customer_level06,
		        bar_customer_level07,
		        bar_customer_level08,
		        bar_customer_level09,
		        bar_customer_level10,
		        bar_customer_level11,
		        membertype,
		        generation,
		        ragged_level01,
		        ragged_level02,
		        ragged_level03,
		        ragged_level04,
		        ragged_level05,
		        ragged_level06,
		        ragged_level07,
		        ragged_level08,
		        ragged_level09,
		        ragged_level10,
		        ragged_level11,
				demand_group,
				A2,
				A1,
				a2_description,
				a1_description,
				soldto_name,
				shipto_name,
				level01_commercial,
				level02_commercial,
				level03_commercial,
				level04_commercial,
				level05_commercial,
				level06_commercial,
				hierarchy_b_id,
				hierarchy_b_desc,
				hierarchy_c_id,
				hierarchy_c_desc
		)
		SELECT	cust.customer_id,
				
				cust.soldtocust,
				cust.shiptocust,
				cust.bar_custno as bar_customer,
				
				cust.bar_customer_desc,
		        cust.bar_customer_level01,
		        cust.bar_customer_level02,
		        cust.bar_customer_level03,
		        cust.bar_customer_level04,
		        cust.bar_customer_level05,
		        cust.bar_customer_level06,
		        cust.bar_customer_level07,
		        cust.bar_customer_level08,
		        cust.bar_customer_level09,
		        cust.bar_customer_level10,
		        cust.bar_customer_level11,
		        cust.membertype,
		        cust.generation,
		        cust.ragged_level01,
		        cust.ragged_level02,
		        cust.ragged_level03,
		        cust.ragged_level04,
		        cust.ragged_level05,
		        cust.ragged_level06,
		        cust.ragged_level07,
		        cust.ragged_level08,
		        cust.ragged_level09,
		        cust.ragged_level10,
		        cust.ragged_level11,
				cust.demand_group,
				COALESCE( ha.hierarchy_a2_id,'unknown') as A2,
				COALESCE( ha.hierarchy_a1_id,'unknown') as A1,
				COALESCE( ha.hierarchy_a2_desc,'unknown') as a2_description,
				COALESCE( ha.hierarchy_a1_desc,'unknown') as a1_description,
				COALESCE( ccac_soldto_name.soldto_name,'unknown') as SoldToName,
				COALESCE( ccac_shipto_name.soldto_name,'unknown') as ShipToName,
				COALESCE( cch.total_customer, 'unknown' ) as level01_commercial,
				COALESCE( cch.segment, 'unknown' ) as level02_commercial,
				COALESCE( cch.channel, 'unknown' ) as level03_commercial,
				COALESCE( cch.market, 'unknown' ) as level04_commercial,
				COALESCE( cch.major_customer, 'unknown' ) as level05_commercial,
				COALESCE( cch.base_customer, 'unknown' ) as level06_commercial,
				COALESCE( hb.hierarchy_b_id, 'unknown' ) as hierarchy_b_id,
				COALESCE( hb.hierarchy_b_desc, 'unknown' ) as hierarchy_b_desc,
				COALESCE( hc.hierarchy_c_id, 'unknown' ) as hierarchy_c_id,
				COALESCE( hc.hierarchy_c_desc, 'unknown' ) as hierarchy_c_desc
		FROM	tmp_customer_core_unknown_shipto as cust
				left outer join stage_dim_customer sdc
					on 	lower(sdc.soldto) = lower(cust.soldtocust) and
						lower(sdc.shipto) = lower(cust.shiptocust) and
						lower(sdc.bar_customer) = lower(cust.bar_custno)
				left join tmp_A_hierarchy ha on lower(ha.soldto) = lower(cust.soldtocust)
--				left outer join tmp_A1_hierarchy ha1 on lower(ha1.soldto) = lower(cust.soldtocust)
--				left outer join tmp_A2_hierarchy ha2 on lower(ha2.soldto) = lower(cust.soldtocust)
				left outer join tmp_B_hierarchy hb on lower(hb.shipto) = lower(cust.shiptocust)
				left outer join tmp_C_hierarchy hc on lower(hc.soldto) = lower(cust.soldtocust)
				
				left outer join ref_data.customer_commercial_hierarchy cch 
					on 	lower(cch.base_customer) = lower(cust.bar_custno)
	            	
				LEFT JOIN tmp_soldto_name_mapping as ccac_soldto_name 
					on lower(ccac_soldto_name.soldto_num) = lower(cust.soldtocust)
				LEFT JOIN tmp_soldto_name_mapping as ccac_shipto_name 
					on lower(ccac_shipto_name.soldto_num) = lower(cust.shiptocust)
		WHERE 	sdc.bar_customer is null 
	;
/* check for dups */
--select 	customer_id,  count(*)
--from 	stage_dim_customer
--group by customer_id
--having count(*) > 1
--order by 2 desc
--;
	/* 	create dummy records for members of the 
	 * 	BA&R Hierarchy not currently represented in 
	 * 	the transactions 
	 */
	insert into stage_dim_customer (
				customer_id,
				soldto,
				shipto,
				bar_customer,
				bar_customer_desc,
		        bar_customer_level01,
		        bar_customer_level02,
		        bar_customer_level03,
		        bar_customer_level04,
		        bar_customer_level05,
		        bar_customer_level06,
		        bar_customer_level07,
		        bar_customer_level08,
		        bar_customer_level09,
		        bar_customer_level10,
		        bar_customer_level11,
		        membertype,
		        generation,
		        ragged_level01,
		        ragged_level02,
		        ragged_level03,
		        ragged_level04,
		        ragged_level05,
		        ragged_level06,
		        ragged_level07,
		        ragged_level08,
		        ragged_level09,
		        ragged_level10,
		        ragged_level11,
				demand_group,
				A2,
				A1,
				a2_description,
				a1_description,
				soldto_name,
				shipto_name,
				level01_commercial,
				level02_commercial,
				level03_commercial,
				level04_commercial,
				level05_commercial,
				level06_commercial,
				hierarchy_b_id,
				hierarchy_b_desc,
				hierarchy_c_id,
				hierarchy_c_desc
	
		)
		SELECT	'unknown|unknown|' || lower(hier.bar_custno) as customer_id,
				'unknown' as soldto,
				'unknown' as shipto,
				hier.bar_custno as bar_customer,
				hier.bar_customer_desc,
		        hier.bar_customer_level01,
		        hier.bar_customer_level02,
		        hier.bar_customer_level03,
		        hier.bar_customer_level04,
		        hier.bar_customer_level05,
		        hier.bar_customer_level06,
		        hier.bar_customer_level07,
		        hier.bar_customer_level08,
		        hier.bar_customer_level09,
		        hier.bar_customer_level10,
		        hier.bar_customer_level11,
		        hier.membertype,
		        hier.generation,
		        hier.ragged_level01,
		        hier.ragged_level02,
		        hier.ragged_level03,
		        hier.ragged_level04,
		        hier.ragged_level05,
		        hier.ragged_level06,
		        hier.ragged_level07,
		        hier.ragged_level08,
		        hier.ragged_level09,
		        hier.ragged_level10,
		        hier.ragged_level11,
				'unknown' as demand_group,
				'unknown' as A2,
				'unknown' as A1,
				'unknown' as a2_description,
				'unknown' as a1_description,
				'unknown' as soldto_name,
				'unknown' as shipto_name,
				COALESCE( cch.total_customer, 'unknown' ) as level01_commercial,
				COALESCE( cch.segment, 'unknown' ) as level02_commercial,
				COALESCE( cch.channel, 'unknown' ) as level03_commercial,
				COALESCE( cch.market, 'unknown' ) as level04_commercial,
				COALESCE( cch.major_customer, 'unknown' ) as level05_commercial,
				COALESCE( cch.base_customer, 'unknown' ) as level06_commercial,
				'unknown' as hierarchy_b_id,
				'unknown' as hierarchy_b_desc,
				'unknown' as hierarchy_c_id,
				'unknown' as hierarchy_c_desc
		from 	tmp_customer_bar_hierarchy hier
				left outer join stage_dim_customer sdc
					on 	lower(sdc.bar_customer) = lower(hier.bar_custno)
				left outer join ref_data.customer_commercial_hierarchy cch 
					on 	lower(cch.base_customer) = lower(hier.bar_custno)
		where 	sdc.bar_customer is null
	;
	/* Account Exception: Royalty */
	insert into stage_dim_customer (
				customer_id,
				
				soldto,
				shipto,
				bar_customer,
				
				bar_customer_desc,
		        bar_customer_level01,
		        bar_customer_level02,
		        bar_customer_level03,
		        bar_customer_level04,
		        bar_customer_level05,
		        bar_customer_level06,
		        bar_customer_level07,
		        bar_customer_level08,
		        bar_customer_level09,
		        bar_customer_level10,
		        bar_customer_level11,
		        membertype,
		        generation,
		        ragged_level01,
		        ragged_level02,
		        ragged_level03,
		        ragged_level04,
		        ragged_level05,
		        ragged_level06,
		        ragged_level07,
		        ragged_level08,
		        ragged_level09,
		        ragged_level10,
		        ragged_level11,
				demand_group,
				A2,
				A1,
				a2_description,
				a1_description,
				soldto_name,
				shipto_name,
				level01_commercial,
				level02_commercial,
				level03_commercial,
				level04_commercial,
				level05_commercial,
				level06_commercial,
				hierarchy_b_id,
				hierarchy_b_desc,
				hierarchy_c_id,
				hierarchy_c_desc
		)
		SELECT	cust.customer_id,
				
				cust.soldtocust,
				cust.shiptocust,
				cust.bar_custno as bar_customer,
				
				cust.bar_customer_desc,
		        cust.bar_customer_level01,
		        cust.bar_customer_level02,
		        cust.bar_customer_level03,
		        cust.bar_customer_level04,
		        cust.bar_customer_level05,
		        cust.bar_customer_level06,
		        cust.bar_customer_level07,
		        cust.bar_customer_level08,
		        cust.bar_customer_level09,
		        cust.bar_customer_level10,
		        cust.bar_customer_level11,
		        cust.membertype,
		        cust.generation,
		        cust.ragged_level01,
		        cust.ragged_level02,
		        cust.ragged_level03,
		        cust.ragged_level04,
		        cust.ragged_level05,
		        cust.ragged_level06,
		        cust.ragged_level07,
		        cust.ragged_level08,
		        cust.ragged_level09,
		        cust.ragged_level10,
		        cust.ragged_level11,
				cust.demand_group,
				'unknown' as A2,
				'unknown' as A1,
				'unknown' as a2_description,
				'unknown' as a1_description,
				cust.soldtocust as SoldToName,
				cust.shiptocust as ShipToName,
				COALESCE( cch.total_customer, 'unknown' ) as level01_commercial,
				COALESCE( cch.segment, 'unknown' ) as level02_commercial,
				COALESCE( cch.channel, 'unknown' ) as level03_commercial,
				COALESCE( cch.market, 'unknown' ) as level04_commercial,
				COALESCE( cch.major_customer, 'unknown' ) as level05_commercial,
				COALESCE( cch.base_customer, 'unknown' ) as level06_commercial,
				'unknown' as hierarchy_b_id,
				'unknown' as hierarchy_b_desc,
				'unknown' as hierarchy_c_id,
				'unknown' as hierarchy_c_desc
		FROM	tmp_customer_core_account_exception_royalty as cust				
				left outer join ref_data.customer_commercial_hierarchy cch 
					on 	lower(cch.base_customer) = lower(cust.bar_custno)
	;	 
	/* Account Exception: rule27 */
	insert into stage_dim_customer (
				customer_id,
				
				soldto,
				shipto,
				bar_customer,
				
				bar_customer_desc,
		        bar_customer_level01,
		        bar_customer_level02,
		        bar_customer_level03,
		        bar_customer_level04,
		        bar_customer_level05,
		        bar_customer_level06,
		        bar_customer_level07,
		        bar_customer_level08,
		        bar_customer_level09,
		        bar_customer_level10,
		        bar_customer_level11,
		        membertype,
		        generation,
		        ragged_level01,
		        ragged_level02,
		        ragged_level03,
		        ragged_level04,
		        ragged_level05,
		        ragged_level06,
		        ragged_level07,
		        ragged_level08,
		        ragged_level09,
		        ragged_level10,
		        ragged_level11,
				demand_group,
				A2,
				A1,
				a2_description,
				a1_description,
				soldto_name,
				shipto_name,
				level01_commercial,
				level02_commercial,
				level03_commercial,
				level04_commercial,
				level05_commercial,
				level06_commercial,
				hierarchy_b_id,
				hierarchy_b_desc,
				hierarchy_c_id,
				hierarchy_c_desc
		)
		SELECT	cust.customer_id,
				
				cust.soldtocust,
				cust.shiptocust,
				cust.bar_custno as bar_customer,
				
				cust.bar_customer_desc,
		        cust.bar_customer_level01,
		        cust.bar_customer_level02,
		        cust.bar_customer_level03,
		        cust.bar_customer_level04,
		        cust.bar_customer_level05,
		        cust.bar_customer_level06,
		        cust.bar_customer_level07,
		        cust.bar_customer_level08,
		        cust.bar_customer_level09,
		        cust.bar_customer_level10,
		        cust.bar_customer_level11,
		        cust.membertype,
		        cust.generation,
		        cust.ragged_level01,
		        cust.ragged_level02,
		        cust.ragged_level03,
		        cust.ragged_level04,
		        cust.ragged_level05,
		        cust.ragged_level06,
		        cust.ragged_level07,
		        cust.ragged_level08,
		        cust.ragged_level09,
		        cust.ragged_level10,
		        cust.ragged_level11,
				cust.demand_group,
				'unknown' as A2,
				'unknown' as A1,
				'unknown' as a2_description,
				'unknown' as a1_description,
				cust.soldtocust as SoldToName,
				cust.shiptocust as ShipToName,
				COALESCE( cch.total_customer, 'unknown' ) as level01_commercial,
				COALESCE( cch.segment, 'unknown' ) as level02_commercial,
				COALESCE( cch.channel, 'unknown' ) as level03_commercial,
				COALESCE( cch.market, 'unknown' ) as level04_commercial,
				COALESCE( cch.major_customer, 'unknown' ) as level05_commercial,
				COALESCE( cch.base_customer, 'unknown' ) as level06_commercial,
				'unknown' as hierarchy_b_id,
				'unknown' as hierarchy_b_desc,
				'unknown' as hierarchy_c_id,
				'unknown' as hierarchy_c_desc
		FROM	tmp_customer_core_account_exception_rule27 as cust				
				left outer join ref_data.customer_commercial_hierarchy cch 
					on 	lower(cch.base_customer) = lower(cust.bar_custno)
	;
	/* Account Exception: rule28 */
	insert into stage_dim_customer (
				customer_id,
				
				soldto,
				shipto,
				bar_customer,
				
				bar_customer_desc,
		        bar_customer_level01,
		        bar_customer_level02,
		        bar_customer_level03,
		        bar_customer_level04,
		        bar_customer_level05,
		        bar_customer_level06,
		        bar_customer_level07,
		        bar_customer_level08,
		        bar_customer_level09,
		        bar_customer_level10,
		        bar_customer_level11,
		        membertype,
		        generation,
		        ragged_level01,
		        ragged_level02,
		        ragged_level03,
		        ragged_level04,
		        ragged_level05,
		        ragged_level06,
		        ragged_level07,
		        ragged_level08,
		        ragged_level09,
		        ragged_level10,
		        ragged_level11,
				demand_group,
				A2,
				A1,
				a2_description,
				a1_description,
				soldto_name,
				shipto_name,
				level01_commercial,
				level02_commercial,
				level03_commercial,
				level04_commercial,
				level05_commercial,
				level06_commercial,
				hierarchy_b_id,
				hierarchy_b_desc,
				hierarchy_c_id,
				hierarchy_c_desc
		)
		SELECT	cust.customer_id,
				
				cust.soldtocust,
				cust.shiptocust,
				cust.bar_custno as bar_customer,
				
				cust.bar_customer_desc,
		        cust.bar_customer_level01,
		        cust.bar_customer_level02,
		        cust.bar_customer_level03,
		        cust.bar_customer_level04,
		        cust.bar_customer_level05,
		        cust.bar_customer_level06,
		        cust.bar_customer_level07,
		        cust.bar_customer_level08,
		        cust.bar_customer_level09,
		        cust.bar_customer_level10,
		        cust.bar_customer_level11,
		        cust.membertype,
		        cust.generation,
		        cust.ragged_level01,
		        cust.ragged_level02,
		        cust.ragged_level03,
		        cust.ragged_level04,
		        cust.ragged_level05,
		        cust.ragged_level06,
		        cust.ragged_level07,
		        cust.ragged_level08,
		        cust.ragged_level09,
		        cust.ragged_level10,
		        cust.ragged_level11,
				cust.demand_group,
				'unknown' as A2,
				'unknown' as A1,
				'unknown' as a2_description,
				'unknown' as a1_description,
				cust.soldtocust as SoldToName,
				cust.shiptocust as ShipToName,
				COALESCE( cch.total_customer, 'unknown' ) as level01_commercial,
				COALESCE( cch.segment, 'unknown' ) as level02_commercial,
				COALESCE( cch.channel, 'unknown' ) as level03_commercial,
				COALESCE( cch.market, 'unknown' ) as level04_commercial,
				COALESCE( cch.major_customer, 'unknown' ) as level05_commercial,
				COALESCE( cch.base_customer, 'unknown' ) as level06_commercial,
				'unknown' as hierarchy_b_id,
				'unknown' as hierarchy_b_desc,
				'unknown' as hierarchy_c_id,
				'unknown' as hierarchy_c_desc
		FROM	tmp_customer_core_account_exception_rule28 as cust				
				left outer join ref_data.customer_commercial_hierarchy cch 
					on 	lower(cch.base_customer) = lower(cust.bar_custno)
	;
	/* Account Exception: FOB */
	insert into stage_dim_customer (
				customer_id,
				
				soldto,
				shipto,
				bar_customer,
				
				bar_customer_desc,
		        bar_customer_level01,
		        bar_customer_level02,
		        bar_customer_level03,
		        bar_customer_level04,
		        bar_customer_level05,
		        bar_customer_level06,
		        bar_customer_level07,
		        bar_customer_level08,
		        bar_customer_level09,
		        bar_customer_level10,
		        bar_customer_level11,
		        membertype,
		        generation,
		        ragged_level01,
		        ragged_level02,
		        ragged_level03,
		        ragged_level04,
		        ragged_level05,
		        ragged_level06,
		        ragged_level07,
		        ragged_level08,
		        ragged_level09,
		        ragged_level10,
		        ragged_level11,
				demand_group,
				A2,
				A1,
				a2_description,
				a1_description,
				soldto_name,
				shipto_name,
				level01_commercial,
				level02_commercial,
				level03_commercial,
				level04_commercial,
				level05_commercial,
				level06_commercial,
				hierarchy_b_id,
				hierarchy_b_desc,
				hierarchy_c_id,
				hierarchy_c_desc
		)
		SELECT	cust.customer_id,
				
				cust.soldtocust,
				cust.shiptocust,
				cust.bar_custno as bar_customer,
				
				cust.bar_customer_desc,
		        cust.bar_customer_level01,
		        cust.bar_customer_level02,
		        cust.bar_customer_level03,
		        cust.bar_customer_level04,
		        cust.bar_customer_level05,
		        cust.bar_customer_level06,
		        cust.bar_customer_level07,
		        cust.bar_customer_level08,
		        cust.bar_customer_level09,
		        cust.bar_customer_level10,
		        cust.bar_customer_level11,
		        cust.membertype,
		        cust.generation,
		        cust.ragged_level01,
		        cust.ragged_level02,
		        cust.ragged_level03,
		        cust.ragged_level04,
		        cust.ragged_level05,
		        cust.ragged_level06,
		        cust.ragged_level07,
		        cust.ragged_level08,
		        cust.ragged_level09,
		        cust.ragged_level10,
		        cust.ragged_level11,
				cust.demand_group,
				'unknown' as A2,
				'unknown' as A1,
				'unknown' as a2_description,
				'unknown' as a1_description,
				cust.soldtocust as SoldToName,
				cust.shiptocust as ShipToName,
				COALESCE( cch.total_customer, 'unknown' ) as level01_commercial,
				COALESCE( cch.segment, 'unknown' ) as level02_commercial,
				COALESCE( cch.channel, 'unknown' ) as level03_commercial,
				COALESCE( cch.market, 'unknown' ) as level04_commercial,
				COALESCE( cch.major_customer, 'unknown' ) as level05_commercial,
				COALESCE( cch.base_customer, 'unknown' ) as level06_commercial,
				'unknown' as hierarchy_b_id,
				'unknown' as hierarchy_b_desc,
				'unknown' as hierarchy_c_id,
				'unknown' as hierarchy_c_desc
		FROM	tmp_customer_core_account_exception_fob as cust				
				left outer join ref_data.customer_commercial_hierarchy cch 
					on 	lower(cch.base_customer) = lower(cust.bar_custno)
				left outer join stage_dim_customer stg_dc
					on 	lower(cust.soldtocust) = lower(stg_dc.soldto) and
						lower(cust.shiptocust) = lower(stg_dc.shipto) and
						lower(cust.bar_custno) = lower(stg_dc.bar_customer)
		where 	lower(stg_dc.soldto) is null
	;
	/* Account Exception: FOB Std Cos */
	insert into stage_dim_customer (
				customer_id,
				
				soldto,
				shipto,
				bar_customer,
				
				bar_customer_desc,
		        bar_customer_level01,
		        bar_customer_level02,
		        bar_customer_level03,
		        bar_customer_level04,
		        bar_customer_level05,
		        bar_customer_level06,
		        bar_customer_level07,
		        bar_customer_level08,
		        bar_customer_level09,
		        bar_customer_level10,
		        bar_customer_level11,
		        membertype,
		        generation,
		        ragged_level01,
		        ragged_level02,
		        ragged_level03,
		        ragged_level04,
		        ragged_level05,
		        ragged_level06,
		        ragged_level07,
		        ragged_level08,
		        ragged_level09,
		        ragged_level10,
		        ragged_level11,
				demand_group,
				A2,
				A1,
				a2_description,
				a1_description,
				soldto_name,
				shipto_name,
				level01_commercial,
				level02_commercial,
				level03_commercial,
				level04_commercial,
				level05_commercial,
				level06_commercial,
				hierarchy_b_id,
				hierarchy_b_desc,
				hierarchy_c_id,
				hierarchy_c_desc
		)
		SELECT	cust.customer_id,
				
				cust.soldtocust,
				cust.shiptocust,
				cust.bar_custno as bar_customer,
				
				cust.bar_customer_desc,
		        cust.bar_customer_level01,
		        cust.bar_customer_level02,
		        cust.bar_customer_level03,
		        cust.bar_customer_level04,
		        cust.bar_customer_level05,
		        cust.bar_customer_level06,
		        cust.bar_customer_level07,
		        cust.bar_customer_level08,
		        cust.bar_customer_level09,
		        cust.bar_customer_level10,
		        cust.bar_customer_level11,
		        cust.membertype,
		        cust.generation,
		        cust.ragged_level01,
		        cust.ragged_level02,
		        cust.ragged_level03,
		        cust.ragged_level04,
		        cust.ragged_level05,
		        cust.ragged_level06,
		        cust.ragged_level07,
		        cust.ragged_level08,
		        cust.ragged_level09,
		        cust.ragged_level10,
		        cust.ragged_level11,
				cust.demand_group,
				'unknown' as A2,
				'unknown' as A1,
				'unknown' as a2_description,
				'unknown' as a1_description,
				cust.soldtocust as SoldToName,
				cust.shiptocust as ShipToName,
				COALESCE( cch.total_customer, 'unknown' ) as level01_commercial,
				COALESCE( cch.segment, 'unknown' ) as level02_commercial,
				COALESCE( cch.channel, 'unknown' ) as level03_commercial,
				COALESCE( cch.market, 'unknown' ) as level04_commercial,
				COALESCE( cch.major_customer, 'unknown' ) as level05_commercial,
				COALESCE( cch.base_customer, 'unknown' ) as level06_commercial,
				'unknown' as hierarchy_b_id,
				'unknown' as hierarchy_b_desc,
				'unknown' as hierarchy_c_id,
				'unknown' as hierarchy_c_desc
		FROM	tmp_customer_core_account_exception_fob_std_cos as cust				
				left outer join ref_data.customer_commercial_hierarchy cch 
					on 	lower(cch.base_customer) = lower(cust.bar_custno)
				left outer join stage_dim_customer as stg_dc
					on 	lower(cust.soldtocust) = lower(stg_dc.soldto) and
						lower(cust.shiptocust) = lower(stg_dc.shipto) and
						lower(cust.bar_custno) = lower(stg_dc.bar_customer)
		where 	lower(stg_dc.soldto) is null
	;

	/* Account Exception: RSA */
	insert into stage_dim_customer (
				customer_id,
				
				soldto,
				shipto,
				bar_customer,
				
				bar_customer_desc,
		        bar_customer_level01,
		        bar_customer_level02,
		        bar_customer_level03,
		        bar_customer_level04,
		        bar_customer_level05,
		        bar_customer_level06,
		        bar_customer_level07,
		        bar_customer_level08,
		        bar_customer_level09,
		        bar_customer_level10,
		        bar_customer_level11,
		        membertype,
		        generation,
		        ragged_level01,
		        ragged_level02,
		        ragged_level03,
		        ragged_level04,
		        ragged_level05,
		        ragged_level06,
		        ragged_level07,
		        ragged_level08,
		        ragged_level09,
		        ragged_level10,
		        ragged_level11,
				demand_group,
				A2,
				A1,
				a2_description,
				a1_description,
				soldto_name,
				shipto_name,
				level01_commercial,
				level02_commercial,
				level03_commercial,
				level04_commercial,
				level05_commercial,
				level06_commercial,
				hierarchy_b_id,
				hierarchy_b_desc,
				hierarchy_c_id,
				hierarchy_c_desc
		)
		SELECT	cust.customer_id,
				
				cust.soldtocust,
				cust.shiptocust,
				cust.bar_custno as bar_customer,
				
				cust.bar_customer_desc,
		        cust.bar_customer_level01,
		        cust.bar_customer_level02,
		        cust.bar_customer_level03,
		        cust.bar_customer_level04,
		        cust.bar_customer_level05,
		        cust.bar_customer_level06,
		        cust.bar_customer_level07,
		        cust.bar_customer_level08,
		        cust.bar_customer_level09,
		        cust.bar_customer_level10,
		        cust.bar_customer_level11,
		        cust.membertype,
		        cust.generation,
		        cust.ragged_level01,
		        cust.ragged_level02,
		        cust.ragged_level03,
		        cust.ragged_level04,
		        cust.ragged_level05,
		        cust.ragged_level06,
		        cust.ragged_level07,
		        cust.ragged_level08,
		        cust.ragged_level09,
		        cust.ragged_level10,
		        cust.ragged_level11,
				cust.demand_group,
				'unknown' as A2,
				'unknown' as A1,
				'unknown' as a2_description,
				'unknown' as a1_description,
				cust.soldtocust as SoldToName,
				cust.shiptocust as ShipToName,
				COALESCE( cch.total_customer, 'unknown' ) as level01_commercial,
				COALESCE( cch.segment, 'unknown' ) as level02_commercial,
				COALESCE( cch.channel, 'unknown' ) as level03_commercial,
				COALESCE( cch.market, 'unknown' ) as level04_commercial,
				COALESCE( cch.major_customer, 'unknown' ) as level05_commercial,
				COALESCE( cch.base_customer, 'unknown' ) as level06_commercial,
				'unknown' as hierarchy_b_id,
				'unknown' as hierarchy_b_desc,
				'unknown' as hierarchy_c_id,
				'unknown' as hierarchy_c_desc
		FROM	tmp_customer_core_account_exception_rsa_reconcile as cust				
				left outer join ref_data.customer_commercial_hierarchy cch 
					on 	lower(cch.base_customer) = lower(cust.bar_custno)
				left outer join stage_dim_customer as stg_dc
					on 	lower(cust.soldtocust) = lower(stg_dc.soldto) and
						lower(cust.shiptocust) = lower(stg_dc.shipto) and
						lower(cust.bar_custno) = lower(stg_dc.bar_customer)
		where 	lower(stg_dc.soldto) is null
	;
--select 	dc.soldto,
--		dc.a1, dc.a1_description,
--		dc.a2, dc.a2_description 
--from 	stage_dim_customer as dc
--where 	soldto in ('0010059859','0010164701')
	/* ------------------------------------------------------------------ 
	 * 	Part 05: Update existing rows in target table
	 *		TODO: add hash check to skip rows w/ no changes
	 * ------------------------------------------------------------------
	 */
	UPDATE 	dw.dim_customer
	SET 	soldto_name 	   = stg.soldto_name,
			shipto_name        = stg.shipto_name,
			base_customer      = stg.bar_customer,
			base_customer_desc = stg.bar_customer_desc,
			level01_BAR        = stg.bar_customer_level01,
			level02_BAR        = stg.bar_customer_level02,
			level03_BAR        = stg.bar_customer_level03,
			level04_BAR        = stg.bar_customer_level04,
			level05_BAR        = stg.bar_customer_level05,
			level06_BAR        = stg.bar_customer_level06,
			level07_BAR        = stg.bar_customer_level07,
			level08_BAR        = stg.bar_customer_level08,
			level09_BAR        = stg.bar_customer_level09,
			level10_BAR        = stg.bar_customer_level10,
			level11_BAR        = stg.bar_customer_level11,
			membertype         = stg.membertype,
			generation         = stg.generation,
			ragged_level01_BAR = stg.ragged_level01,
			ragged_level02_BAR = stg.ragged_level02,
			ragged_level03_BAR = stg.ragged_level03,
			ragged_level04_BAR = stg.ragged_level04,
			ragged_level05_BAR = stg.ragged_level05,
			ragged_level06_BAR = stg.ragged_level06,
			ragged_level07_BAR = stg.ragged_level07,
			ragged_level08_BAR = stg.ragged_level08,
			ragged_level09_BAR = stg.ragged_level09,
			ragged_level10_BAR = stg.ragged_level10,
			ragged_level11_BAR = stg.ragged_level11,
			demand_group       = stg.demand_group,
			a2                 = stg.a2,
			a1                 = stg.a1,
			a2_desc            = stg.a2_description,
			a1_desc            = stg.a1_description,
			start_date         = cast('1900-01-01' as date),
			end_date           = cast('12-31-9999' as date),
			audit_loadts       = getdate(),
			level01_commercial = stg.level01_commercial,
			level02_commercial = stg.level02_commercial,
			level03_commercial = stg.level03_commercial,
			level04_commercial = stg.level04_commercial,
			level05_commercial = stg.level05_commercial,
			level06_commercial = stg.level06_commercial,
			hierarchy_b_id	   = stg.hierarchy_b_id,
			hierarchy_b_desc   = stg.hierarchy_b_desc,
			hierarchy_c_id 	   = stg.hierarchy_c_id,
			hierarchy_c_desc   = stg.hierarchy_c_desc
			
	FROM 	stage_dim_customer stg 
	WHERE 	lower(stg.customer_id) = lower(dim_customer.customer_id)
	;
	
  
	/* ------------------------------------------------------------------ 
	 * 	Part 06: Insert new rows
	 * ------------------------------------------------------------------
	 */
	INSERT INTO dw.dim_customer (
			customer_id,
			soldto_number,
			shipto_number,
			soldto_name,
			shipto_name,
			base_customer,
			base_customer_desc,
			level01_BAR,
			level02_BAR,
			level03_BAR,
			level04_BAR,
			level05_BAR,
			level06_BAR,
			level07_BAR,
			level08_BAR,
			level09_BAR,
			level10_BAR,
			level11_BAR,
			membertype,
			generation,
			ragged_level01_BAR,
			ragged_level02_BAR,
			ragged_level03_BAR,
			ragged_level04_BAR,
			ragged_level05_BAR,
			ragged_level06_BAR,
			ragged_level07_BAR,
			ragged_level08_BAR,
			ragged_level09_BAR,
			ragged_level10_BAR,
			ragged_level11_BAR,
			demand_group,
			a2,
			a1,
			a2_desc,
			a1_desc,
			start_date,
			end_date,
			audit_loadts,
			level01_commercial,
			level02_commercial,
			level03_commercial,
			level04_commercial,
			level05_commercial,
			level06_commercial,
			hierarchy_b_id,
			hierarchy_b_desc,
			hierarchy_c_id,
			hierarchy_c_desc
		)
		Select 	stg.customer_id AS customer_id,
				stg.soldto as soldto_number,
				stg.shipto as shipto_number,
				stg.soldto_name,
				stg.shipto_name,
				stg.bar_customer as base_customer,
				stg.bar_customer_desc as base_customer_desc,
				stg.bar_customer_level01 as level01,
				stg.bar_customer_level02 as level02,
				stg.bar_customer_level03 as level03,
				stg.bar_customer_level04 as level04,
				stg.bar_customer_level05 as level05,
				stg.bar_customer_level06 as level06,
				stg.bar_customer_level07 as level07,
				stg.bar_customer_level08 as level08,
				stg.bar_customer_level09 as level09,
				stg.bar_customer_level10 as level10,
				stg.bar_customer_level11 as level11,
				stg.membertype,
				cast(stg.generation as int) as generation,
				stg.ragged_level01 as ragged_level01,
				stg.ragged_level02 as ragged_level02,
				stg.ragged_level03 as ragged_level03,
				stg.ragged_level04 as ragged_level04,
				stg.ragged_level05 as ragged_level05,
				stg.ragged_level06 as ragged_level06,
				stg.ragged_level07 as ragged_level07,
				stg.ragged_level08 as ragged_level08,
				stg.ragged_level09 as ragged_level09,
				stg.ragged_level10 as ragged_level10,
				stg.ragged_level11 as ragged_level11,
				stg.demand_group,
				stg.a2,
				stg.a1,
				stg.a2_description,
				stg.a1_description,
				cast('1900-01-01' as date) start_date,
				cast('12-31-9999' as date) as end_date,
				getdate() as audit_loadts,
				stg.level01_commercial,
				stg.level02_commercial,
				stg.level03_commercial,
				stg.level04_commercial,
				stg.level05_commercial,
				stg.level06_commercial,
				stg.hierarchy_b_id,
				stg.hierarchy_b_desc,
				stg.hierarchy_c_id,
				stg.hierarchy_c_desc
		from 	stage_dim_customer stg
				left outer join dw.dim_customer dc 
					on 	dc.customer_id = stg.customer_id
		where 	dc.customer_id is null
	;
	---remove this : sk : 050620201
	delete from dw.dim_customer where customer_id in ('unknown|unknown|consumer_oth','unknown|unknown|clubs');
          
exception
when others then raise info 'exception occur while ingesting data in dim_customer';
end
$$
;