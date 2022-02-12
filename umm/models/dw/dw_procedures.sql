CREATE OR REPLACE PROCEDURE dw.p_build_dim_business_unit(flag_reload integer)
 LANGUAGE plpgsql
AS $$
--call dw.p_build_dim_business_unit (1)
BEGIN
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_business_unit;
	end if;
	--TESTING
	--delete from dw.dim_business_unit;
	--call dw.p_build_dim_business_unit(1)
	--select count(*) from dw.dim_business_unit
	drop table if exists stage_dim_business_unit;
		create temporary table stage_dim_business_unit
		diststyle all
		as 
		select 
			'unknown' as bar_entity,
		    'unknown' as bar_entity_description,
		    'unknown' as geography,
		    'unknown' as region,
		    'unknown' as subregion,
		    cast('1900-01-01' as date) as start_date,
		    cast('9999-12-31' as date) as end_date,
		    cast('1900-01-01' as date) as audit_loadts
		union
		select 	distinct 
				ent.name as bar_entity,
				ent.description as bar_entity_description,
				ent.level4 as geography,
				ent.level5 as region,
				ent.level6 as subregion,
			    getdate() as start_date,
			    cast('9999-12-31' as date) as end_date,
			    getdate() as audit_loadts
		from 	ref_data.entity ent
		where  level4 = 'GTS_NA'
		union 
		select 
			'ADJ_RSA' as bar_entity,
		    'ADJ_RSA' as bar_entity_description,
		    'ADJ_RSA' as geography,
		    'ADJ_RSA' as region,
		    'ADJ_RSA' as subregion,
		    cast('1900-01-01' as date) as start_date,
		    cast('9999-12-31' as date) as end_date,
		    cast('1900-01-01' as date) as audit_loadts
	;
	drop table if exists stage_dim_business_unit_i;
	create temporary table stage_dim_business_unit_i
	diststyle all 
	as 
	Select s.bar_entity,
		  s.bar_entity_description,
		  s.geography,
		  s.region,
		  s.subregion,
		  s.start_date,
		  s.end_date,
		  s.audit_loadts
	from stage_dim_business_unit s 
	left join dw.dim_business_unit t on s.bar_entity = t.bar_entity
	where t.bar_entity is null; 
		
	insert into dw.dim_business_unit (
				bar_entity,
				bar_entity_description,
				geography,
				region,
				subregion,
				start_date,
				end_date,
				audit_loadts
		)
	Select *
	from stage_dim_business_unit_i
	;

	update dw.dim_business_unit 
		set bar_entity_description=s.bar_entity_description,
		  geography=s.geography,
		  region=s.region,
		  subregion=s.subregion,
		  start_date=s.start_date,
		  end_date=s.end_date,
		  audit_loadts=s.audit_loadts
	from  stage_dim_business_unit s   
	where dim_business_unit.bar_entity = s.bar_entity
	and (dim_business_unit.bar_entity_description!=s.bar_entity_description OR
		dim_business_unit.geography!=s.geography   OR 
		dim_business_unit.subregion!=s.subregion OR 
		dim_business_unit.region!=s.region
		)
;

	EXCEPTION
		when others then raise info 'exception occur while ingesting data in dim_business_unit';
END
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_dim_currency(flag_reload integer)
 LANGUAGE plpgsql
AS $$
Begin
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_currency;
	end if;
	
	drop table if exists stage_dim_currency;
	create temporary table stage_dim_currency
	diststyle all
	as 
	select 	currency_cd,
			currency_format,
			row_number() over(order by currency_cd) as currency_sort
	from 	(
				select 	distinct 
						bcta.bar_currtype as currency_cd,
						'#,0.00' as currency_format
				from 	stage.bods_core_transaction_agg bcta 
			)
	union all 
	Select 'All Converted to USD' as currency_cd,
		  '#,0.00' as 	currency_format,
		   99 as currency_sort
	;
	
	drop table if exists stage_dim_currency_i;
		   
	create temporary table stage_dim_currency_i
	diststyle all
	as
	select s.currency_cd,
		  s.currency_format,
		  s.currency_sort
	from stage_dim_currency s 
	left join dw.dim_currency t on s.currency_cd = t.currency_cd 
	where t.currency_cd is null; 
	insert into dw.dim_currency (currency_cd ,currency_format, currency_sort)
	select  currency_cd,
		   currency_format, 
		   currency_sort
	from stage_dim_currency_i; 
	update dw.dim_currency 
	set currency_format = s.currency_format, 
		   currency_sort = s.currency_sort
	from stage_dim_currency s 
	where dim_currency.currency_cd = s.currency_cd ;
	

	EXCEPTION
		when others then raise info 'exception occur while ingesting data in dim_currency';
END
$$
;

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
		from 	sapc11.kna1_current as c11
		where 	c11.kunnr is not null
	;
/* leaving P10 demand groups blank */
--	insert into tmp_demand_grp_mapping (soldto_num, demand_group)
--		select 	lower(p10.kunnr) as soldto_num,
--				p10.bran1 AS demand_group
--		from 	sapp10.kna1_current as p10
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
					from 	sapc11.knvh_current knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA1_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a1_id,
							kna1.name1 as hierarchy_a1_desc
					from 	sapc11.kna1_current kna1
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
					from 	sapp10.knvh_current knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA1_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a1_id,
							kna1.name1 as hierarchy_a1_desc
					from 	sapp10.kna1_current kna1
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
					from 	sapc11.knvh_current knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA2_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a2_id,
							kna1.name1 as hierarchy_a2_desc
					from 	sapc11.kna1_current kna1
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
					from 	sapp10.knvh_current knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA2_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a2_id,
							kna1.name1 as hierarchy_a2_desc
					from 	sapp10.kna1_current kna1
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
					from 	sapc11.knvh_current knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA2_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a1_id,
							kna1.name1 as hierarchy_a1_desc
					from 	sapc11.kna1_current kna1
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
					from 	sapp10.knvh_current knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA2_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a1_id,
							kna1.name1 as hierarchy_a1_desc
					from 	sapp10.kna1_current kna1
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
					from 	sapc11.knvh_current knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA1_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a2_id,
							kna1.name1 as hierarchy_a2_desc
					from 	sapc11.kna1_current kna1
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
					from 	sapp10.knvh_current knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA1_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a2_id,
							kna1.name1 as hierarchy_a2_desc
					from 	sapp10.kna1_current kna1
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
					from 	sapc11.knvh_current knvh
					where 	knvh.HITYP='B' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HB_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_b_id,
							kna1.name1 as hierarchy_b_desc
					from 	sapc11.kna1_current kna1
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
					from 	sapp10.knvh_current knvh
					where 	knvh.HITYP='B' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HB_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_b_id,
							kna1.name1 as hierarchy_b_desc
					from 	sapp10.kna1_current kna1
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
					from 	sapc11.knvh_current knvh
					where 	knvh.HITYP='C' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HC_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_c_id,
							kna1.name1 as hierarchy_c_desc
					from 	sapc11.kna1_current kna1
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
					from 	sapp10.knvh_current knvh
					where 	knvh.HITYP='C' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HC_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_c_id,
							kna1.name1 as hierarchy_c_desc
					from 	sapp10.kna1_current kna1
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
		from 	bods.c11_0customer_attr_current as c11
		where 	c11.kunnr is not null
	;
	insert into tmp_soldto_name_mapping (soldto_num, soldto_name)
		select 	lower(p10.kunnr) as soldto_num,
				p10.name1 as soldto_name
		from 	sapp10.kna1_current as p10
				left outer join tmp_soldto_name_mapping as mapping
					on 	lower(p10.kunnr) = lower(mapping.soldto_num)
		where 	p10.kunnr is not null and
				mapping.soldto_num is null
	;
--	CONTAINS DUPLICATE
	insert into tmp_soldto_name_mapping (soldto_num, soldto_name)
		select 	lower(lawson.cust_nbr) as soldto_num,
				lawson.cust_name as soldto_name
		from 	bods.extr_lawson_mac_cust_current as lawson
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

CREATE OR REPLACE PROCEDURE dw.p_build_dim_customer_restatement()
 LANGUAGE plpgsql
AS $$
BEGIN
	/*
	 * 		drop PROCEDURE dw.p_build_dim_customer_restatement()
	 * 		call dw.p_build_dim_customer_restatement();
	 * 		select count(*) from dw.dim_customer_restatement;
	 * 		grant execute on procedure dw.p_build_dim_customer_restatement() to group "g-ada-rsabible-sb-ro";
	 * 
	 * 
	 * 	STEPS:
	 *      Step 1 - map soldto -> demand group (sapc11/sapp10 kna1_current)                                                              
	 *      Step 2 - add bar customer via demand group (manual mapping table from SSG -> ref_data.demand_group_to_bar_customer_mapping)   
	 *      Step 3 - add bar hierarchy (bods.drm_customer_current)                                                                        
	 *      Step 4 - add A/B/C hierarchies (sapc11/sapp10 -> knvh_current & kna1_current)                                                 
	 *      Step 5 - add Commercial Hierarchy (manual mapping table from SSG -> ref_data.customer_commercial_hierarchy)                   
	 * 
	 */
	/* ================================================================================
	 * 		MASTER DATA -> SoldTo->DemandGroup
	 * ================================================================================
	 */
	drop table if exists master_soldto_demand_grp_mapping
	;
	create temporary table master_soldto_demand_grp_mapping
	DISTSTYLE ALL
	as
		select 	lower(c11.kunnr) as soldto_num,
				c11.bran1 AS demand_group
		from 	sapc11.kna1_current as c11
		where 	coalesce(trim(rtrim(c11.kunnr)),'') != '' and
				coalesce(trim(rtrim(c11.bran1)),'') != ''
	;
	insert into master_soldto_demand_grp_mapping (soldto_num, demand_group)
		select 	lower(p10.kunnr) as soldto_num,
				p10.bran1 AS demand_group
		from 	sapp10.kna1_current as p10
				left outer join master_soldto_demand_grp_mapping as mapping
					on 	lower(p10.kunnr) = lower(mapping.soldto_num)
		where 	coalesce(trim(rtrim(p10.kunnr)),'') != '' and
				coalesce(trim(rtrim(p10.bran1)),'') != '' and
				mapping.soldto_num is null
	;
--select * from master_soldto_demand_grp_mapping
--where soldto_num in ('0001013303', '0001013307');
	/*	BASE CASE: soldtos that can be...
	 * 		1. mapped to demand group (via master data)
	 * 			AND
	 * 	 	2. mapped to bar customer via demand group (manual) mapping table 
	 */
	drop table if exists restate_base_case
	;
	create temporary table restate_base_case
	DISTSTYLE ALL
	as
	select 	dmdgrp.soldto_num,
			barcust.bar_customer as bar_custno,
			dmdgrp.demand_group
	from 	master_soldto_demand_grp_mapping as dmdgrp
			inner join ref_data.demand_group_to_bar_customer_mapping as barcust
				on 	lower(barcust.demand_group) = lower(dmdgrp.demand_group)
		and NOT (barcust.demand_group = 'FARM' and barcust.bar_customer = 'TSC')
	;
--select * from restate_base_case
--where soldto_num in ('0001013303', '0001013307');
/* DEBUG: in = out */
--select 	count(*), count(distinct soldto_num) from master_soldto_demand_grp_mapping;
--select 	count(*), count(distinct soldto_num) from restate_base_case;

--select 	*
--from 	restate_base_case
--where 	soldto_num = '0001027455'
	/* ================================================================================
	 * 		EXCEPTIONS 
	 * ================================================================================
	 */
	drop table if exists exceptions_soldto
	;
	create temporary table exceptions_soldto as 
		SELECT 	distinct 
				lower(f.alloc_soldtocust) as soldto_num,
				lower(f.mapped_bar_custno) as bar_custno
		FROM	dw.fact_pnl_commercial_stacked as f
		where 	lower(f.mapped_bar_custno) IN ('customer_none', 'psd_oth') OR 
				lower(f.mapped_bar_custno) LIKE ('%_oth') OR
				lower(f.alloc_soldtocust) IN (
					'adj_royalty',
					'adj_fob_no_cust',
					'adj_fob',
					'adj_rsa',
					'adj_service',
					'adj_rebuild',
					'adj_no_prod',
					'adj_no_cust',
					'adj_psd',
					'',
					'unknown'
				)
	;
--select 	* 
--from 	exceptions_soldto
--where 	soldto_num IN ('0000000068','0001010009')
--;
--select 	*
--from 	exceptions_soldto
--where 	soldto_num = '0001027455'
--
--select 	distinct alloc_soldtocust, mapped_bar_custno
--from 	dw.fact_pnl_commercial_stacked
--where 	alloc_soldtocust = '0001027455'
	/* ================================================================================
	 * 		EDGE CASE MAPPINGS
	 * 		stage.core_tran_delta 
	 * ================================================================================
	 */
	/* 	EDGE CASES: soldtos that could NOT be...
	 * 		1. mapped to demand group (via master data)
	 * 			OR
	 * 		2. mapped to bar customer via demand group (manual) mapping table
	 * 
	 * 	These will be mapped to bar_customer via BODS transactions.
	 * 
	 */
	drop table if exists restate_edge_case_base
	;
	create temporary table restate_edge_case_base as 
		SELECT 	distinct 
				lower(f.alloc_soldtocust) as soldto_num
		FROM	dw.fact_pnl_commercial_stacked as f
				left outer join restate_base_case as base_case
					on 	lower(base_case.soldto_num) = lower(f.alloc_soldtocust)
		WHERE 	lower(base_case.soldto_num) is null
	;
/* DEBUG: in = out */
--select 	count(*), count(distinct soldto_num) from restate_edge_case_base;
--select 	*
--from 	restate_edge_case_base
--where 	soldto_num = '0001042383'

	drop table if exists _bods_trans_phase0_mapping
	;
	create temporary table _bods_trans_phase0_mapping as 
	with
		cte_base as (
			select 	distinct 
					src.soldtocust as soldto_num,
					src.mapped_bar_custno as bar_custno,
					src.postdate
			from 	stage.core_tran_delta_cleansed as src
					left join ref_data.data_processing_rule as dpr
						on 	dpr.dataprocessing_hash = src.mapped_dataprocessing_hash 
			where 	dpr.data_processing_ruleid = 1 
		),
		cte_next as (
			select 	base.soldto_num,
					base.bar_custno,
					base.postdate,
					lead(base.bar_custno) 
						over(partition by base.soldto_num order by base.postdate) as bar_custno_next
			from 	cte_base as base 
		),
		cte_historical as (
			select 	nxt.soldto_num,
					nxt.bar_custno,
					nxt.postdate,
					nxt.bar_custno_next,
					lead(nxt.postdate) 
						over (partition by nxt.soldto_num order by nxt.postdate) as postdate_next,
					row_number() 
						over (partition by nxt.soldto_num order by nxt.postdate) as rnk
			from 	cte_next as nxt 
			where 	nxt.bar_custno != nxt.bar_custno_next or 
					nxt.bar_custno_next is null
		)
		select 	hist.soldto_num,
				hist.bar_custno,				
				case
					when hist.rnk = 1 then cast('1900-01-01' as date) 
					else cast(hist.postdate as date) 
				end as start_date,
				
				case
					when hist.bar_custno_next is null then cast('9999-12-31' as date) 
					else cast(dateadd(day,-1,hist.postdate_next) as date)
				end as end_date,
				
				case when hist.bar_custno_next is null then 1 else 0 end as current_flag,
					
				getdate() as audit_loadts
		from 	cte_historical as hist
	;
	drop table if exists restate_edge_case
	;
	create temporary table restate_edge_case 
	DISTSTYLE ALL
	as
		select 	base.soldto_num,
				mapping.bar_custno as bar_custno,
				COALESCE(dmdgrp.demand_group, 'unknown') as demand_group
		from 	(
					select 	distinct soldto_num
					from 	restate_edge_case_base
				) as base
				inner join _bods_trans_phase0_mapping as mapping
					on 	lower(mapping.soldto_num) = lower(base.soldto_num) and
						mapping.current_flag = 1
				left outer join master_soldto_demand_grp_mapping as dmdgrp
					on 	lower(dmdgrp.soldto_num) = lower(base.soldto_num)
	;
--select 	*
--from 	restate_edge_case
--where 	soldto_num = '0001042383'
	/* ================================================================================
	 * 		RESTATEMENT BASE
	 * ================================================================================
	 */
	drop table if exists tmp_restate
	;
	create temporary table tmp_restate 
	DISTSTYLE ALL
	as
	with 
		cte_base as (
			select 	soldto_num,
					bar_custno, 
					demand_group
			from 	restate_edge_case
			union all 
			select 	soldto_num,
					bar_custno, 
					demand_group
			from 	restate_base_case
		)
		select 	cte_base.soldto_num,
				cte_base.bar_custno, 
				cte_base.demand_group
		from 	cte_base
				left outer join exceptions_soldto as ex
					on 	lower(ex.soldto_num) = lower(cte_base.soldto_num) and 
						lower(ex.bar_custno) = lower(cte_base.bar_custno)
		where 	ex.soldto_num is null and 
				lower(cte_base.bar_custno) NOT IN ('customer_none', 'psd_oth') AND 
				lower(cte_base.bar_custno) NOT LIKE ('%_oth') 
	;
--select 	bar_custno
--from 	tmp_restate
--group by bar_custno
--order by 1
--
--select 	* 
--from 	restate_base_case
--where 	soldto_num IN ('0000000068','0001010009')
--;
--
--select 	* 
--from 	tmp_restate
--where 	soldto_num IN ('0000000068','0001010009')
--;
	/* ================================================================================
	 * 		BAR CUSTOMER HIERARCHY
	 * ================================================================================
	 */
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
				leaf as bar_customer_level11
		from 	cte_cust_current
		where 	level02 != 'CSToBeRemoved' AND 
				lower(leaf) NOT IN ('customer none', 'psd_oth') AND 
				lower(leaf) NOT LIKE ('%_oth')
	;
	/* ================================================================================
	 * 		COMMERCIAL HIERARCHY
	 * 			ref_data.customer_commercial_hierarchy
	 * 			(this is a flat file in S3)
	 * ================================================================================
	 */
--	select * from ref_data.customer_commercial_hierarchy order by 6,5,4,3,2;
	

	/* ================================================================================
	 * 		A1/A2 Hierarchy
	 * ================================================================================
	 */
--	drop table if exists tmp_A1_hierarchy
--	;
--	create temporary table tmp_A1_hierarchy
--	DISTSTYLE ALL
--	as
--		select 	HA1_id.soldto,
--				HA1_id.hierarchy_a1_id,
--				HA1_desc.hierarchy_a1_desc
--		from 	(
--					select 	distinct 
--							knvh.kunnr as soldto,
--							knvh.hkunnr as hierarchy_a1_id,
--							row_number() over (
--								partition by knvh.kunnr
--								order by knvh.datab desc 
--							) as rnk
--					from 	sapc11.knvh_current knvh
--					where 	knvh.HITYP='A' and 
--							knvh.DATBI = '99991231' and 
--							knvh.HVKORG = knvh.VKORG
--				) HA1_id
--				left outer join (
--					select 	distinct 
--							kna1.kunnr as hierarchy_a1_id,
--							kna1.name1 as hierarchy_a1_desc
--					from 	sapc11.kna1_current kna1
--				) HA1_desc
--					on 	HA1_desc.hierarchy_a1_id = HA1_id.hierarchy_a1_id
--		where	HA1_id.rnk = 1
--	;
--	insert into tmp_A1_hierarchy (soldto, hierarchy_a1_id, hierarchy_a1_desc)
--		select 	HA1_id.soldto,
--				HA1_id.hierarchy_a1_id,
--				HA1_desc.hierarchy_a1_desc
--		from 	(
--					select 	distinct 
--							knvh.kunnr as soldto,
--							knvh.hkunnr as hierarchy_a1_id,
--							row_number() over (
--								partition by knvh.kunnr
--								order by knvh.datab desc 
--							) as rnk
--					from 	sapp10.knvh_current knvh
--					where 	knvh.HITYP='A' and 
--							knvh.DATBI = '99991231' and 
--							knvh.HVKORG = knvh.VKORG
--				) HA1_id
--				left outer join (
--					select 	distinct 
--							kna1.kunnr as hierarchy_a1_id,
--							kna1.name1 as hierarchy_a1_desc
--					from 	sapp10.kna1_current kna1
--				) HA1_desc
--					on 	HA1_desc.hierarchy_a1_id = HA1_id.hierarchy_a1_id
--				left outer join tmp_A1_hierarchy mapping
--					on 	lower(mapping.soldto) = lower(HA1_id.soldto)
--		where	HA1_id.rnk = 1 and 
--				mapping.soldto is null
--	;
--	drop table if exists tmp_A2_hierarchy
--	;
--	create temporary table tmp_A2_hierarchy
--	DISTSTYLE ALL
--	as
--		select 	HA2_id.soldto,
--				HA2_id.hierarchy_a2_id,
--				HA2_desc.hierarchy_a2_desc
--		from 	(
--					select 	distinct 
--							knvh.kunnr as soldto,
--							knvh.hkunnr as hierarchy_a2_id,
--							row_number() over (
--								partition by knvh.kunnr
--								order by knvh.datab desc 
--							) as rnk
--					from 	sapc11.knvh_current knvh
--					where 	knvh.HITYP='A' and 
--							knvh.DATBI = '99991231' and 
--							knvh.HVKORG = knvh.VKORG
--				) HA2_id
--				left outer join (
--					select 	distinct 
--							kna1.kunnr as hierarchy_a2_id,
--							kna1.name1 as hierarchy_a2_desc
--					from 	sapc11.kna1_current kna1
--				) HA2_desc
--					on 	HA2_desc.hierarchy_a2_id = HA2_id.hierarchy_a2_id
--		where	HA2_id.rnk = 1
--	;
--	insert into tmp_A2_hierarchy (soldto, hierarchy_a2_id, hierarchy_a2_desc)
--		select 	HA2_id.soldto,
--				HA2_id.hierarchy_a2_id,
--				HA2_desc.hierarchy_a2_desc
--		from 	(
--					select 	distinct 
--							knvh.kunnr as soldto,
--							knvh.kunnr as hierarchy_a2_id,
--							row_number() over (
--								partition by knvh.kunnr
--								order by knvh.datab desc 
--							) as rnk
--					from 	sapp10.knvh_current knvh
--					where 	knvh.HITYP='A' and 
--							knvh.DATBI = '99991231' and 
--							knvh.HVKORG = knvh.VKORG
--				) HA2_id
--				left outer join (
--					select 	distinct 
--							kna1.kunnr as hierarchy_a2_id,
--							kna1.name1 as hierarchy_a2_desc
--					from 	sapp10.kna1_current kna1
--				) HA2_desc
--					on 	HA2_desc.hierarchy_a2_id = HA2_id.hierarchy_a2_id
--				left outer join tmp_A2_hierarchy mapping
--					on 	lower(mapping.soldto) = lower(HA2_id.soldto)
--		where	HA2_id.rnk = 1 and
--			 	mapping.soldto is null
--	;

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
					from 	sapc11.knvh_current knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA2_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a1_id,
							kna1.name1 as hierarchy_a1_desc
					from 	sapc11.kna1_current kna1
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
					from 	sapp10.knvh_current knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA2_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a1_id,
							kna1.name1 as hierarchy_a1_desc
					from 	sapp10.kna1_current kna1
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
					from 	sapc11.knvh_current knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA1_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a2_id,
							kna1.name1 as hierarchy_a2_desc
					from 	sapc11.kna1_current kna1
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
					from 	sapp10.knvh_current knvh
					where 	knvh.HITYP='A' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG
				) HA1_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_a2_id,
							kna1.name1 as hierarchy_a2_desc
					from 	sapp10.kna1_current kna1
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







	/* ================================================================================
	 * 		B Hierarchy
	 * ================================================================================
	 */
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
					from 	sapc11.knvh_current knvh
					where 	knvh.HITYP='B' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HB_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_b_id,
							kna1.name1 as hierarchy_b_desc
					from 	sapc11.kna1_current kna1
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
					from 	sapp10.knvh_current knvh
					where 	knvh.HITYP='B' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HB_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_b_id,
							kna1.name1 as hierarchy_b_desc
					from 	sapp10.kna1_current kna1
				) HB_desc
					on 	HB_desc.hierarchy_b_id = HB_id.hierarchy_b_id
				left outer join tmp_B_hierarchy mapping
					on 	lower(mapping.shipto) = lower(HB_id.shipto)
		where 	mapping.shipto is null
	;
	/* ================================================================================
	 * 		C Hierarchy
	 * ================================================================================
	 */
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
					from 	sapc11.knvh_current knvh
					where 	knvh.HITYP='C' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HC_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_c_id,
							kna1.name1 as hierarchy_c_desc
					from 	sapc11.kna1_current kna1
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
					from 	sapp10.knvh_current knvh
					where 	knvh.HITYP='C' and 
							knvh.DATBI = '99991231' and 
							knvh.HVKORG = knvh.VKORG and 
							knvh.VKORG IN ('0020', '0010')
				) HC_id
				left outer join (
					select 	distinct 
							kna1.kunnr as hierarchy_c_id,
							kna1.name1 as hierarchy_c_desc
					from 	sapp10.kna1_current kna1
				) HC_desc
					on 	HC_desc.hierarchy_c_id = HC_id.hierarchy_c_id
				left outer join tmp_C_hierarchy mapping
					on 	lower(mapping.soldto) = lower(HC_id.soldto)
		where 	mapping.soldto is null
	;

	/* ================================================================================
	 * 		Build Final Restatement Table
	 * ================================================================================
	 */
	drop table if exists tmp_restate_final
	;
	create temporary table tmp_restate_final
	DISTSTYLE ALL
	as
		select 	tmp.soldto_num as soldto_number,
				tmp.bar_custno as base_customer,
				
				bar_hier.bar_customer_desc as base_customer_desc,
				bar_hier.bar_customer_level01 as level01_BAR,
				bar_hier.bar_customer_level02 as level02_BAR,
				bar_hier.bar_customer_level03 as level03_BAR,
				bar_hier.bar_customer_level04 as level04_BAR,
				bar_hier.bar_customer_level05 as level05_BAR,
				bar_hier.bar_customer_level06 as level06_BAR,
				bar_hier.bar_customer_level07 as level07_BAR,
				bar_hier.bar_customer_level08 as level08_BAR,
				bar_hier.bar_customer_level09 as level09_BAR,
				bar_hier.bar_customer_level10 as level10_BAR,
				bar_hier.bar_customer_level11 as level11_BAR,
				
				tmp.demand_group,
				
				COALESCE( comm_hier.total_customer, 'unknown' ) as level01_commercial,
				COALESCE( comm_hier.segment, 'unknown' ) as level02_commercial,
				COALESCE( comm_hier.channel, 'unknown' ) as level03_commercial,
				COALESCE( comm_hier.market, 'unknown' ) as level04_commercial,
				COALESCE( comm_hier.major_customer, 'unknown' ) as level05_commercial,
				COALESCE( comm_hier.base_customer, 'unknown' ) as level06_commercial,
				
				COALESCE( a_hier.hierarchy_a2_id,'unknown') as a2,
				COALESCE( a_hier.hierarchy_a1_id,'unknown') as a1,
				COALESCE( a_hier.hierarchy_a2_desc,'unknown') as a2_desc,
				COALESCE( a_hier.hierarchy_a1_desc,'unknown') as a1_desc,
				
				/* these are keyed off of shipto */
--				COALESCE( b_hier.hierarchy_b_id, 'unknown' ) as hierarchy_b_id,
--				COALESCE( b_hier.hierarchy_b_desc, 'unknown' ) as hierarchy_b_desc,
				'unknown' as hierarchy_b_id,
				'unknown' as hierarchy_b_desc,
				
				COALESCE( c_hier.hierarchy_c_id, 'unknown' ) as hierarchy_c_id,
				COALESCE( c_hier.hierarchy_c_desc, 'unknown' ) as hierarchy_c_desc
		from 	tmp_restate as tmp
	
				left outer join tmp_customer_bar_hierarchy as bar_hier
					on 	lower(bar_hier.bar_custno) = lower(tmp.bar_custno)
					
				left outer join ref_data.customer_commercial_hierarchy as comm_hier
					on 	lower(comm_hier.base_customer) = lower(tmp.bar_custno)
				
--				left outer join tmp_A1_hierarchy as a1_hier
--					on 	lower(a1_hier.soldto) = lower(tmp.soldto_num)
--				left outer join tmp_A2_hierarchy as a2_hier
--					on 	lower(a2_hier.soldto) = lower(tmp.soldto_num)
				
				LEFT OUTER JOIN tmp_A_hierarchy	AS a_hier
					on lower(a_hier.soldto) = lower(tmp.soldto_num)
					
--				left outer join tmp_B_hierarchy as b_hier
--					on 	lower(b_hier.shipto) = lower(tmp.soldto)
				left outer join tmp_C_hierarchy as c_hier
					on 	lower(c_hier.soldto) = lower(tmp.soldto_num)
	;

/* DEBUG: counts IN = OUT */
--select count(*), count(distinct soldto_num) from tmp_restate;
--select count(*), count(distinct soldto_number) from tmp_restate_final;


	delete from dw.dim_customer_restatement;
	insert into dw.dim_customer_restatement (
				soldto_number,
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
				demand_group,
				a2,
				a1,
				a2_desc,
				a1_desc,
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
		select 	stg.soldto_number,
				stg.base_customer,
				stg.base_customer_desc,
				stg.level01_BAR,
				stg.level02_BAR,
				stg.level03_BAR,
				stg.level04_BAR,
				stg.level05_BAR,
				stg.level06_BAR,
				stg.level07_BAR,
				stg.level08_BAR,
				stg.level09_BAR,
				stg.level10_BAR,
				stg.level11_BAR,
				stg.demand_group,
				stg.a2,
				stg.a1,
				stg.a2_desc,
				stg.a1_desc,
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
		from 	tmp_restate_final as stg
	;
/* DEBUG: counts IN = OUT */
--select 	count(distinct soldto_number) from dw.dim_customer dc;
--select 	count(distinct soldto_number) from dw.dim_customer_restatement dc;

exception
when others then raise info 'exception occur while ingesting data in dim_customer_restatement';
end
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_dim_dataprocessing_outcome(flag_reload integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
-- call dw.p_build_dim_dataprocessing_outcome (2)
-- select * from dw.dim_dataprocessing_outcome
BEGIN  
	
	/* This table does not use identity based surrogate key, so does not need Insert / Update Strategy */
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_dataprocessing_outcome;
	end if;

	
	delete from dw.dim_dataprocessing_outcome;
	insert into dw.dim_dataprocessing_outcome (
				dataprocessing_outcome_key,
				dataprocessing_outcome_id,
				dataprocessing_phase,
				dataprocessing_outcome_desc,
				start_date,
				end_date,
				audit_loadts
		)
		values 
		( 0, 0,'phase 0' ,'Pass through',cast('1900-01-01' as date) , cast('9999-12-31' as date) ,cast(getdate() as timestamp)),  
		( 1, 1,'phase 1' ,'Allocated: SKU',cast('1900-01-01' as date) , cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		( 2, 1,'phase 2' ,'Allocated: One Level Up',cast('1900-01-01' as date) , cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		( 3, 1,'phase 3' ,'Allocated: FOB',cast('1900-01-01' as date) , cast('9999-12-31' as date) ,cast(getdate() as timestamp)),		
		( 4, 1,'phase 4' ,'Allocated: FOB Division',cast('1900-01-01' as date) , cast('9999-12-31' as date) ,cast(getdate() as timestamp)),		
		( 5, 1,'phase 5' ,'Allocated: Royalty',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),		
		( 6, 1,'phase 6' ,'Allocated: RSA',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),	
		( 9, 1,'phase 9' ,'Allocated: Two Levels Down (parent bar_product)',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(11, 1,'phase 11','Allocated: One level up and then 2 level down (parent bar_product)',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(100,2,'phase 100','Unallocated',cast('1900-01-01' as date), cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(101,1,'phase 101','Unallocated: Product_None, Customer_None',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(102,1,'phase 102','Unallocated: Service Customer and Products',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(103,2,'phase 103','Unallocated: ADJ_FOB',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(104,2,'phase 104','Unallocated: ADJ_FOB_NO_CUST',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(105,2,'phase 105','Unallocated: ADJ_RSA',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(20,1,'phase 20','Allocated: ADJ_INV_ADJ',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(27,1,'phase 90','Allocated: ADJ_INV_ADJ_GAP',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(21,1,'phase 21','Allocated: ADJ_WC',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(22,1,'phase 91','Allocated: ADJ_WC_GAP',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(23,1,'phase 24','Allocated: ADJ_PPV',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(24,1,'phase 94','Allocated: ADJ_PPV_GAP',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(25,1,'phase 25','Allocated: ADJ_LABOH_ADJ',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(26,1,'phase 95','Allocated: ADJ_LABOH_ADJ_GAP',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(28,1,'phase 23','Allocated: ADJ_FRGHT',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(29,1,'phase 93','Allocated: ADJ-FRGHT_GAP',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(30,1,'phase 22','Allocated: ADJ_DUTY',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(31,1,'phase 92','Allocated: ADJ_DUTY_GAP',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(32,1,'phase 34','Allocated: ADJ_PPV_UNMATCH',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(33,1,'phase 33','Allocated: ADJ_FRGHT_UNMATCH',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp)),
		(34,1,'phase 32','Allocated: ADJ_DUTY_UNMATCH',cast('1900-01-01' as date),  cast('9999-12-31' as date) ,cast(getdate() as timestamp));
	
	
	
	
exception
when others then raise info 'exception occur while ingesting data in dim_dataprocessing_outcome';
end;
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_dim_dataprocessing_rule(flag_reload integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN  
	/* This table does not use identity based surrogate key, so does not need Insert / Update Strategy */
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_dataprocessing_rule;
	end if;

	delete from dw.dim_dataprocessing_rule;
	INSERT INTO dw.dim_dataprocessing_rule (
				data_processing_ruleid, 
				dataprocessing_group,
				soldtoflag, 
				barcustflag, 
				skuflag, 
				barproductflag, 
				barbrandflag,
				dataprocessing_rule_description, 
				dataprocessing_rule_steps,
				audit_loadts
		)
		SELECT	dpr.data_processing_ruleid, 
				dpr.dataprocessing_group,
				dpr.soldtoflag, 
				dpr.barcustflag, 
				dpr.skuflag, 
				dpr.barproductflag, 
				dpr.barbrandflag,
				dpr.dataprocessing_rule_description, 
				dpr.dataprocessing_rule_steps,
				getdate() as audit_loadts
		from 	ref_data.data_processing_rule dpr
	;	
	exception
		when others then raise info 'exception occur while ingesting data in dim_dataprocessing_rule';
END;
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_dim_date(flag_reload integer)
 LANGUAGE plpgsql
AS $$ 
Begin 
	
	/* This table does not use identity based surrogate key, so does not need Insert / Update Strategy */
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_date;
	end if;
	

	delete from dw.dim_date;
	INSERT INTO dw.dim_date (
				dy_id,
				dy_dte,
				absolute_dy_nbr,
				absolute_wk_nbr,
				absolute_mth_nbr,
				absolute_qtr_nbr,
				dy_in_wk_nbr,
				dy_in_wk_name,
				julian_dy_nbr,
				clndr_wk_nbr,
				clndr_wk_id,
				clndr_mth_nbr,
				clndr_mth_id,
				clndr_mth_name,
				clndr_qtr_nbr,
				clndr_qtr_id,
				clndr_qtr_name,
				clndr_yr_id,
				clndr_dy_in_mth_nbr,
				is_first_dy_in_clndr_mth_flag,
				is_last_dy_in_clndr_mth_flag,
				wk_begin_dte,
				wk_end_dte,
				fmth_begin_dte,
				fmth_end_dte,
				fqtr_begin_dte,
				fqtr_end_dte,
				fyr_begin_dte,
				fyr_end_dte,
				fwk_nbr,
				fwk_id,
				fwk_cd,
				fmth_nbr,
				fmth_id,
				fmth_cd,
				fmth_name,
				fmth_short_name,
				fqtr_nbr,
				fqtr_id,
				fqtr_cd,
				fqtr_name,
				fyr_id,
				fdy_in_mth_nbr,
				fscl_days_remaining_in_mth,
				fdy_in_qtr_nbr,
				fscl_days_remaining_in_qtr,
				fdy_in_yr_nbr,
				fscl_days_remaining_in_yr,
				fwk_in_mth_nbr,
				fwk_in_qtr,
				is_wk_dy_flag,
				is_weekend_flag,
				is_first_dy_of_fwk_flag,
				is_last_dy_of_fwk_flag,
				is_first_dy_of_fmth_flag,
				is_last_dy_of_fmth_flag,
				is_first_dy_of_fqtr_flag,
				is_last_dy_of_fqtr_flag,
				is_first_dy_of_fyr_flag,
				is_last_dy_of_fyr_flag,
				season_name,
				holiday_name,
				holiday_season_name,
				holiday_observed_name,
				special_event_name
	
		)
		SELECT	dy_id,
				dy_dte,
				absolute_dy_nbr,
				absolute_wk_nbr,
				absolute_mth_nbr,
				absolute_qtr_nbr,
				dy_in_wk_nbr,
				dy_in_wk_name,
				julian_dy_nbr,
				clndr_wk_nbr,
				clndr_wk_id,
				clndr_mth_nbr,
				clndr_mth_id,
				clndr_mth_name,
				clndr_qtr_nbr,
				clndr_qtr_id,
				clndr_qtr_name,
				clndr_yr_id,
				clndr_dy_in_mth_nbr,
				is_first_dy_in_clndr_mth_flag,
				is_last_dy_in_clndr_mth_flag,
				wk_begin_dte,
				wk_end_dte,
				fmth_begin_dte,
				fmth_end_dte,
				fqtr_begin_dte,
				fqtr_end_dte,
				fyr_begin_dte,
				fyr_end_dte,
				fwk_nbr,
				fwk_id,
				fwk_cd,
				fmth_nbr,
				fmth_id,
				fmth_cd,
				fmth_name,
				fmth_short_name,
				fqtr_nbr,
				fqtr_id,
				fqtr_cd,
				fqtr_name,
				fyr_id,
				fdy_in_mth_nbr,
				fscl_days_remaining_in_mth,
				fdy_in_qtr_nbr,
				fscl_days_remaining_in_qtr,
				fdy_in_yr_nbr,
				fscl_days_remaining_in_yr,
				fwk_in_mth_nbr,
				fwk_in_qtr,
				is_wk_dy_flag,
				is_weekend_flag,
				is_first_dy_of_fwk_flag,
				is_last_dy_of_fwk_flag,
				is_first_dy_of_fmth_flag,
				is_last_dy_of_fmth_flag,
				is_first_dy_of_fqtr_flag,
				is_last_dy_of_fqtr_flag,
				is_first_dy_of_fyr_flag,
				is_last_dy_of_fyr_flag,
				season_name,
				holiday_name,
				holiday_season_name,
				holiday_observed_name,
				special_event_name
		FROM 	ref_data.calendar
	;

exception
when others then raise info 'exception occur while ingesting data in dim_date';
END 
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_dim_product(flag_reload integer)
 LANGUAGE plpgsql
AS $$
Begin
	/*
	 * 			call dw.p_build_dim_product(0) -- incremental
	 * 			call dw.p_build_dim_product(1) -- kill n fill
	 * 			select * from dw.dim_product where material like '%ADJ_ROYALTY%'
	 */
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_product;
	end if;
	
	/* current version of every material 
	 * 	all of spras E + Z (where E doesn't exists)
	 * 	for c11 & P10 & Lawson
	 * 
	 */
	drop table if exists tmp_material_master
	;
	create temporary table tmp_material_master as 
	with
		cte_mm_E as (
			select 	mm.matnr as material,
					mm.maktx as material_desc
			from 	sapc11.makt_current mm
			where 	mm.spras = 'E'
		)
		,cte_mm_Z as (
			select 	mm.matnr as material,
					mm.maktx as material_desc
			from 	sapc11.makt_current mm 
					left outer join cte_mm_E mm_E
						on	mm_E.material = mm.matnr
			where 	mm.spras = 'Z' and 
					mm_E.material is null 
		)
		,cte_c11 as (
			select 	mm_E.material,
					mm_E.material_desc
			from 	cte_mm_E mm_E
			union all
			select 	mm_Z.material,
					mm_Z.material_desc
			from 	cte_mm_Z mm_Z
		)
	select 	c11.material,
			c11.material_desc
	from 	cte_c11 as c11 	
	;
	insert into tmp_material_master (material, material_desc)
		with
			cte_mm_E as (
				select 	mm.matnr as material,
						mm.maktx as material_desc
				from 	sapp10.makt_current mm
				where 	mm.spras = 'E'
			)
			,cte_mm_Z as (
				select 	mm.matnr as material,
						mm.maktx as material_desc
				from 	sapp10.makt_current mm 
						left outer join cte_mm_E mm_E
							on	mm_E.material = mm.matnr
				where 	mm.spras = 'Z' and 
						mm_E.material is null 
			)
			,cte_p10 as (
				select 	mm_E.material,
						mm_E.material_desc
				from 	cte_mm_E mm_E
				union all
				select 	mm_Z.material,
						mm_Z.material_desc
				from 	cte_mm_Z mm_Z
			)
		select 	p10.material,
				p10.material_desc
		from 	cte_p10 as p10 
				left outer join tmp_material_master as mapping
					on 	lower(mapping.material) = lower(p10.material)
		where 	mapping.material is null
	;
	insert into tmp_material_master (material, material_desc)
		select 	lawson.prod_cd as material,
				MAX(lawson.prod_name) as material_desc
		from 	bods.extr_lawson_mac_prod_current as lawson
				left outer join tmp_material_master as mapping
					on 	lower(mapping.material) = lower(lawson.prod_cd)
		where 	lawson.div_cd = 'USM' and 
				mapping.material is NULL
		group by lawson.prod_cd
	;

	--05/01:shrikant k changes
	--cast columns to varchar50
	--add new combinations from rule 21 and 26
	/* trans-based mapping: material -> bar_brand / bar_product */
	drop table if exists tmp_trans_material_map
	;
	create temporary table tmp_trans_material_map as 
		select 	distinct 
				cast(lower(bcta.material) as varchar(50)) as material,
				cast(lower(bcta.mapped_bar_product) as varchar(50)) as bar_product,
				coalesce(cast(lower(bcta.mapped_bar_brand) as varchar(50)),'unknown') as bar_brand
		from 	stage.bods_core_transaction_agg bcta 
		where 	bcta.material is not null
		
	;
	insert into tmp_trans_material_map (material, bar_product, bar_brand)
		select 	distinct 
				lower(alloc.alloc_material) as material,
				lower(alloc.alloc_bar_product) as bar_product,
				coalesce(lower(alloc.mapped_bar_brand),'unknown') as bar_brand
		from 	stage.sgm_allocated_data_rule_09 alloc
				left outer join tmp_trans_material_map tr
					on 	lower(tr.material) = lower(alloc.alloc_material) and 
						lower(tr.bar_product) = lower(alloc.alloc_bar_product) and 
						coalesce(lower(tr.bar_brand),'unknown') = coalesce(lower(alloc.mapped_bar_brand),'unknown')
		where 	tr.material is null and 
				alloc.alloc_material is NOT NULL
		;
	insert into tmp_trans_material_map (material, bar_product, bar_brand)
		select 	distinct 
				lower(alloc.alloc_material) as material,
				lower(alloc.alloc_bar_product) as bar_product,
				coalesce(lower(alloc.mapped_bar_brand),'unknown') as bar_brand
		from 	stage.sgm_allocated_data_rule_21 alloc
				left outer join tmp_trans_material_map tr
					on 	lower(tr.material) = lower(alloc.alloc_material) and 
						lower(tr.bar_product) = lower(alloc.alloc_bar_product) and 
						coalesce(lower(tr.bar_brand),'unknown') = coalesce(lower(alloc.mapped_bar_brand),'unknown')
		where 	tr.material is null and 
				alloc.alloc_material is NOT NULL;
		
	insert into tmp_trans_material_map (material, bar_product, bar_brand)
		select 	distinct 
				lower(alloc.alloc_material) as material,
				lower(alloc.alloc_bar_product) as bar_product,
				coalesce(lower(alloc.mapped_bar_brand),'unknown') as bar_brand
		from 	stage.sgm_allocated_data_rule_26 alloc
				left outer join tmp_trans_material_map tr
					on 	lower(tr.material) = lower(alloc.alloc_material) and 
						lower(tr.bar_product) = lower(alloc.alloc_bar_product) and 
						coalesce(lower(tr.bar_brand),'unknown') = coalesce(lower(alloc.mapped_bar_brand),'unknown')
		where 	tr.material is null and 
				alloc.alloc_material is NOT NULL
	;	
	
	/* special members for Allocation Exception Royalty (A40910) */
	drop table if exists tmp_allocation_exception_royalty_material_map
	;
	create temporary table tmp_allocation_exception_royalty_material_map as 
		select 	distinct 
				'ADJ_ROYALTY' as material,
				lower(bcta.mapped_bar_product) as bar_product,
				coalesce(lower(bcta.mapped_bar_brand),'unknown') as bar_brand
		from 	stage.bods_core_transaction_agg bcta 
		where 	bcta.bar_acct = 'A40910'
	;
			
	/* special members for Allocation Exception CUSTOMER_NONE, PRODUCT_NONE (Rule 27) */
	drop table if exists tmp_allocation_exception_rule27_material_map
	;
	create temporary table tmp_allocation_exception_rule27_material_map as 
		select 	distinct 
				'ADJ_NO_PROD' as material,
				lower(alloc.mapped_bar_product) as bar_product,
				coalesce(lower(alloc.mapped_bar_brand),'unknown') as bar_brand
		from 	stage.sgm_allocated_data_rule_27 alloc
	;
			
	/* special members for Allocation Exception SERVICE (Rule 28) */
	drop table if exists tmp_allocation_exception_rule28_material_map
	;
	create temporary table tmp_allocation_exception_rule28_material_map as 
		select 	distinct 
				'ADJ_SERVICE' as material,
				lower(alloc.mapped_bar_product) as bar_product,
				coalesce(lower(alloc.mapped_bar_brand),'unknown') as bar_brand
		from 	stage.sgm_allocated_data_rule_28 alloc
		union all 
		select 	distinct 
				'ADJ_REBUILD' as material,
				lower(alloc.mapped_bar_product) as bar_product,
				coalesce(lower(alloc.mapped_bar_brand),'unknown') as bar_brand
		from 	stage.sgm_allocated_data_rule_28 alloc
	;
	/* special members for Allocation Exception RSA (reconcile) */
	drop table if exists tmp_allocation_exception_rsa_reconcile_material_map
	;
	create temporary table tmp_allocation_exception_rsa_reconcile_material_map as 
		Select	distinct
				'ADJ_RSA' as material,
				'ADJ_RSA' as bar_product,
				'N/A' as bar_brand,
				lower(coalesce(stg.rsa_reconcile_bar_division,'unknown')) as rsa_division,
				'ADJ_RSA' as bar_product_level08_category,
				'ADJ_RSA' as bar_product_level09_portfolio
		from 	stage.sgm_allocated_data_rule_23 stg
		where	stg.source_system = 'rsa_bible' and 
				stg.dataprocessing_outcome_id = 2
	;
	/* special members for Allocation Exception RSA (alloc) */
	insert into tmp_trans_material_map (material, bar_product, bar_brand)
		Select	distinct
				lower(alloc.alloc_material) as material,
				lower(alloc.alloc_bar_product) as bar_product,
				lower(alloc.mapped_bar_brand) as bar_brand
		from 	stage.sgm_allocated_data_rule_23 alloc
				left outer join tmp_trans_material_map tr
					on 	lower(tr.material) = lower(alloc.alloc_material) and 
						lower(tr.bar_product) = lower(alloc.alloc_bar_product) and 
						lower(tr.bar_brand) = lower(alloc.mapped_bar_brand) 
		where 	alloc.source_system = 'rsa_bible' and 
				alloc.dataprocessing_outcome_id != 2 and 
				tr.material is NULL and 
				alloc.alloc_material is NOT NULL
	;
		
	/* current version of bar_product hierarchy */
	drop table if exists tmp_bar_product_hierarchy
	;
	create temporary table tmp_bar_product_hierarchy as 
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
			where 	loaddts = ( select max(loaddts) from bods.drm_product_current dpc )
				and membertype != 'Parent'
		)
		select 	bar_product,
				bar_product_desc,
				membertype,
				portfolio,
				portfolio_desc,
				generation,
				level01_bar,
				case when generation <= 2  then case when bar_product = 'Product_None' then bar_product else parent end else level02_bar end as level02_bar,
				case when generation <= 3  then case when bar_product = 'Product_None' then bar_product else parent end else level03_bar end as level03_bar,
				case when generation <= 4  then case when bar_product = 'Product_None' then bar_product else parent end else level04_bar end as level04_bar,
				case when generation <= 5  then case when bar_product = 'Product_None' then bar_product else parent end else level05_bar end as level05_bar,
				case when generation <= 6  then case when bar_product = 'Product_None' then bar_product else parent end else level06_bar end as level06_bar,
				case when generation <= 7  then case when bar_product = 'Product_None' then bar_product else parent end else level07_bar end as level07_bar,
				case when generation <= 8  then case when bar_product = 'Product_None' then bar_product else parent end else level08_bar end as level08_bar,
				bar_product as level09_bar
		from 	cte_base 
	;
	/* current version of commercial product hierarchy */
	drop table if exists tmp_bar_gpp_commercial_hierarchy
	;
	create temporary table tmp_bar_gpp_commercial_hierarchy as 
		select 	gpp.material,
				gpp.gpp_portfolio,
				gpp.gpp_division as gpp_division_code,
				hier.gts as prd_comm_level01_gts,
				hier.super_bu as prd_comm_level02_super_bu,
				hier.subcategory as prd_comm_level03_subcategory,
				hier.category as prd_comm_level04_category,
				gpp.gpp_portfolio as prd_comm_level05_gpp_portfolio
		from 	ref_data.sku_gpp_mapping gpp
				inner join ref_data.product_commercial_hierarchy hier
					ON lower(CONCAT('P', hier.gpp_portfolio)) = lower(gpp.gpp_portfolio)
	;
	/* current version of bar_product hierarchy */
	drop table if exists tmp_bar_product_hierarchy_rsa_reconcile
	;
	create temporary table tmp_bar_product_hierarchy_rsa_reconcile as
		select  distinct 
				bph.level01_bar,
				bph.level02_bar,
				bph.level03_bar,
				bph.level04_bar,
				bph.level05_bar,
				bph.level06_bar,
				bph.level07_bar
		from 	tmp_bar_product_hierarchy bph
				inner join tmp_allocation_exception_rsa_reconcile_material_map rsa
					on 	lower(rsa.rsa_division) = lower(bph.level07_bar)
	;

	/* ------------------------------------------------------------------ 
	 * 	Part 02: Create stage table to build from scratch
	 * ------------------------------------------------------------------
	 */
	DROP TABLE IF EXISTS stage_dim_product;
	CREATE TEMPORARY TABLE stage_dim_product (
		product_id varchar(200) NULL,
		material varchar(100) NULL,
		bar_product varchar(100) NULL,
		product_brand varchar(100) NULL,
		
		sku varchar(200) NULL,
		
		portfolio varchar(100) NULL,
		portfolio_desc varchar(200) NULL,
		
		member_type varchar(50) NULL,
		generation int4 NULL,
		level01_bar varchar(100) NULL,
		level02_bar varchar(100) NULL,
		level03_bar varchar(100) NULL,
		level04_bar varchar(100) NULL,
		level05_bar varchar(100) NULL,
		level06_bar varchar(100) NULL,
		level07_bar varchar(100) NULL,
		level08_bar varchar(100) NULL,
		level09_bar varchar(100) NULL,
		start_date date NOT NULL,
		end_date date NOT NULL,
		audit_loadts timestamp NOT NULL,
		
		gpp_division_code varchar(10) NULL,
		prd_comm_level01_gts varchar(100) NULL,
		prd_comm_level02_super_bu varchar(100) NULL,
		prd_comm_level03_subcategory varchar(100) NULL,
		prd_comm_level04_category varchar(100) NULL,
		prd_comm_level05_gpp_portfolio varchar(100) NULL
	) DISTSTYLE ALL 
	;
	/* ------------------------------------------------------------------ 
	 * 	Part 03: Load stage table
	 * ------------------------------------------------------------------
	 */
	insert into stage_dim_product (
				product_id, 
				material,
				bar_product,
				product_brand,
				
				sku,
				
				portfolio, 
				portfolio_desc,
				
				member_type,  
				generation,
				level01_bar,
				level02_bar,
				level03_bar,
				level04_bar,
				level05_bar,
				level06_bar,
				level07_bar,
				level08_bar,
				level09_bar,
				start_date, 
				end_date, 
				audit_loadts,
		
				gpp_division_code,
				prd_comm_level01_gts,
				prd_comm_level02_super_bu,
				prd_comm_level03_subcategory,
				prd_comm_level04_category,
				prd_comm_level05_gpp_portfolio
		)
		select  'unknown|unknown|unknown' as product_id,
				'unknown' as material,
			    'unknown' as bar_product,
			    'unknown' as product_brand,
			    
			    'unknown' as sku,
			    
			    'unknown' as portfolio, 
			    'unknown' as portfolio_desc,
			    
			    'unknown' as member_type,
				cast(null as int) as generation,
				'unknown' as level01_bar,
				'unknown' as level02_bar,
				'unknown' as level03_bar,
				'unknown' as level04_bar,
				'unknown' as level05_bar,
				'unknown' as level06_bar,
				'unknown' as level07_bar,
				'unknown' as level08_bar,
				'unknown' as level09_bar,
			    cast('01-01-1900' as date) start_date,
	  			cast('12-31-9999' as date) as end_date,
	  			getdate() as audit_loadts,
		
				NULL as gpp_division_code,
				'unknown' as prd_comm_level01_gts,
				'unknown' as prd_comm_level02_super_bu,
				'unknown' as prd_comm_level03_subcategory,
				'unknown' as prd_comm_level04_category,
				'unknown' as prd_comm_level05_gpp_portfolio
	  	union all 
		select 	prd_map.material 
				|| '|' || coalesce(prd_map.bar_product,'unknown') 
				|| '|' || coalesce(prd_map.bar_brand,'unknown') as product_id,
				
				prd_map.material as material,
				coalesce(prd_map.bar_product,'unknown') as bar_product,
				coalesce(prd_map.bar_brand,'unknown') as product_brand,
				
				mm.material_desc as sku,
				
				case when hier.bar_product is null then 'unknown' else hier.portfolio       end as portfolio,
				case when hier.bar_product is null then 'unknown' else hier.portfolio_desc  end as portfolio_desc,
				
				case when hier.bar_product is null then 'unknown' else hier.membertype      end as membertype,
				hier.generation,
				case when hier.level01_bar is null then 'unknown' else hier.level01_bar end as level01_bar,
				case when hier.level02_bar is null then 'unknown' else hier.level02_bar end as level02_bar,
				case when hier.level03_bar is null then 'unknown' else hier.level03_bar end as level03_bar,
				case when hier.level04_bar is null then 'unknown' else hier.level04_bar end as level04_bar,
				case when hier.level05_bar is null then 'unknown' else hier.level05_bar end as level05_bar,
				case when hier.level06_bar is null then 'unknown' else hier.level06_bar end as level06_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level07_bar,
				case when hier.level08_bar is null then 'unknown' else hier.level08_bar end as level08_bar,
				case when hier.level09_bar is null then 'unknown' else hier.level09_bar end as level09_bar,
			    cast('01-01-1900' as date) start_date,
	  			cast('12-31-9999' as date) as end_date,
	  			getdate() as audit_loadts,
		
				COALESCE( comm_hier.gpp_division_code, 'unknown') as gpp_division_code,
				COALESCE( comm_hier.prd_comm_level01_gts, 'unknown') as prd_comm_level01_gts,
				COALESCE( comm_hier.prd_comm_level02_super_bu, 'unknown') as prd_comm_level02_super_bu,
				COALESCE( comm_hier.prd_comm_level03_subcategory, 'unknown') as prd_comm_level03_subcategory,
				COALESCE( comm_hier.prd_comm_level04_category, 'unknown') as prd_comm_level04_category,
				COALESCE( comm_hier.prd_comm_level05_gpp_portfolio, 'unknown') as prd_comm_level05_gpp_portfolio
	  			
		from 	tmp_trans_material_map prd_map
				left outer join tmp_material_master mm
					on 	lower(mm.material) = lower(prd_map.material)
				left outer join tmp_bar_product_hierarchy as hier
					on 	lower(hier.bar_product) = lower(prd_map.bar_product)
				left outer join tmp_bar_gpp_commercial_hierarchy comm_hier
					on 	lower(comm_hier.material) = lower(mm.material)
		;
	

	/* allocation exception Royalty */
	insert into stage_dim_product (
				product_id, 
				material,
				bar_product,
				product_brand,
				
				sku,
				
				portfolio, 
				portfolio_desc,
				
				member_type,  
				generation,
				level01_bar,
				level02_bar,
				level03_bar,
				level04_bar,
				level05_bar,
				level06_bar,
				level07_bar,
				level08_bar,
				level09_bar,
				start_date, 
				end_date, 
				audit_loadts,
		
				gpp_division_code,
				prd_comm_level01_gts,
				prd_comm_level02_super_bu,
				prd_comm_level03_subcategory,
				prd_comm_level04_category,
				prd_comm_level05_gpp_portfolio
		)
		select 	prd_map.material || '|' || prd_map.bar_product || '|' || prd_map.bar_brand as product_id,
				
				prd_map.material,
				prd_map.bar_product,
				prd_map.bar_brand as product_brand,
				
				prd_map.material as sku,
				
				case when hier.bar_product is null then 'unknown' else hier.portfolio       end as portfolio,
				case when hier.bar_product is null then 'unknown' else hier.portfolio_desc  end as portfolio_desc,
				
				case when hier.bar_product is null then 'unknown' else hier.membertype      end as membertype,
				hier.generation,
				case when hier.level01_bar is null then 'unknown' else hier.level01_bar end as level01_bar,
				case when hier.level02_bar is null then 'unknown' else hier.level02_bar end as level02_bar,
				case when hier.level03_bar is null then 'unknown' else hier.level03_bar end as level03_bar,
				case when hier.level04_bar is null then 'unknown' else hier.level04_bar end as level04_bar,
				case when hier.level05_bar is null then 'unknown' else hier.level05_bar end as level05_bar,
				case when hier.level06_bar is null then 'unknown' else hier.level06_bar end as level06_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level07_bar,
				case when hier.level08_bar is null then 'unknown' else hier.level08_bar end as level08_bar,
				case when hier.level09_bar is null then 'unknown' else hier.level09_bar end as level09_bar,
			    cast('01-01-1900' as date) start_date,
	  			cast('12-31-9999' as date) as end_date,
	  			getdate() as audit_loadts,
		
				'unknown' as gpp_division_code,
				'unknown' as prd_comm_level01_gts,
				'unknown' as prd_comm_level02_super_bu,
				'unknown' as prd_comm_level03_subcategory,
				'unknown' as prd_comm_level04_category,
				'unknown' as prd_comm_level05_gpp_portfolio
	  			
		from 	tmp_allocation_exception_royalty_material_map prd_map
				left outer join tmp_bar_product_hierarchy as hier
					on 	lower(hier.bar_product) = lower(prd_map.bar_product)
	;
	
	/* allocation exception PRODUCT_NONE / CUSTOMER_NONE (Rule 27) */
	insert into stage_dim_product (
				product_id, 
				material,
				bar_product,
				product_brand,
				
				sku,
				
				portfolio, 
				portfolio_desc,
				
				member_type,  
				generation,
				level01_bar,
				level02_bar,
				level03_bar,
				level04_bar,
				level05_bar,
				level06_bar,
				level07_bar,
				level08_bar,
				level09_bar,
				start_date, 
				end_date, 
				audit_loadts,
		
				gpp_division_code,
				prd_comm_level01_gts,
				prd_comm_level02_super_bu,
				prd_comm_level03_subcategory,
				prd_comm_level04_category,
				prd_comm_level05_gpp_portfolio
		)
		select 	prd_map.material || '|' || prd_map.bar_product || '|' || prd_map.bar_brand as product_id,
				
				prd_map.material,
				prd_map.bar_product,
				prd_map.bar_brand as product_brand,
				
				prd_map.material as sku,
				
				case when hier.bar_product is null then 'unknown' else hier.portfolio       end as portfolio,
				case when hier.bar_product is null then 'unknown' else hier.portfolio_desc  end as portfolio_desc,
				
				case when hier.bar_product is null then 'unknown' else hier.membertype      end as membertype,
				hier.generation,
				case when hier.level01_bar is null then 'unknown' else hier.level01_bar end as level01_bar,
				case when hier.level02_bar is null then 'unknown' else hier.level02_bar end as level02_bar,
				case when hier.level03_bar is null then 'unknown' else hier.level03_bar end as level03_bar,
				case when hier.level04_bar is null then 'unknown' else hier.level04_bar end as level04_bar,
				case when hier.level05_bar is null then 'unknown' else hier.level05_bar end as level05_bar,
				case when hier.level06_bar is null then 'unknown' else hier.level06_bar end as level06_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level07_bar,
				case when hier.level08_bar is null then 'unknown' else hier.level08_bar end as level08_bar,
				case when hier.level09_bar is null then 'unknown' else hier.level09_bar end as level09_bar,
			    cast('01-01-1900' as date) start_date,
	  			cast('12-31-9999' as date) as end_date,
	  			getdate() as audit_loadts,
		
				'unknown' as gpp_division_code,
				'unknown' as prd_comm_level01_gts,
				'unknown' as prd_comm_level02_super_bu,
				'unknown' as prd_comm_level03_subcategory,
				'unknown' as prd_comm_level04_category,
				'unknown' as prd_comm_level05_gpp_portfolio
	  			
		from 	tmp_allocation_exception_rule27_material_map as prd_map
				left outer join tmp_bar_product_hierarchy as hier
					on 	lower(hier.bar_product) = lower(prd_map.bar_product)
	;
	
	/* allocation exception SERVICE (Rule 28) */
	insert into stage_dim_product (
				product_id, 
				material,
				bar_product,
				product_brand,
				
				sku,
				
				portfolio, 
				portfolio_desc,
				
				member_type,  
				generation,
				level01_bar,
				level02_bar,
				level03_bar,
				level04_bar,
				level05_bar,
				level06_bar,
				level07_bar,
				level08_bar,
				level09_bar,
				start_date, 
				end_date, 
				audit_loadts,
		
				gpp_division_code,
				prd_comm_level01_gts,
				prd_comm_level02_super_bu,
				prd_comm_level03_subcategory,
				prd_comm_level04_category,
				prd_comm_level05_gpp_portfolio
		)
		select 	prd_map.material || '|' || prd_map.bar_product || '|' || prd_map.bar_brand as product_id,
				
				prd_map.material,
				prd_map.bar_product,
				prd_map.bar_brand as product_brand,
				
				prd_map.material as sku,
				
				case when hier.bar_product is null then 'unknown' else hier.portfolio       end as portfolio,
				case when hier.bar_product is null then 'unknown' else hier.portfolio_desc  end as portfolio_desc,
				
				case when hier.bar_product is null then 'unknown' else hier.membertype      end as membertype,
				hier.generation,
				case when hier.level01_bar is null then 'unknown' else hier.level01_bar end as level01_bar,
				case when hier.level02_bar is null then 'unknown' else hier.level02_bar end as level02_bar,
				case when hier.level03_bar is null then 'unknown' else hier.level03_bar end as level03_bar,
				case when hier.level04_bar is null then 'unknown' else hier.level04_bar end as level04_bar,
				case when hier.level05_bar is null then 'unknown' else hier.level05_bar end as level05_bar,
				case when hier.level06_bar is null then 'unknown' else hier.level06_bar end as level06_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level07_bar,
				case when hier.level08_bar is null then 'unknown' else hier.level08_bar end as level08_bar,
				case when hier.level09_bar is null then 'unknown' else hier.level09_bar end as level09_bar,
			    cast('01-01-1900' as date) start_date,
	  			cast('12-31-9999' as date) as end_date,
	  			getdate() as audit_loadts,
		
				'unknown' as gpp_division_code,
				'unknown' as prd_comm_level01_gts,
				'unknown' as prd_comm_level02_super_bu,
				'unknown' as prd_comm_level03_subcategory,
				'unknown' as prd_comm_level04_category,
				'unknown' as prd_comm_level05_gpp_portfolio
	  			
		from 	tmp_allocation_exception_rule28_material_map as prd_map
				left outer join tmp_bar_product_hierarchy as hier
					on 	lower(hier.bar_product) = lower(prd_map.bar_product)
	;

	/* allocation exception RSA */
	insert into stage_dim_product (
				product_id, 
				material,
				bar_product,
				product_brand,
				
				sku,
				
				portfolio, 
				portfolio_desc,
				
				member_type,  
				generation,
				level01_bar,
				level02_bar,
				level03_bar,
				level04_bar,
				level05_bar,
				level06_bar,
				level07_bar,
				level08_bar,
				level09_bar,
				start_date, 
				end_date, 
				audit_loadts,
		
				gpp_division_code,
				prd_comm_level01_gts,
				prd_comm_level02_super_bu,
				prd_comm_level03_subcategory,
				prd_comm_level04_category,
				prd_comm_level05_gpp_portfolio
		)
		select 	prd_map.material || '|' || 
					prd_map.rsa_division || '|' || 
					prd_map.bar_brand
					as product_id,
				
				prd_map.material as material,
				prd_map.bar_product as bar_product,
				prd_map.bar_brand as product_brand,
				
				prd_map.material as sku,
				
				prd_map.bar_product as portfolio,
				prd_map.bar_product as portfolio_desc,
				
				null as membertype,
				null generation,
				case when hier.level01_bar is null then 'unknown' else hier.level01_bar end as level01_bar,
				case when hier.level02_bar is null then 'unknown' else hier.level02_bar end as level02_bar,
				case when hier.level03_bar is null then 'unknown' else hier.level03_bar end as level03_bar,
				case when hier.level04_bar is null then 'unknown' else hier.level04_bar end as level04_bar,
				case when hier.level05_bar is null then 'unknown' else hier.level05_bar end as level05_bar,
				case when hier.level06_bar is null then 'unknown' else hier.level06_bar end as level06_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level07_bar,
--				prd_map.bar_product_level08_category as level08_bar,
--				prd_map.bar_product_level09_portfolio as level09_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level08_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level09_bar,
			    cast('01-01-1900' as date) start_date,
	  			cast('12-31-9999' as date) as end_date,
	  			getdate() as audit_loadts,
		
				'unknown' as gpp_division_code,
				'unknown' as prd_comm_level01_gts,
				'unknown' as prd_comm_level02_super_bu,
				'unknown' as prd_comm_level03_subcategory,
				'unknown' as prd_comm_level04_category,
				'unknown' as prd_comm_level05_gpp_portfolio
	  			
		from 	tmp_allocation_exception_rsa_reconcile_material_map prd_map
				left outer join tmp_bar_product_hierarchy_rsa_reconcile as hier
					on 	lower(hier.level07_bar) = lower(prd_map.rsa_division)
				left outer join stage_dim_product stg
					on 	lower(stg.material) = lower(prd_map.material) and 
						lower(stg.bar_product) = lower(prd_map.bar_product) and
						lower(stg.product_brand) = lower(prd_map.bar_brand)
		where 	stg.material is null
	;

	/* ------------------------------------------------------------------ 
	 * 	Part 04: Create placeholder rows for records in hierarchy 
	 *		that don't exist in any transactions that have been
	 *		processed.
	 *
	 *	2021-04-29 BA: crossjoin w/ brand
	 * ------------------------------------------------------------------
	 */
	insert into stage_dim_product (
				product_id, 
				material,
				bar_product,
				product_brand,
				
				sku,
				
				portfolio, 
				portfolio_desc,
				
				member_type,  
				generation,
				level01_bar,
				level02_bar,
				level03_bar,
				level04_bar,
				level05_bar,
				level06_bar,
				level07_bar,
				level08_bar,
				level09_bar,
				start_date, 
				end_date, 
				audit_loadts,
		
				gpp_division_code,
				prd_comm_level01_gts,
				prd_comm_level02_super_bu,
				prd_comm_level03_subcategory,
				prd_comm_level04_category,
				prd_comm_level05_gpp_portfolio
		)
		select 	'BA&R placeholder|' || hier.bar_product || '|' || brand.mapped_bar_brand as product_id,
				'BA&R placeholder' as material,
				hier.bar_product as bar_product,
				brand.mapped_bar_brand as product_brand,
				
				'BA&R placeholder' as sku,
				
				case when hier.bar_product is null then 'unknown' else hier.portfolio       end as portfolio,
				case when hier.bar_product is null then 'unknown' else hier.portfolio_desc  end as portfolio_desc,
				
				case when hier.bar_product is null then 'unknown' else hier.membertype      end as membertype,
				hier.generation,
				case when hier.level01_bar is null then 'unknown' else hier.level01_bar end as level01_bar,
				case when hier.level02_bar is null then 'unknown' else hier.level02_bar end as level02_bar,
				case when hier.level03_bar is null then 'unknown' else hier.level03_bar end as level03_bar,
				case when hier.level04_bar is null then 'unknown' else hier.level04_bar end as level04_bar,
				case when hier.level05_bar is null then 'unknown' else hier.level05_bar end as level05_bar,
				case when hier.level06_bar is null then 'unknown' else hier.level06_bar end as level06_bar,
				case when hier.level07_bar is null then 'unknown' else hier.level07_bar end as level07_bar,
				case when hier.level08_bar is null then 'unknown' else hier.level08_bar end as level08_bar,
				case when hier.level09_bar is null then 'unknown' else hier.level09_bar end as level09_bar,
			    cast('01-01-1900' as date) start_date,
	  			cast('12-31-9999' as date) as end_date,
	  			getdate() as audit_loadts,
		
				NULL as gpp_division_code,
				'unknown' as prd_comm_level01_gts,
				'unknown' as prd_comm_level02_super_bu,
				'unknown' as prd_comm_level03_subcategory,
				'unknown' as prd_comm_level04_category,
				'unknown' as prd_comm_level05_gpp_portfolio
		from 	tmp_bar_product_hierarchy hier
				cross join (
					select  distinct lower(bta.mapped_bar_brand) as mapped_bar_brand
					from 	stage.bods_core_transaction_agg bta
					where 	bta.mapped_bar_brand is not null
					union all
					select 	'BA&R placeholder' as mapped_bar_brand
				) brand
				left outer join dw.dim_product dp
					on 	lower(dp.level01_bar) = lower(case when hier.level01_bar is null then 'unknown' else hier.level01_bar end) and 
						lower(dp.level02_bar) = lower(case when hier.level02_bar is null then 'unknown' else hier.level02_bar end) and 
						lower(dp.level03_bar) = lower(case when hier.level03_bar is null then 'unknown' else hier.level03_bar end) and 
						lower(dp.level04_bar) = lower(case when hier.level04_bar is null then 'unknown' else hier.level04_bar end) and 
						lower(dp.level05_bar) = lower(case when hier.level05_bar is null then 'unknown' else hier.level05_bar end) and  
						lower(dp.level06_bar) = lower(case when hier.level06_bar is null then 'unknown' else hier.level06_bar end) and 
						lower(dp.level07_bar) = lower(case when hier.level07_bar is null then 'unknown' else hier.level07_bar end) and 
						lower(dp.level08_bar) = lower(case when hier.level08_bar is null then 'unknown' else hier.level08_bar end) and 
						lower(dp.level09_bar) = lower(case when hier.level09_bar is null then 'unknown' else hier.level09_bar end)
		where 	dp.level09_bar is null
	;

	/* ------------------------------------------------------------------ 
	 * 	Part 05: Update existing rows in target table
	 *		TODO: add hash check to skip rows w/ no changes
	 * ------------------------------------------------------------------
	 */
	UPDATE 	dw.dim_product
	SET 	sku = stg.sku,
			
			portfolio = stg.portfolio, 
			portfolio_desc = stg.portfolio_desc, 
			
			member_type = stg.member_type, 
			generation = stg.generation,
			level01_bar = stg.level01_bar,
			level02_bar = stg.level02_bar,
			level03_bar = stg.level03_bar,
			level04_bar = stg.level04_bar,
			level05_bar = stg.level05_bar,
			level06_bar = stg.level06_bar,
			level07_bar = stg.level07_bar,
			level08_bar = stg.level08_bar,
			level09_bar = stg.level09_bar,
			start_date = stg.start_date,
			end_date = stg.end_date,
			audit_loadts = stg.audit_loadts,
		
			gpp_division_code = stg.gpp_division_code,
			prd_comm_level01_gts = stg.prd_comm_level01_gts,
			prd_comm_level02_super_bu = stg.prd_comm_level02_super_bu,
			prd_comm_level03_subcategory = stg.prd_comm_level03_subcategory,
			prd_comm_level04_category = stg.prd_comm_level04_category,
			prd_comm_level05_gpp_portfolio = stg.prd_comm_level05_gpp_portfolio
			
	FROM 	stage_dim_product stg 
	WHERE 	lower(stg.product_id) = lower(dim_product.product_id)
		AND lower(dim_product.material) != lower('BA&R placeholder')
	;
	/* ------------------------------------------------------------------ 
	 * 	Part 06: Update existing 'placeholder' rows in target table
	 * 		that now have a product id, sku_name, and/or brand
	 * ------------------------------------------------------------------
	 */
  
	/* ------------------------------------------------------------------ 
	 * 	Part 07: Insert any new rows
	 * ------------------------------------------------------------------
	 */
	insert into dw.dim_product (
				product_id, 
				material,
				bar_product,
				product_brand,
				
				sku,
				portfolio, 
				portfolio_desc,
				member_type,  
				generation,
				level01_bar,
				level02_bar,
				level03_bar,
				level04_bar,
				level05_bar,
				level06_bar,
				level07_bar,
				level08_bar,
				level09_bar,
				start_date, 
				end_date, 
				audit_loadts,
		
				gpp_division_code,
				prd_comm_level01_gts,
				prd_comm_level02_super_bu,
				prd_comm_level03_subcategory,
				prd_comm_level04_category,
				prd_comm_level05_gpp_portfolio
		)
		select 	stg.product_id, 
				UPPER(stg.material) as material,
				stg.bar_product,
				UPPER(stg.product_brand) as product_brand,
				
				stg.sku,
				stg.portfolio, 
				stg.portfolio_desc,
				stg.member_type,  
				stg.generation,
				stg.level01_bar,
				stg.level02_bar,
				stg.level03_bar,
				stg.level04_bar,
				stg.level05_bar,
				stg.level06_bar,
				stg.level07_bar,
				stg.level08_bar,
				stg.level09_bar,
				stg.start_date, 
				stg.end_date, 
				stg.audit_loadts,
		
				stg.gpp_division_code,
				stg.prd_comm_level01_gts,
				stg.prd_comm_level02_super_bu,
				stg.prd_comm_level03_subcategory,
				stg.prd_comm_level04_category,
				stg.prd_comm_level05_gpp_portfolio
		from 	stage_dim_product stg
				left outer join dw.dim_product dp
					on 	lower(stg.product_id) = lower(dp.product_id)
		where	dp.product_id is null
	;
exception
when others then raise info 'exception occur while ingesting data in dim_prod';
END
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_dim_product_restatement()
 LANGUAGE plpgsql
AS $$
Begin
	
	/*
	 * 		call dw.p_build_dim_product_restatement ();
	 * 		grant all on procedure dw.p_build_dim_product_restatement() to group "g-ada-rsabible-sb-ro";
	 * 		select count(*) from dw.dim_product_restatement;
	 * 
	 * 
	 */
	
	/************************************************************************************************
	 * STEPS : This process is full load for each run
	 * 1. Get All distinct allocated materials from fact sgm exluding exception rules
	 * 2. Map material to GPP using mara table - ref_data.sku_gpp_mapping : based off mara_current
	 * 3. Anything left over - will be mapped to sequence below
	 * 		ref_data.sku_barproduct_mapping_c11_bods
	 * 		ref_data.sku_barproduct_mapping_p10_bods
	 * 		ref_data.sku_barproduct_mapping_lawson_bods
	 * 4. validate it provides 100% coverage
	 * 5. map the material, bar_product to bods_drm_product table to get rest of hierarchy details
	 * 6. load the table
	 * 
	 */
	
	drop table if exists stage_material_to_map; 
	
	create temporary table stage_material_to_map
	diststyle all 
	as 
	select 	distinct 
			fpcs.alloc_material as material,
			fpcs.mapped_bar_product as bar_product,
			fpcs.source_system_id 
	from 	dw.fact_pnl_commercial_stacked fpcs 
	where	not(
				lower(fpcs.alloc_material) in (  
					'adj_royalty',
					'adj_fob_nocust', 
					'adj_fob_no_cust', --need to consolidate 
					'adj_royalty', 
					'unknown',  
					'adj_fob',
					'adj_rsa',
					'adj_service',
					'adj_rebuild',
					'adj_no_prod',
					'adj_no_cust',
					'mgsv-sku'
				) or 
				(
					lower(fpcs.mapped_bar_product) like '%_oth' or 
					lower(fpcs.mapped_bar_product) in ('product_none', 'p60999','oth_service')
				)
	   		)
	;
--select 	*
--from 	stage_material_to_map
--where 	material = '28-242'
--
--select 	distinct alloc_material, mapped_bar_product
--from 	dw.fact_pnl_commercial_stacked
--where 	alloc_material = '28-242'

--select	material, bar_product, count(*)
--from 	stage_material_to_map
--group by material, bar_product
--having count(*) > 1
--order by count(*) desc
--  
--select material,bar_product, sum(amt_usd) as amt_usd 
--from (
--	select distinct mat.material, 
--			coalesce(gpp_portfolio,sbmcb.bar_product,sbmpb.bar_product,sbmlb.bar_product,'unknown') as portfolio, 
--			mat.bar_product,
--			mat.source_system_id 
--	from stage_material_to_map mat
--	left join ref_data.sku_gpp_mapping sgm on lower(mat.material) = lower(sgm.material) and sgm.current_flag =1 
--	left join ref_data.sku_barproduct_mapping_c11_bods sbmcb  on lower(mat.material) = lower(sbmcb.material) and sbmcb.current_flag =1   
--	left join ref_data.sku_barproduct_mapping_p10_bods sbmpb  on lower(mat.material) = lower(sbmpb.material) and sbmpb.current_flag =1  
--	left join ref_data.sku_barproduct_mapping_lawson_bods sbmlb  on lower(mat.material) = lower(sbmlb.material) and sbmlb.current_flag =1  
--	where coalesce(gpp_portfolio,sbmcb.bar_product,sbmpb.bar_product,sbmlb.bar_product) is null
--) a 
--inner join dw.fact_pnl_commercial_stacked fpcs on loweR(a.material) = lower(fpcs.alloc_material)
--where bar_acct= 'A40110' 
--and fiscal_month_id between 201901 and 201912
--group by material,bar_product
--order by 3 desc;
	
	drop table if exists stage_material_to_restate; 
	
	create temporary table stage_material_to_restate
	diststyle all 
	as 
  	select distinct lower(mat.material) as material, 
			coalesce(gpp_portfolio,sbmcb.bar_product,sbmpb.bar_product,sbmlb.bar_product,'unknown') as portfolio
	from stage_material_to_map mat
	left join ref_data.sku_gpp_mapping sgm on lower(mat.material) = lower(sgm.material) and sgm.current_flag =1 
	left join ref_data.sku_barproduct_mapping_c11_bods sbmcb  on lower(mat.material) = lower(sbmcb.material) and sbmcb.current_flag =1   
	left join ref_data.sku_barproduct_mapping_p10_bods sbmpb  on lower(mat.material) = lower(sbmpb.material) and sbmpb.current_flag =1  
	left join ref_data.sku_barproduct_mapping_lawson_bods sbmlb  on lower(mat.material) = lower(sbmlb.material) and sbmlb.current_flag =1 ;

--select	count(*), count(distinct lower(material))
--from 	stage_material_to_restate
	/* Validate : If material have more than one portfolio */ 
	/*************************************************************
	select count(distinct portfolio ), material
	from stage_material_to_restate
	group by material 
	having count(distinct portfolio )>1;
	*******************************************************************/
	
	
	
	/* current version of bar_product hierarchy */
	drop table if exists tmp_bar_product_hierarchy;
	create temporary table tmp_bar_product_hierarchy 
	diststyle all 
	as 
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
			where 	loaddts = ( select max(loaddts) from bods.drm_product_current dpc )
				and membertype != 'Parent'
		)
		select 	bar_product,
				bar_product_desc,
				membertype,
				portfolio,
				portfolio_desc,
				generation,
				level01_bar,
				case when generation <= 2  then case when bar_product = 'Product_None' then bar_product else parent end else level02_bar end as level02_bar,
				case when generation <= 3  then case when bar_product = 'Product_None' then bar_product else parent end else level03_bar end as level03_bar,
				case when generation <= 4  then case when bar_product = 'Product_None' then bar_product else parent end else level04_bar end as level04_bar,
				case when generation <= 5  then case when bar_product = 'Product_None' then bar_product else parent end else level05_bar end as level05_bar,
				case when generation <= 6  then case when bar_product = 'Product_None' then bar_product else parent end else level06_bar end as level06_bar,
				case when generation <= 7  then case when bar_product = 'Product_None' then bar_product else parent end else level07_bar end as level07_bar,
				case when generation <= 8  then case when bar_product = 'Product_None' then bar_product else parent end else level08_bar end as level08_bar,
				bar_product as level09_bar
		from 	cte_base 
		where 	lower(bar_product) not in ('product_none', 'p60999','oth_service')
	;
		/* Validate : bar_product & portfolio are same & no duplicate portfolio  */ 
		/******************************************************************************
		select count(1), bar_product
		from tmp_bar_product_hierarchy
		group by bar_product
		having count(1) >1;
		******************************************************************************/
		delete from dw.dim_product_restatement;
	
		insert into dw.dim_product_restatement(
				material,
				portfolio,
				portfolio_desc,
				bar_product,
				bar_product_desc,
				member_type,
				generation,
				level01_bar,
				level02_bar,
				level03_bar,
				level04_bar,
				level05_bar,
				level06_bar,
				level07_bar,
				level08_bar,
				level09_bar,
				start_date,
				end_date,
				audit_loadts)
		select mat.material, 
			  mat.portfolio, 
			  isnull(bph.portfolio_desc,'unknown') portfolio_desc,
			  isnull(bph.bar_product, 'unknown') bar_product,
			  isnull(bph.bar_product_desc,'unknown') bar_product_desc,
			  isnull(bph.membertype,'Base') membertype ,
			  isnull(bph.generation,9) generation,
			  isnull(bph.level01_bar,'unknown') level01_bar,
			  isnull(bph.level02_bar,'unknown') level02_bar,
			  isnull(bph.level03_bar,'unknown') level03_bar,
			  isnull(bph.level04_bar,'unknown') level04_bar,
			  isnull(bph.level05_bar,'unknown') level05_bar,
			  isnull(bph.level06_bar,'unknown') level06_bar,
			  isnull(bph.level07_bar,'unknown') level07_bar,
			  isnull(bph.level08_bar,'unknown') level08_bar,
			  isnull(bph.level09_bar,'unknown') level09_bar,
			  cast('1900-01-01' as date) as start_date, 
			  cast('9999-12-31' as date) as end_date, 
			  cast(getdate() as timestamp) as audit_loadts
		from stage_material_to_restate mat
		left join tmp_bar_product_hierarchy bph on lower(bph.portfolio) = lower(mat.portfolio) 
	;

EXCEPTION
		when others then raise info 'exception occur while ingesting data in dim_material_restate_hierarchy';
END
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_dim_scenario(flag_reload integer)
 LANGUAGE plpgsql
AS $$
Begin
	
	/* This table does not use identity based surrogate key, so does not need Insert / Update Strategy */
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_scenario;
	end if;

	delete from dw.dim_scenario;
	
	--'Actuals, Budget Forcast'
	insert into dw.dim_scenario (scenario_id ,Scenario)
	values (1,'Actuals');
	
	insert into dw.dim_scenario (scenario_id ,Scenario)
	values (2,'Budgeted');
	
	insert into dw.dim_scenario (scenario_id ,Scenario)
	values (3,'Forecast');
exception
when others then raise info 'exception occur while ingesting data in reference_dims';
END
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_dim_source_system(flag_reload integer)
 LANGUAGE plpgsql
AS $$
Begin
	/* This table does not use identity based surrogate key, so does not need Insert / Update Strategy */
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_source_system;
	end if;

	delete from dw.dim_source_system;
	insert into dw.dim_source_system ( source_system_id , source_system )
		Select 1 as source_system_id, 
		       'sap_c11' as source_system 
		union all 
		Select 2 as source_system_id, 
		       'sap_p10' as source_system
		union all 
		Select 3 as source_system_id, 
		       'sap_lawson' as source_system
		UNION ALL 
		SELECT 4 AS source_system_id,
		       'hfm' AS source_system
		union all 
		Select 5 as source_system_id,
		   'ext_c11fob' as source_system
		union all 
		Select 6 as source_system_id,
		   'ext_c11std' as source_system
		union all 
		Select 7 as source_system_id,
		   'rsa_bible' as source_system
		union all 
		Select 8 as source_system_id,
		   'agm-inv-adj-gap' as source_system
		union all 
		Select 9 as source_system_id,
		   'adj-wa-tran' as source_system
		union all 
		Select 10 as source_system_id,
		   'adj-wa-tran-gap' as source_system
	;		
exception
when others then raise info 'exception occur while ingesting data in reference_dims';
end;
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_dim_transactional_attributes(flag_reload integer)
 LANGUAGE plpgsql
AS $$
Begin
	
	/* This table does not use identity based surrogate key, so does not need Insert -Update Strategy */
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_transactional_attributes;
	end if;
	-- call dw.p_build_dim_transactional_attributes() 
	-- select * from dw.dim_transactional_attributes
	
	delete from dw.dim_transactional_attributes;
	insert into dw.dim_transactional_attributes ( dim_transactional_attributes_id, PCR )
		SELECT 	DISTINCT 
				lower(pcr) as dim_transactional_attributes_id,
				lower(pcr) as PCR
		FROM 	ref_data.rsa_bible 
		WHERE 	pcr IS NOT NULL and 
				pcr != ''
	;
exception
when others then raise info 'exception occur while ingesting data in dim_transactional_attributes';
end;
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_commercial_allocation_rule_09(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
--this proc is temporary while we create the fact load process 
--ward 
--record count is 711688
BEGIN 
	
	
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;

	drop table if exists fact_pnl_commercial_allocation_rule_09
	;
	create temporary table fact_pnl_commercial_allocation_rule_09
	diststyle even
	sortkey (posting_week_enddate)
	as
	Select 	
			 tr.*
		    ,COALESCE(dc.customer_key, dc_bar.customer_key) as customer_key
		    ,COALESCE ( 
		    	dp.product_key,
		    	(
			    	select 	dp_unk.product_key 
			    	from 	dw.dim_product dp_unk
			    	where 	dp_unk.product_id = 'unknown|unknown|unknown'
			    	limit 1
			    )
		    ) as product_key
		    ,NULL AS dim_transactional_attributes_id
		    ,ddo.dataprocessing_outcome_key
		    ,dbu.business_unit_key
		    ,dss.source_system_id 
		    /* agg_measures */
		    ,product_sales + service_and_installation_sales + other_sales_revenue_total as  gross_sales
		    ,allowances_total as allowances
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + allowances_total as invoice_sales
		    ,(rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as sales_deduction
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as net_sales
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) as std_cos
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos as total_cos
		    ,((product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other)) + 
		     ((standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos) as std_gross_margin
	            
	from (
		    Select 
		         f.posting_week_enddate 
		        ,f.fiscal_month_id 
		        ,f.bar_acct
		        ,f.bar_currtype 
		        ,COALESCE( f.bar_entity, 'unknown' ) as bar_entity
		        
		        ,f.org_tranagg_id
				,f.org_dataprocessing_ruleid
				,f.mapped_dataprocessing_ruleid
				,f.dataprocessing_outcome_id
				,f.dataprocessing_phase
				,f.org_bar_brand
				,f.org_bar_custno
				,f.org_bar_product
				,f.mapped_bar_brand
				,f.mapped_bar_custno
				,f.mapped_bar_product
				
		        
		        ,COALESCE( f.org_soldtocust, 'unknown' ) as org_soldtocust
		        ,COALESCE( f.org_shiptocust, 'unknown' ) as org_shiptocust
		        ,COALESCE( f.org_material, 'unknown' ) as org_material
		        ,COALESCE( f.alloc_soldtocust, 'unknown' ) as alloc_soldtocust
		        ,COALESCE( f.alloc_shiptocust, 'unknown' ) as alloc_shiptocust
		        ,COALESCE( f.alloc_material, 'unknown' ) as alloc_material
		        ,COALESCE( f.alloc_bar_product, 'unknown' ) as alloc_bar_product
		        
				,CASE
					WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NULL THEN 'unknown|unknown'
					WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NOT NULL THEN 'BA&R placeholder|' || f.alloc_bar_product
					ELSE f.alloc_material || '|' || f.alloc_bar_product
				 END || '|' || COALESCE( f.mapped_bar_brand, 'unknown' ) as product_id
				 
				,(
					COALESCE( f.alloc_soldtocust, 'unknown' ) || '|' || 
					COALESCE( f.alloc_shiptocust, 'unknown' ) || '|' || 
					COALESCE( f.mapped_bar_custno, 'unknown' )
				 ) as customer_id
				,'unknown|unknown|' || COALESCE( f.mapped_bar_custno, 'unknown' ) as customer_id_bar
		        
		        ,cast(1 as integer) as scenario_id  -- Hard coded to Actuals - other values are future scope
     
		        ,f.allocated_amt as amt
		        ,'n/a' as uom
		        ,0 as tran_volume
		        ,0 as sales_volume
		        ,source_system
		        
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) sales_invoiced  
		        ,(case when bar_acct = 'A40116' then allocated_amt else 0 end) sales_freight_income   
		        ,(case when bar_acct = 'A40210' then allocated_amt else 0 end) product_sales_export 
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) as product_sales_domenstic_total 
		        ,(case when bar_acct = 'A40111' then allocated_amt else 0 end) fob_invoice_sale    
		        ,(case when bar_acct = 'A40310' then allocated_amt else 0 end) rental_sales 
		        ,(case when bar_acct = 'A40120' then allocated_amt else 0 end) destroy_in_field  
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40111' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40310' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40120' then allocated_amt else 0 end)  as product_sales 
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) as billable_service_revenue  
		        ,(case when bar_acct = 'A40510' then allocated_amt else 0 end) contract_service_revenue   
		        ,(case when bar_acct = 'A40610' then allocated_amt else 0 end) install_revenue  
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40610' then allocated_amt else 0 end) as service_and_installation_sales   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) franchise_revenue   
		        ,(case when bar_acct = 'A40910' then allocated_amt else 0 end) royalty_revenue   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40910' then allocated_amt else 0 end) as other_sales_revenue_total  
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) as returns_domestic  
		        ,(case when bar_acct = 'A41210' then allocated_amt else 0 end) as returns_export   
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end)  as sales_returns 
		        ,(case when bar_acct = 'A40115' then allocated_amt else 0 end) as rsa_and_price_adjustments
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40115' then allocated_amt else 0 end) as allowances_total
		        ,(case when bar_acct = 'A43112' then allocated_amt else 0 end) as rebates
		        ,(case when bar_acct = 'A43116' then allocated_amt else 0 end) as coop_advertising
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) as cashdiscount_domestic
		        ,(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cashdiscount_export
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) +
		          	(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cash_discount_total 
		        ,(case when bar_acct = 'A43111' then allocated_amt else 0 end) as fillrate_fine
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) as fob_deductions
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) as discounts_allow_domestic
		        ,(case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_allow_export
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		         (case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_and_allowexcl_coop_adv
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) as freight_domestic
		        ,(case when bar_acct = 'A42210' then allocated_amt else 0 end) as freight_export
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) as sales_freight
		        ,(case when bar_acct = 'A43120' then allocated_amt else 0 end) as customer_considerations
		        ,(case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_adjustments_other
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A43120' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_deduction_other
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) as standard_material_cost_domestic
		        ,(case when bar_acct = 'A60111' then allocated_amt else 0 end) as standard_material_cost_fob
		        ,(case when bar_acct = 'A60210' then allocated_amt else 0 end) as standard_material_cost_export
		        ,(case when bar_acct = 'A60112' then allocated_amt else 0 end) as standard_material_cost_serv_install
		        ,(case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost_serv_install_3p
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60111' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60210' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60112' then allocated_amt else 0 end) +
			         (case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) as cos_trd_domestic_labor
		        ,(case when bar_acct = 'A61210' then allocated_amt else 0 end) as cos_trd_export_labor
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) as std_labor_cost_manuf
		        ,(case when bar_acct = 'A60410' then allocated_amt else 0 end) as cos_service
		        ,(case when bar_acct = 'A60510' then allocated_amt else 0 end) as cos_monitoring
		        ,(case when bar_acct = 'A60610' then allocated_amt else 0 end) as cos_installations
		        ,(case when bar_acct = 'A60612' then allocated_amt else 0 end) as std_labor_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos_serv_install_3p
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60410' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60610' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A60612' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) as std_oh_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_oh_cos_serv_install_3p
		        ,(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos_export
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62613' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) as free_goods_cos
		        ,(case when bar_acct = 'A60115' then allocated_amt else 0 end) as targeted_funds_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A60115' then allocated_amt else 0 end) as free_goods_total
		        ,(case when bar_acct = 'A60710' then allocated_amt else 0 end) as cos_freight_outbound
		        ,(case when bar_acct = 'A60114' then allocated_amt else 0 end) as merchandising_cos
		        ,(case when bar_acct = 'A60310' then allocated_amt else 0 end) as rental_cos
			from 	stage.sgm_allocated_data_rule_09 f 
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= f.posting_week_enddate  and 
							dd.range_end_date >= f.posting_week_enddate	
		)as tr
		LEFT OUTER JOIN dw.dim_product dp on lower(tr.product_id) = lower(dp.product_id) 
		LEFT OUTER JOIN dw.dim_business_unit dbu on lower(tr.bar_entity) = lower(dbu.bar_entity)
		LEFT OUTER JOIN dw.dim_dataprocessing_outcome ddo 
			on 	ddo.dataprocessing_outcome_id = tr.dataprocessing_outcome_id and 
				lower(ddo.dataprocessing_phase) = lower(tr.dataprocessing_phase) 
		LEFT OUTER JOIN dw.dim_customer dc on lower(dc.customer_id) = lower(tr.customer_id)
		LEFT OUTER JOIN dw.dim_customer dc_bar on lower(dc_bar.customer_id) = lower(tr.customer_id_bar)
		LEFT OUTER JOIN dw.dim_source_system dss on lower(tr.source_system) = lower(dss.source_system) 
	;



	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_commercial 
	where 	mapped_dataprocessing_ruleid  = 9 and 
			posting_week_enddate between 
			(select range_start_date from vtbl_date_range) and 
			(select range_end_date from vtbl_date_range)
	;
	INSERT INTO dw.fact_pnl_commercial (
				org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
			    alloc_bar_product,
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				audit_loadts,
				dim_transactional_attributes_id
		)
		Select	org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
			    alloc_bar_product,
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				getdate() as audit_loadts,
				dim_transactional_attributes_id
		from 	fact_pnl_commercial_allocation_rule_09 f
	;
exception when others then raise info 'exception occur while ingesting data in fact_pnl_commercial for rule09';
end
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_commercial_allocation_rule_13(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 

	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	--5 Minutes 23 sec
	drop table if exists fact_pnl_commercial_allocation_rule_13
	;
	create temporary table fact_pnl_commercial_allocation_rule_13
	diststyle even
	sortkey (posting_week_enddate)
	as
	Select 	
			 tr.*
		    ,COALESCE(
		    	dc.customer_key,
		    	dc_stast.customer_key,
		    	dc_unkst.customer_key,
		    	dc_bar.customer_key
		    ) as customer_key
		    ,COALESCE ( 
		    	dp.product_key,
		    	(
			    	select 	dp_unk.product_key 
			    	from 	dw.dim_product dp_unk
			    	where 	dp_unk.product_id = 'unknown|unknown|unknown'
			    	limit 1
			    )
		    ) as product_key
		    ,NULL AS dim_transactional_attributes_id
		    ,ddo.dataprocessing_outcome_key
		    ,dbu.business_unit_key
		    ,dss.source_system_id 
		    /* agg_measures */
		    ,product_sales + service_and_installation_sales + other_sales_revenue_total as  gross_sales
		    ,allowances_total as allowances
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + allowances_total as invoice_sales
		    ,(rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as sales_deduction
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as net_sales
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) as std_cos
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos as total_cos
		    ,((product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other)) + 
		     ((standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos) as std_gross_margin
	            
	from (
		    Select 
		         f.posting_week_enddate 
		         ,f.fiscal_month_id 
		        ,f.bar_acct
		        ,f.bar_currtype 
		        ,COALESCE( f.bar_entity, 'unknown' ) as bar_entity
		        
		        ,f.org_tranagg_id
				,f.org_dataprocessing_ruleid
				,f.mapped_dataprocessing_ruleid
				,f.dataprocessing_outcome_id
				,f.dataprocessing_phase
				,f.org_bar_brand
				,f.org_bar_custno
				,f.org_bar_product
				,f.mapped_bar_brand
				,f.mapped_bar_custno
				,f.mapped_bar_product
				
		        
		        ,COALESCE( f.org_soldtocust, 'unknown' ) as org_soldtocust
		        ,COALESCE( f.org_shiptocust, 'unknown' ) as org_shiptocust
		        ,COALESCE( f.org_material, 'unknown' ) as org_material
		        ,COALESCE( f.alloc_soldtocust, 'unknown' ) as alloc_soldtocust
		        ,COALESCE( f.alloc_shiptocust, 'unknown' ) as alloc_shiptocust
		        ,COALESCE( f.alloc_material, 'unknown' ) as alloc_material
		        ,COALESCE( f.alloc_bar_product, 'unknown' ) as alloc_bar_product
		        
				,CASE
					WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NULL THEN 'unknown|unknown'
					WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NOT NULL THEN 'BA&R placeholder|' || f.alloc_bar_product
					ELSE f.alloc_material || '|' || f.alloc_bar_product
				 END || '|' || COALESCE( f.mapped_bar_brand, 'unknown' ) as product_id
				,(
					COALESCE( f.alloc_soldtocust, 'unknown' ) || '|' || 
					COALESCE( f.alloc_shiptocust, 'unknown' ) || '|' || 
					COALESCE( f.mapped_bar_custno, 'unknown' )
				 ) as customer_id
				,(
					COALESCE( f.alloc_soldtocust, 'unknown' ) || '|' || 
					COALESCE( f.alloc_soldtocust, 'unknown' ) || '|' || 
					COALESCE( f.mapped_bar_custno, 'unknown' )
				 ) as customer_id_soldto_as_shipto
				,(
					COALESCE( f.alloc_soldtocust, 'unknown' ) || '|' || 
					'unknown' || '|' || 
					COALESCE( f.mapped_bar_custno, 'unknown' )
				 ) as customer_id_unk_shipto
				,'unknown|unknown|' || COALESCE( f.mapped_bar_custno, 'unknown' ) as customer_id_bar
		        
		        ,cast(1 as integer) as scenario_id  -- Hard coded to Actuals - other values are future scope
		        ,source_system  -- this is hard coded to C11 for now
		        
		        ,f.allocated_amt as amt
		        ,'n/a' as uom
		        ,0 as tran_volume
		        ,0 as sales_volume
		        
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) sales_invoiced  
		        ,(case when bar_acct = 'A40116' then allocated_amt else 0 end) sales_freight_income   
		        ,(case when bar_acct = 'A40210' then allocated_amt else 0 end) product_sales_export 
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) as product_sales_domenstic_total 
		        ,(case when bar_acct = 'A40111' then allocated_amt else 0 end) fob_invoice_sale    
		        ,(case when bar_acct = 'A40310' then allocated_amt else 0 end) rental_sales 
		        ,(case when bar_acct = 'A40120' then allocated_amt else 0 end) destroy_in_field  
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40111' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40310' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40120' then allocated_amt else 0 end)  as product_sales 
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) as billable_service_revenue  
		        ,(case when bar_acct = 'A40510' then allocated_amt else 0 end) contract_service_revenue   
		        ,(case when bar_acct = 'A40610' then allocated_amt else 0 end) install_revenue  
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40610' then allocated_amt else 0 end) as service_and_installation_sales   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) franchise_revenue   
		        ,(case when bar_acct = 'A40910' then allocated_amt else 0 end) royalty_revenue   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40910' then allocated_amt else 0 end) as other_sales_revenue_total  
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) as returns_domestic  
		        ,(case when bar_acct = 'A41210' then allocated_amt else 0 end) as returns_export   
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end)  as sales_returns 
		        ,(case when bar_acct = 'A40115' then allocated_amt else 0 end) as rsa_and_price_adjustments
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40115' then allocated_amt else 0 end) as allowances_total
		        ,(case when bar_acct = 'A43112' then allocated_amt else 0 end) as rebates
		        ,(case when bar_acct = 'A43116' then allocated_amt else 0 end) as coop_advertising
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) as cashdiscount_domestic
		        ,(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cashdiscount_export
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) +
		          	(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cash_discount_total 
		        ,(case when bar_acct = 'A43111' then allocated_amt else 0 end) as fillrate_fine
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) as fob_deductions
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) as discounts_allow_domestic
		        ,(case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_allow_export
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		         (case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_and_allowexcl_coop_adv
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) as freight_domestic
		        ,(case when bar_acct = 'A42210' then allocated_amt else 0 end) as freight_export
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) as sales_freight
		        ,(case when bar_acct = 'A43120' then allocated_amt else 0 end) as customer_considerations
		        ,(case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_adjustments_other
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A43120' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_deduction_other
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) as standard_material_cost_domestic
		        ,(case when bar_acct = 'A60111' then allocated_amt else 0 end) as standard_material_cost_fob
		        ,(case when bar_acct = 'A60210' then allocated_amt else 0 end) as standard_material_cost_export
		        ,(case when bar_acct = 'A60112' then allocated_amt else 0 end) as standard_material_cost_serv_install
		        ,(case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost_serv_install_3p
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60111' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60210' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60112' then allocated_amt else 0 end) +
			         (case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) as cos_trd_domestic_labor
		        ,(case when bar_acct = 'A61210' then allocated_amt else 0 end) as cos_trd_export_labor
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) as std_labor_cost_manuf
		        ,(case when bar_acct = 'A60410' then allocated_amt else 0 end) as cos_service
		        ,(case when bar_acct = 'A60510' then allocated_amt else 0 end) as cos_monitoring
		        ,(case when bar_acct = 'A60610' then allocated_amt else 0 end) as cos_installations
		        ,(case when bar_acct = 'A60612' then allocated_amt else 0 end) as std_labor_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos_serv_install_3p
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60410' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60610' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A60612' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) as std_oh_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_oh_cos_serv_install_3p
		        ,(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos_export
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62613' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) as free_goods_cos
		        ,(case when bar_acct = 'A60115' then allocated_amt else 0 end) as targeted_funds_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A60115' then allocated_amt else 0 end) as free_goods_total
		        ,(case when bar_acct = 'A60710' then allocated_amt else 0 end) as cos_freight_outbound
		        ,(case when bar_acct = 'A60114' then allocated_amt else 0 end) as merchandising_cos
		        ,(case when bar_acct = 'A60310' then allocated_amt else 0 end) as rental_cos
			from 	stage.sgm_allocated_data_rule_13 f 
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= f.posting_week_enddate  and 
							dd.range_end_date >= f.posting_week_enddate	
		)as tr
		LEFT OUTER JOIN dw.dim_product dp on lower(tr.product_id) = lower(dp.product_id) 
		LEFT OUTER JOIN dw.dim_business_unit dbu on lower(tr.bar_entity) = lower(dbu.bar_entity)
		LEFT OUTER JOIN dw.dim_dataprocessing_outcome ddo 
			on 	ddo.dataprocessing_outcome_id = tr.dataprocessing_outcome_id and 
				lower(ddo.dataprocessing_phase) = lower(tr.dataprocessing_phase) 
		LEFT OUTER JOIN dw.dim_customer dc on lower(dc.customer_id) = lower(tr.customer_id)
		LEFT OUTER JOIN dw.dim_customer dc_stast on lower(dc_stast.customer_id) = lower(tr.customer_id_soldto_as_shipto)
		LEFT OUTER JOIN dw.dim_customer dc_unkst on lower(dc_unkst.customer_id) = lower(tr.customer_id_unk_shipto)
		LEFT OUTER JOIN dw.dim_customer dc_bar on lower(dc_bar.customer_id) = lower(tr.customer_id_bar)
		LEFT OUTER JOIN dw.dim_source_system dss on lower(tr.source_system) = lower(dss.source_system);

--select 	count(*) row_count,
--		sum(case when customer_key is null then 1 else 0 end) as missing_custkey,
--		sum(case when product_key is null then 1 else 0 end) as missing_prodkey
--from 	fact_pnl_commercial_allocation_rule_13
--;

	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_commercial 
	where 	mapped_dataprocessing_ruleid  = 13 and 
			posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
	INSERT INTO dw.fact_pnl_commercial (
				org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				
			    org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
				alloc_bar_product,
				
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				audit_loadts,
				dim_transactional_attributes_id
		)
		Select	org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				
			    org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
				alloc_bar_product,
				
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				getdate() as audit_loadts,
				dim_transactional_attributes_id
		from 	fact_pnl_commercial_allocation_rule_13 f
	;
exception when others then raise info 'exception occur while ingesting data in fact_pnl_commercial for rule13';
end
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_commercial_allocation_rule_21(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 

	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	--5 Minutes 23 sec
	drop table if exists fact_pnl_commercial_allocation_rule_21
	;
	create temporary table fact_pnl_commercial_allocation_rule_21
	diststyle even
	sortkey (posting_week_enddate)
	as
	Select 	
			 tr.*
		    ,COALESCE(dc.customer_key, dc_bar.customer_key) as customer_key
		    ,COALESCE ( 
		    	dp.product_key,
		    	(
			    	select 	dp_unk.product_key 
			    	from 	dw.dim_product dp_unk
			    	where 	dp_unk.product_id = 'unknown|unknown|unknown'
			    	limit 1
			    )
		    ) as product_key
		    ,NULL AS dim_transactional_attributes_id
		    ,ddo.dataprocessing_outcome_key
		    ,dbu.business_unit_key
		    ,dss.source_system_id 
		    /* agg_measures */
		    ,product_sales + service_and_installation_sales + other_sales_revenue_total as  gross_sales
		    ,allowances_total as allowances
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + allowances_total as invoice_sales
		    ,(rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as sales_deduction
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as net_sales
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) as std_cos
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos as total_cos
		    ,((product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other)) + 
		     ((standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos) as std_gross_margin
	   from (
		    Select 
		         f.posting_week_enddate 
		         ,f.fiscal_month_id 
		        ,f.bar_acct
		        ,f.bar_currtype 
		        ,COALESCE( f.bar_entity, 'unknown' ) as bar_entity
		        
		        ,f.org_tranagg_id
				,f.org_dataprocessing_ruleid
				,f.mapped_dataprocessing_ruleid
				,f.dataprocessing_outcome_id
				,f.dataprocessing_phase
				,f.org_bar_brand
				,f.org_bar_custno
				,f.org_bar_product
				,f.mapped_bar_brand
				,f.mapped_bar_custno
				,f.mapped_bar_product
			   ,COALESCE( f.org_soldtocust, 'unknown' ) as org_soldtocust
		        ,COALESCE( f.org_shiptocust, 'unknown' ) as org_shiptocust
		        ,COALESCE( f.org_material, 'unknown' ) as org_material
		        ,COALESCE( f.alloc_soldtocust, 'unknown' ) as alloc_soldtocust
		        ,COALESCE( f.alloc_shiptocust, 'unknown' ) as alloc_shiptocust
		        ,COALESCE( f.alloc_material, 'unknown' ) as alloc_material
		        ,COALESCE( f.alloc_bar_product, 'unknown' ) as alloc_bar_product
				,CASE
					WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NULL THEN 'unknown|unknown'
					WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NOT NULL THEN 'BA&R placeholder|' || f.alloc_bar_product
					ELSE f.alloc_material || '|' || f.alloc_bar_product
				 END || '|' || COALESCE( f.mapped_bar_brand, 'unknown' ) as product_id
				,(
					COALESCE( f.alloc_soldtocust, 'unknown' ) || '|' || 
					COALESCE( f.alloc_shiptocust, 'unknown' ) || '|' || 
					COALESCE( f.mapped_bar_custno, 'unknown' )
				 ) as customer_id
				,'unknown|unknown|' || COALESCE( f.mapped_bar_custno, 'unknown' ) as customer_id_bar
 		        ,cast(1 as integer) as scenario_id  -- Hard coded to Actuals - other values are future scope
		        ,source_system  
		        ,f.allocated_amt as amt
		        ,uom as uom
		        ,f.tran_volume as tran_volume
		        ,f.sales_volume as sales_volume
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) sales_invoiced  
		        ,(case when bar_acct = 'A40116' then allocated_amt else 0 end) sales_freight_income   
		        ,(case when bar_acct = 'A40210' then allocated_amt else 0 end) product_sales_export 
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) as product_sales_domenstic_total 
		        ,(case when bar_acct = 'A40111' then allocated_amt else 0 end) fob_invoice_sale    
		        ,(case when bar_acct = 'A40310' then allocated_amt else 0 end) rental_sales 
		        ,(case when bar_acct = 'A40120' then allocated_amt else 0 end) destroy_in_field  
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40111' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40310' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40120' then allocated_amt else 0 end)  as product_sales 
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) as billable_service_revenue  
		        ,(case when bar_acct = 'A40510' then allocated_amt else 0 end) contract_service_revenue   
		        ,(case when bar_acct = 'A40610' then allocated_amt else 0 end) install_revenue  
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40610' then allocated_amt else 0 end) as service_and_installation_sales   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) franchise_revenue   
		        ,(case when bar_acct = 'A40910' then allocated_amt else 0 end) royalty_revenue   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40910' then allocated_amt else 0 end) as other_sales_revenue_total  
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) as returns_domestic  
		        ,(case when bar_acct = 'A41210' then allocated_amt else 0 end) as returns_export   
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end)  as sales_returns 
		        ,(case when bar_acct = 'A40115' then allocated_amt else 0 end) as rsa_and_price_adjustments
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40115' then allocated_amt else 0 end) as allowances_total
		        ,(case when bar_acct = 'A43112' then allocated_amt else 0 end) as rebates
		        ,(case when bar_acct = 'A43116' then allocated_amt else 0 end) as coop_advertising
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) as cashdiscount_domestic
		        ,(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cashdiscount_export
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) +
		          	(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cash_discount_total 
		        ,(case when bar_acct = 'A43111' then allocated_amt else 0 end) as fillrate_fine
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) as fob_deductions
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) as discounts_allow_domestic
		        ,(case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_allow_export
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		         (case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_and_allowexcl_coop_adv
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) as freight_domestic
		        ,(case when bar_acct = 'A42210' then allocated_amt else 0 end) as freight_export
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) as sales_freight
		        ,(case when bar_acct = 'A43120' then allocated_amt else 0 end) as customer_considerations
		        ,(case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_adjustments_other
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A43120' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_deduction_other
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) as standard_material_cost_domestic
		        ,(case when bar_acct = 'A60111' then allocated_amt else 0 end) as standard_material_cost_fob
		        ,(case when bar_acct = 'A60210' then allocated_amt else 0 end) as standard_material_cost_export
		        ,(case when bar_acct = 'A60112' then allocated_amt else 0 end) as standard_material_cost_serv_install
		        ,(case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost_serv_install_3p
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60111' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60210' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60112' then allocated_amt else 0 end) +
			         (case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) as cos_trd_domestic_labor
		        ,(case when bar_acct = 'A61210' then allocated_amt else 0 end) as cos_trd_export_labor
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) as std_labor_cost_manuf
		        ,(case when bar_acct = 'A60410' then allocated_amt else 0 end) as cos_service
		        ,(case when bar_acct = 'A60510' then allocated_amt else 0 end) as cos_monitoring
		        ,(case when bar_acct = 'A60610' then allocated_amt else 0 end) as cos_installations
		        ,(case when bar_acct = 'A60612' then allocated_amt else 0 end) as std_labor_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos_serv_install_3p
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60410' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60610' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A60612' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) as std_oh_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_oh_cos_serv_install_3p
		        ,(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos_export
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62613' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) as free_goods_cos
		        ,(case when bar_acct = 'A60115' then allocated_amt else 0 end) as targeted_funds_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A60115' then allocated_amt else 0 end) as free_goods_total
		        ,(case when bar_acct = 'A60710' then allocated_amt else 0 end) as cos_freight_outbound
		        ,(case when bar_acct = 'A60114' then allocated_amt else 0 end) as merchandising_cos
		        ,(case when bar_acct = 'A60310' then allocated_amt else 0 end) as rental_cos
			from 	stage.sgm_allocated_data_rule_21 f 
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= f.posting_week_enddate  and 
							dd.range_end_date >= f.posting_week_enddate	
		)as tr
		LEFT OUTER JOIN dw.dim_product dp on lower(tr.product_id) = lower(dp.product_id) 
		LEFT OUTER JOIN dw.dim_business_unit dbu on lower(tr.bar_entity) = lower(dbu.bar_entity)
		LEFT OUTER JOIN dw.dim_dataprocessing_outcome ddo 
			on 	ddo.dataprocessing_outcome_id = tr.dataprocessing_outcome_id and 
				lower(ddo.dataprocessing_phase) = lower(tr.dataprocessing_phase) 
		LEFT OUTER JOIN dw.dim_customer dc on lower(dc.customer_id) = lower(tr.customer_id)
		LEFT OUTER JOIN dw.dim_customer dc_bar on lower(dc_bar.customer_id) = lower(tr.customer_id_bar)
		LEFT OUTER JOIN dw.dim_source_system dss on lower(tr.source_system) = lower(dss.source_system)
	;


	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_commercial 
	where 	mapped_dataprocessing_ruleid = 21 and 
			posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
	INSERT INTO dw.fact_pnl_commercial (
				org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
			    org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
				alloc_bar_product,
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				audit_loadts,
				dim_transactional_attributes_id
		)
		Select	org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
			    org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
				alloc_bar_product,
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				getdate() as audit_loadts,
				dim_transactional_attributes_id
		from 	fact_pnl_commercial_allocation_rule_21 f
	;
exception when others then raise info 'exception occur while ingesting data in fact_pnl_commercial for rule21';
end
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_commercial_allocation_rule_22(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 

	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	--5 Minutes 23 sec
	drop table if exists fact_pnl_commercial_allocation_rule_22
	;
	create temporary table fact_pnl_commercial_allocation_rule_22
	diststyle even
	sortkey (posting_week_enddate)
	as
	Select 	
			 tr.*
		    ,COALESCE(dc.customer_key, dc_bar.customer_key) as customer_key
		    ,COALESCE ( 
		    	dp.product_key,
		    	(
			    	select 	dp_unk.product_key 
			    	from 	dw.dim_product dp_unk
			    	where 	dp_unk.product_id = 'unknown|unknown|unknown'
			    	limit 1
			    )
		    ) as product_key
		    ,NULL AS dim_transactional_attributes_id
		    ,ddo.dataprocessing_outcome_key
		    ,dbu.business_unit_key
		    ,dss.source_system_id 
		    /* agg_measures */
		    ,product_sales + service_and_installation_sales + other_sales_revenue_total as  gross_sales
		    ,allowances_total as allowances
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + allowances_total as invoice_sales
		    ,(rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as sales_deduction
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as net_sales
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) as std_cos
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos as total_cos
		    ,((product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other)) + 
		     ((standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos) as std_gross_margin
	            
	from (
		    Select 
		         f.posting_week_enddate 
		         ,f.fiscal_month_id 
		        ,f.bar_acct
		        ,f.bar_currtype 
		        ,COALESCE( f.bar_entity, 'unknown' ) as bar_entity
		        
		        ,f.org_tranagg_id
				,f.org_dataprocessing_ruleid
				,f.mapped_dataprocessing_ruleid
				,f.dataprocessing_outcome_id
				,f.dataprocessing_phase
				,f.org_bar_brand
				,f.org_bar_custno
				,f.org_bar_product
				,f.mapped_bar_brand
				,f.mapped_bar_custno
				,f.mapped_bar_product
				
		        
		        ,COALESCE( f.org_soldtocust, 'unknown' ) as org_soldtocust
		        ,COALESCE( f.org_shiptocust, 'unknown' ) as org_shiptocust
		        ,COALESCE( f.org_material, 'unknown' ) as org_material
		        ,COALESCE( f.alloc_soldtocust, 'unknown' ) as alloc_soldtocust
		        ,COALESCE( f.alloc_shiptocust, 'unknown' ) as alloc_shiptocust
		        ,COALESCE( f.alloc_material, 'unknown' ) as alloc_material
		        ,COALESCE( f.alloc_bar_product, 'unknown' ) as alloc_bar_product
		        
				,CASE
					WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NULL THEN 'unknown|unknown'
					WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NOT NULL THEN 'BA&R placeholder|' || f.alloc_bar_product
					ELSE f.alloc_material || '|' || f.alloc_bar_product
				 END || '|' || COALESCE( f.mapped_bar_brand, 'unknown' ) as product_id
				,(
					COALESCE( f.alloc_soldtocust, 'unknown' ) || '|' || 
					COALESCE( f.alloc_shiptocust, 'unknown' ) || '|' || 
					COALESCE( f.mapped_bar_custno, 'unknown' )
				 ) as customer_id
				,'unknown|unknown|' || COALESCE( f.mapped_bar_custno, 'unknown' ) as customer_id_bar
		        
		        ,cast(1 as integer) as scenario_id  -- Hard coded to Actuals - other values are future scope
		        ,source_system  -- this is hard coded to C11 for now
		        
		        ,f.allocated_amt as amt
		        ,'n/a' as uom
		        ,f.tran_volume as tran_volume
		        ,f.sales_volume as sales_volume
		        
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) sales_invoiced  
		        ,(case when bar_acct = 'A40116' then allocated_amt else 0 end) sales_freight_income   
		        ,(case when bar_acct = 'A40210' then allocated_amt else 0 end) product_sales_export 
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) as product_sales_domenstic_total 
		        ,(case when bar_acct = 'A40111' then allocated_amt else 0 end) fob_invoice_sale    
		        ,(case when bar_acct = 'A40310' then allocated_amt else 0 end) rental_sales 
		        ,(case when bar_acct = 'A40120' then allocated_amt else 0 end) destroy_in_field  
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40111' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40310' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40120' then allocated_amt else 0 end)  as product_sales 
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) as billable_service_revenue  
		        ,(case when bar_acct = 'A40510' then allocated_amt else 0 end) contract_service_revenue   
		        ,(case when bar_acct = 'A40610' then allocated_amt else 0 end) install_revenue  
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40610' then allocated_amt else 0 end) as service_and_installation_sales   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) franchise_revenue   
		        ,(case when bar_acct = 'A40910' then allocated_amt else 0 end) royalty_revenue   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40910' then allocated_amt else 0 end) as other_sales_revenue_total  
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) as returns_domestic  
		        ,(case when bar_acct = 'A41210' then allocated_amt else 0 end) as returns_export   
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end)  as sales_returns 
		        ,(case when bar_acct = 'A40115' then allocated_amt else 0 end) as rsa_and_price_adjustments
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40115' then allocated_amt else 0 end) as allowances_total
		        ,(case when bar_acct = 'A43112' then allocated_amt else 0 end) as rebates
		        ,(case when bar_acct = 'A43116' then allocated_amt else 0 end) as coop_advertising
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) as cashdiscount_domestic
		        ,(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cashdiscount_export
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) +
		          	(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cash_discount_total 
		        ,(case when bar_acct = 'A43111' then allocated_amt else 0 end) as fillrate_fine
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) as fob_deductions
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) as discounts_allow_domestic
		        ,(case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_allow_export
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		         (case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_and_allowexcl_coop_adv
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) as freight_domestic
		        ,(case when bar_acct = 'A42210' then allocated_amt else 0 end) as freight_export
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) as sales_freight
		        ,(case when bar_acct = 'A43120' then allocated_amt else 0 end) as customer_considerations
		        ,(case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_adjustments_other
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A43120' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_deduction_other
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) as standard_material_cost_domestic
		        ,(case when bar_acct = 'A60111' then allocated_amt else 0 end) as standard_material_cost_fob
		        ,(case when bar_acct = 'A60210' then allocated_amt else 0 end) as standard_material_cost_export
		        ,(case when bar_acct = 'A60112' then allocated_amt else 0 end) as standard_material_cost_serv_install
		        ,(case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost_serv_install_3p
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60111' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60210' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60112' then allocated_amt else 0 end) +
			         (case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) as cos_trd_domestic_labor
		        ,(case when bar_acct = 'A61210' then allocated_amt else 0 end) as cos_trd_export_labor
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) as std_labor_cost_manuf
		        ,(case when bar_acct = 'A60410' then allocated_amt else 0 end) as cos_service
		        ,(case when bar_acct = 'A60510' then allocated_amt else 0 end) as cos_monitoring
		        ,(case when bar_acct = 'A60610' then allocated_amt else 0 end) as cos_installations
		        ,(case when bar_acct = 'A60612' then allocated_amt else 0 end) as std_labor_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos_serv_install_3p
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60410' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60610' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A60612' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) as std_oh_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_oh_cos_serv_install_3p
		        ,(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos_export
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62613' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) as free_goods_cos
		        ,(case when bar_acct = 'A60115' then allocated_amt else 0 end) as targeted_funds_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A60115' then allocated_amt else 0 end) as free_goods_total
		        ,(case when bar_acct = 'A60710' then allocated_amt else 0 end) as cos_freight_outbound
		        ,(case when bar_acct = 'A60114' then allocated_amt else 0 end) as merchandising_cos
		        ,(case when bar_acct = 'A60310' then allocated_amt else 0 end) as rental_cos
			from 	stage.sgm_allocated_data_rule_22 f 
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= f.posting_week_enddate  and 
							dd.range_end_date >= f.posting_week_enddate	
		)as tr
		LEFT OUTER JOIN dw.dim_product dp on lower(tr.product_id) = lower(dp.product_id) 
		LEFT OUTER JOIN dw.dim_business_unit dbu on lower(tr.bar_entity) = lower(dbu.bar_entity)
		LEFT OUTER JOIN dw.dim_dataprocessing_outcome ddo 
			on 	ddo.dataprocessing_outcome_id = tr.dataprocessing_outcome_id and 
				lower(ddo.dataprocessing_phase) = lower(tr.dataprocessing_phase) 
		LEFT OUTER JOIN dw.dim_customer dc on lower(dc.customer_id) = lower(tr.customer_id)
		LEFT OUTER JOIN dw.dim_customer dc_bar on lower(dc_bar.customer_id) = lower(tr.customer_id_bar)
		LEFT OUTER JOIN dw.dim_source_system dss on lower(tr.source_system) = lower(dss.source_system)
	;

--select 	 dc.soldto_number, dc.shipto_number, dc.level11_bar, dc.level10_bar, dc.level09_bar,
--		count(*), sum(amt)
--from 	fact_pnl_commercial_allocation_rule_22 f
--		inner join dw.dim_customer dc on dc.customer_key = f.customer_key
--group by dc.soldto_number, dc.shipto_number, dc.level11_bar, dc.level10_bar, dc.level09_bar
--order by 1,2,3,4,5
--
--select 	dp.material, dp.product_brand, dp.bar_product, dp.level09_bar, dp.level08_bar, dp.level07_bar,
--		count(*), sum(amt)
--from 	fact_pnl_commercial_allocation_rule_22 f
--		inner join dw.dim_product dp on dp.product_key = f.product_key
--group by dp.material, dp.product_brand, dp.bar_product, dp.level09_bar, dp.level08_bar, dp.level07_bar
--order by 1,2,3,4,5

	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_commercial 
	where 	mapped_dataprocessing_ruleid = 22 and 
			posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
	INSERT INTO dw.fact_pnl_commercial (
				org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				
			    org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
				alloc_bar_product,
				
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				audit_loadts,
				dim_transactional_attributes_id
		)
		Select	org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				
			    org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
				alloc_bar_product,
				
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				getdate() as audit_loadts,
				dim_transactional_attributes_id
		from 	fact_pnl_commercial_allocation_rule_22 f
	;

exception when others then raise info 'exception occur while ingesting data in fact_pnl_commercial for rule22';
end
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_commercial_allocation_rule_23(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 
	-- call dw.p_build_fact_pnl_commercial_allocation_rule_23 (202109)
	-- call dw.p_build_fact_pnl_commercial_stacked (202109)
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date,
				cast(max(dt.dy_dte) as date) as range_end_date,
				max(dt.fmth_id) as fiscal_month_id
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
		where 	lower(rt.to_currtype) = 'usd' AND 
				lower(rt.from_currtype) = 'cad'				
	;
	drop table if exists fact_pnl_commercial_allocation_rule_23
	;
	create temporary table fact_pnl_commercial_allocation_rule_23
	diststyle even
	sortkey (posting_week_enddate)
	as
	Select 	
			 tr.*
		    ,COALESCE(dc.customer_key, dc_bar.customer_key) as customer_key
		    ,COALESCE ( 
		    	dp.product_key,
		    	(
			    	select 	dp_unk.product_key 
			    	from 	dw.dim_product dp_unk
			    	where 	dp_unk.product_id = 'unknown|unknown|unknown'
			    	limit 1
			    )
		    ) as product_key
		    ,ddo.dataprocessing_outcome_key
		    ,dbu.business_unit_key
		    ,dss.source_system_id 
		    /* agg_measures */
		    ,product_sales + service_and_installation_sales + other_sales_revenue_total as  gross_sales
		    ,allowances_total as allowances
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + allowances_total as invoice_sales
		    ,(rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as sales_deduction
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as net_sales
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) as std_cos
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos as total_cos
		    ,((product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other)) + 
		     ((standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos) as std_gross_margin
	            
	from (
		    Select 
		         f.posting_week_enddate 
		         ,f.fiscal_month_id 
		        ,f.bar_acct
		        ,f.bar_currtype 
		        ,COALESCE( f.bar_entity, 'unknown' ) as bar_entity
		        
		        ,f.org_tranagg_id
				,f.org_dataprocessing_ruleid
				,f.mapped_dataprocessing_ruleid
				,f.dataprocessing_outcome_id
				,f.dataprocessing_phase
				,f.org_bar_brand
				,f.org_bar_custno
				,f.org_bar_product
				,f.mapped_bar_brand
				,f.mapped_bar_custno
				,f.mapped_bar_product
				
		        
		        ,COALESCE( f.org_soldtocust, 'unknown' ) as org_soldtocust
		        ,COALESCE( f.org_shiptocust, 'unknown' ) as org_shiptocust
		        ,COALESCE( f.org_material, 'unknown' ) as org_material
		        ,COALESCE( f.alloc_soldtocust, 'unknown' ) as alloc_soldtocust
		        ,COALESCE( f.alloc_shiptocust, 'unknown' ) as alloc_shiptocust
		        ,COALESCE( f.alloc_material, 'unknown' ) as alloc_material
		        ,COALESCE( f.alloc_bar_product, 'unknown' ) as alloc_bar_product
		        
				,case 
					when f.dataprocessing_outcome_id = 2 then 
						f.alloc_material || '|' || f.rsa_reconcile_bar_division || '|' || f.mapped_bar_brand
					else 
						CASE
							WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NULL THEN 'unknown|unknown'
							WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NOT NULL THEN 'BA&R placeholder|' || f.alloc_bar_product
							ELSE f.alloc_material || '|' || f.alloc_bar_product
						END || '|' || COALESCE( f.mapped_bar_brand, 'unknown' )
				 end as product_id
				 
				 
				,(
					COALESCE( f.alloc_soldtocust, 'unknown' ) || '|' || 
					COALESCE( f.alloc_shiptocust, 'unknown' ) || '|' || 
					COALESCE( f.mapped_bar_custno, 'unknown' )
				 ) as customer_id
				,'unknown|unknown|' || COALESCE( f.mapped_bar_custno, 'unknown' ) as customer_id_bar
		        
		        ,cast(1 as integer) as scenario_id  -- Hard coded to Actuals - other values are future scope
		        ,source_system
		        
		        ,case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end as amt
		        ,'n/a' as uom
		        ,0 as tran_volume
		        ,0 as sales_volume
		        
		        ,(case when bar_acct = 'A40110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) sales_invoiced  
		        ,(case when bar_acct = 'A40116' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) sales_freight_income   
		        ,(case when bar_acct = 'A40210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) product_sales_export 
		        ,(case when bar_acct = 'A40110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A40116' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A40210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as product_sales_domenstic_total 
		        ,(case when bar_acct = 'A40111' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) fob_invoice_sale    
		        ,(case when bar_acct = 'A40310' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) rental_sales 
		        ,(case when bar_acct = 'A40120' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) destroy_in_field  
		        ,(case when bar_acct = 'A40110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A40116' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A40210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) +
		            (case when bar_acct = 'A40111' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) +
		            (case when bar_acct = 'A40310' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) +
		            (case when bar_acct = 'A40120' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end)  as product_sales 
		        ,(case when bar_acct = 'A40410' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as billable_service_revenue  
		        ,(case when bar_acct = 'A40510' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) contract_service_revenue   
		        ,(case when bar_acct = 'A40610' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) install_revenue  
		        ,(case when bar_acct = 'A40410' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A40510' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) +
		            (case when bar_acct = 'A40610' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as service_and_installation_sales   
		        ,(case when bar_acct = 'A40710' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) franchise_revenue   
		        ,(case when bar_acct = 'A40910' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) royalty_revenue   
		        ,(case when bar_acct = 'A40710' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A40910' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as other_sales_revenue_total  
		        ,(case when bar_acct = 'A41110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as returns_domestic  
		        ,(case when bar_acct = 'A41210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as returns_export   
		        ,(case when bar_acct = 'A41110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A41210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end)  as sales_returns 
		        ,(case when bar_acct = 'A40115' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as rsa_and_price_adjustments
		        ,(case when bar_acct = 'A41110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A41210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A40115' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as allowances_total
		        ,(case when bar_acct = 'A43112' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as rebates
		        ,(case when bar_acct = 'A43116' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as coop_advertising
		        ,(case when bar_acct = 'A43115' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as cashdiscount_domestic
		        ,(case when bar_acct = 'A43215' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as cashdiscount_export
		        ,(case when bar_acct = 'A43115' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) +
		          	(case when bar_acct = 'A43215' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as cash_discount_total 
		        ,(case when bar_acct = 'A43111' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as fillrate_fine
		        ,(case when bar_acct = 'A43130' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as fob_deductions
		        ,(case when bar_acct = 'A43110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as discounts_allow_domestic
		        ,(case when bar_acct = 'A43210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as discounts_allow_export
		        ,(case when bar_acct = 'A43110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		         (case when bar_acct = 'A43210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as discounts_and_allowexcl_coop_adv
		        ,(case when bar_acct = 'A42110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as freight_domestic
		        ,(case when bar_acct = 'A42210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as freight_export
		        ,(case when bar_acct = 'A42110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A42210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as sales_freight
		        ,(case when bar_acct = 'A43120' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as customer_considerations
		        ,(case when bar_acct = 'A43117' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as sales_adjustments_other
		        ,(case when bar_acct = 'A43130' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A43110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A43210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) +
		            (case when bar_acct = 'A42110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A42210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) +
		            (case when bar_acct = 'A43120' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A43117' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as sales_deduction_other
		        ,(case when bar_acct = 'A60110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as standard_material_cost_domestic
		        ,(case when bar_acct = 'A60111' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as standard_material_cost_fob
		        ,(case when bar_acct = 'A60210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as standard_material_cost_export
		        ,(case when bar_acct = 'A60112' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as standard_material_cost_serv_install
		        ,(case when bar_acct = '000000' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as standard_material_cost_serv_install_3p
		        ,(case when bar_acct = 'A60110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) +
			         (case when bar_acct = 'A60111' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) +
			         (case when bar_acct = 'A60210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) +
			         (case when bar_acct = 'A60112' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) +
			         (case when bar_acct = '000000' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as standard_material_cost
		        ,(case when bar_acct = 'A61110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as cos_trd_domestic_labor
		        ,(case when bar_acct = 'A61210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as cos_trd_export_labor
		        ,(case when bar_acct = 'A61110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A61210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as std_labor_cost_manuf
		        ,(case when bar_acct = 'A60410' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as cos_service
		        ,(case when bar_acct = 'A60510' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as cos_monitoring
		        ,(case when bar_acct = 'A60610' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as cos_installations
		        ,(case when bar_acct = 'A60612' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as std_labor_cos_serv_install
		        ,(case when bar_acct = 'A62613' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as std_labor_cos_serv_install_3p
		        ,(case when bar_acct = 'A61110' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A61210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) +
		            (case when bar_acct = 'A60410' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) +
		            (case when bar_acct = 'A60510' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) +
		            (case when bar_acct = 'A60610' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		            (case when bar_acct = 'A60612' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) +
		            (case when bar_acct = 'A62613' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as std_labor_cos
		        ,(case when bar_acct = 'A62612' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as std_oh_cos_serv_install
		        ,(case when bar_acct = 'A62613' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as std_oh_cos_serv_install_3p
		        ,(case when bar_acct = 'A62210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as std_oh_cos_export
		        ,(case when bar_acct = 'A62612' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		        	(case when bar_acct = 'A62613' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		        	(case when bar_acct = 'A62210' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as std_oh_cos
		        ,(case when bar_acct = 'A60116' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as free_goods_cos
		        ,(case when bar_acct = 'A60115' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as targeted_funds_cos
		        ,(case when bar_acct = 'A60116' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) + 
		        	(case when bar_acct = 'A60115' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as free_goods_total
		        ,(case when bar_acct = 'A60710' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as cos_freight_outbound
		        ,(case when bar_acct = 'A60114' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as merchandising_cos
		        ,(case when bar_acct = 'A60310' then case when f.bar_currtype = 'usd' then f.allocated_amt else f.allocated_amt / rt.fxrate end else 0 end) as rental_cos
		        
		        ,lower(f.rsa_pcr) as dim_transactional_attributes_id
			from 	stage.sgm_allocated_data_rule_23 f 
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= f.posting_week_enddate  and 
							dd.range_end_date >= f.posting_week_enddate	
					left outer join vtbl_exchange_rate rt 
						on 	rt.fiscal_month_id = dd.fiscal_month_id
		)as tr
		LEFT OUTER JOIN dw.dim_product dp on lower(tr.product_id) = lower(dp.product_id) 
		LEFT OUTER JOIN dw.dim_business_unit dbu on lower(tr.bar_entity) = lower(dbu.bar_entity)
		LEFT OUTER JOIN dw.dim_dataprocessing_outcome ddo 
			on 	ddo.dataprocessing_outcome_id = tr.dataprocessing_outcome_id and 
				lower(ddo.dataprocessing_phase) = lower(tr.dataprocessing_phase) 
		LEFT OUTER JOIN dw.dim_customer dc on lower(dc.customer_id) = lower(tr.customer_id)
		LEFT OUTER JOIN dw.dim_customer dc_bar on lower(dc_bar.customer_id) = lower(tr.customer_id_bar)
		LEFT OUTER JOIN dw.dim_source_system dss on lower(tr.source_system) = lower(dss.source_system)
	;

	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_commercial 
	where 	mapped_dataprocessing_ruleid  = 23 and 
			posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;

	INSERT INTO dw.fact_pnl_commercial (
				org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				
			    org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
				alloc_bar_product,
				
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				audit_loadts,
				dim_transactional_attributes_id
		)
		Select	org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				
			    org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
				alloc_bar_product,
				
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				getdate() as audit_loadts,
				dim_transactional_attributes_id
		from 	fact_pnl_commercial_allocation_rule_23 f
	;
exception when others then raise info 'exception occur while ingesting data in fact_pnl_commercial for rule23';
end
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_commercial_allocation_rule_26(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 

	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	--5 Minutes 23 sec
	drop table if exists fact_pnl_commercial_allocation_rule_26
	;
	create temporary table fact_pnl_commercial_allocation_rule_26
	diststyle even
	sortkey (posting_week_enddate)
	as
	Select 	
			 tr.*
		    ,COALESCE(dc.customer_key, dc_bar.customer_key) as customer_key
		    ,COALESCE ( 
		    	dp.product_key,
		    	(
			    	select 	dp_unk.product_key 
			    	from 	dw.dim_product dp_unk
			    	where 	dp_unk.product_id = 'unknown|unknown|unknown'
			    	limit 1
			    )
		    ) as product_key
		    ,NULL AS dim_transactional_attributes_id
		    ,ddo.dataprocessing_outcome_key
		    ,dbu.business_unit_key
		    ,dss.source_system_id 
		    /* agg_measures */
		    ,product_sales + service_and_installation_sales + other_sales_revenue_total as  gross_sales
		    ,allowances_total as allowances
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + allowances_total as invoice_sales
		    ,(rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as sales_deduction
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as net_sales
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) as std_cos
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos as total_cos
		    ,((product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other)) + 
		     ((standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos) as std_gross_margin
	   from (
		    Select 
		         f.posting_week_enddate 
		         ,f.fiscal_month_id 
		        ,f.bar_acct
		        ,f.bar_currtype 
		        ,COALESCE( f.bar_entity, 'unknown' ) as bar_entity
		        ,f.org_tranagg_id
				,f.org_dataprocessing_ruleid
				,f.mapped_dataprocessing_ruleid
				,f.dataprocessing_outcome_id
				,f.dataprocessing_phase
				,f.org_bar_brand
				,f.org_bar_custno
				,f.org_bar_product
				,f.mapped_bar_brand
				,f.mapped_bar_custno
				,f.mapped_bar_product
			   ,COALESCE( f.org_soldtocust, 'unknown' ) as org_soldtocust
		        ,COALESCE( f.org_shiptocust, 'unknown' ) as org_shiptocust
		        ,COALESCE( f.org_material, 'unknown' ) as org_material
		        ,COALESCE( f.alloc_soldtocust, 'unknown' ) as alloc_soldtocust
		        ,COALESCE( f.alloc_shiptocust, 'unknown' ) as alloc_shiptocust
		        ,COALESCE( f.alloc_material, 'unknown' ) as alloc_material
		        ,COALESCE( f.alloc_bar_product, 'unknown' ) as alloc_bar_product
				,CASE
					WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NULL THEN 'unknown|unknown'
					WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NOT NULL THEN 'BA&R placeholder|' || f.alloc_bar_product
					ELSE f.alloc_material || '|' || f.alloc_bar_product
				 END || '|' || COALESCE( f.mapped_bar_brand, 'unknown' ) as product_id
				,(
					COALESCE( f.alloc_soldtocust, 'unknown' ) || '|' || 
					COALESCE( f.alloc_shiptocust, 'unknown' ) || '|' || 
					COALESCE( f.mapped_bar_custno, 'unknown' )
				 ) as customer_id
				,'unknown|unknown|' || COALESCE( f.mapped_bar_custno, 'unknown' ) as customer_id_bar
 		        ,cast(1 as integer) as scenario_id  -- Hard coded to Actuals - other values are future scope
		        ,source_system  
		        ,f.allocated_amt as amt
		        ,uom as uom
		        ,f.tran_volume as tran_volume
		        ,f.sales_volume as sales_volume
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) sales_invoiced  
		        ,(case when bar_acct = 'A40116' then allocated_amt else 0 end) sales_freight_income   
		        ,(case when bar_acct = 'A40210' then allocated_amt else 0 end) product_sales_export 
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) as product_sales_domenstic_total 
		        ,(case when bar_acct = 'A40111' then allocated_amt else 0 end) fob_invoice_sale    
		        ,(case when bar_acct = 'A40310' then allocated_amt else 0 end) rental_sales 
		        ,(case when bar_acct = 'A40120' then allocated_amt else 0 end) destroy_in_field  
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40111' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40310' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40120' then allocated_amt else 0 end)  as product_sales 
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) as billable_service_revenue  
		        ,(case when bar_acct = 'A40510' then allocated_amt else 0 end) contract_service_revenue   
		        ,(case when bar_acct = 'A40610' then allocated_amt else 0 end) install_revenue  
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40610' then allocated_amt else 0 end) as service_and_installation_sales   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) franchise_revenue   
		        ,(case when bar_acct = 'A40910' then allocated_amt else 0 end) royalty_revenue   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40910' then allocated_amt else 0 end) as other_sales_revenue_total  
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) as returns_domestic  
		        ,(case when bar_acct = 'A41210' then allocated_amt else 0 end) as returns_export   
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end)  as sales_returns 
		        ,(case when bar_acct = 'A40115' then allocated_amt else 0 end) as rsa_and_price_adjustments
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40115' then allocated_amt else 0 end) as allowances_total
		        ,(case when bar_acct = 'A43112' then allocated_amt else 0 end) as rebates
		        ,(case when bar_acct = 'A43116' then allocated_amt else 0 end) as coop_advertising
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) as cashdiscount_domestic
		        ,(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cashdiscount_export
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) +
		          	(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cash_discount_total 
		        ,(case when bar_acct = 'A43111' then allocated_amt else 0 end) as fillrate_fine
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) as fob_deductions
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) as discounts_allow_domestic
		        ,(case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_allow_export
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		         (case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_and_allowexcl_coop_adv
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) as freight_domestic
		        ,(case when bar_acct = 'A42210' then allocated_amt else 0 end) as freight_export
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) as sales_freight
		        ,(case when bar_acct = 'A43120' then allocated_amt else 0 end) as customer_considerations
		        ,(case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_adjustments_other
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A43120' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_deduction_other
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) as standard_material_cost_domestic
		        ,(case when bar_acct = 'A60111' then allocated_amt else 0 end) as standard_material_cost_fob
		        ,(case when bar_acct = 'A60210' then allocated_amt else 0 end) as standard_material_cost_export
		        ,(case when bar_acct = 'A60112' then allocated_amt else 0 end) as standard_material_cost_serv_install
		        ,(case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost_serv_install_3p
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60111' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60210' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60112' then allocated_amt else 0 end) +
			         (case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) as cos_trd_domestic_labor
		        ,(case when bar_acct = 'A61210' then allocated_amt else 0 end) as cos_trd_export_labor
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) as std_labor_cost_manuf
		        ,(case when bar_acct = 'A60410' then allocated_amt else 0 end) as cos_service
		        ,(case when bar_acct = 'A60510' then allocated_amt else 0 end) as cos_monitoring
		        ,(case when bar_acct = 'A60610' then allocated_amt else 0 end) as cos_installations
		        ,(case when bar_acct = 'A60612' then allocated_amt else 0 end) as std_labor_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos_serv_install_3p
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60410' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60610' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A60612' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) as std_oh_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_oh_cos_serv_install_3p
		        ,(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos_export
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62613' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) as free_goods_cos
		        ,(case when bar_acct = 'A60115' then allocated_amt else 0 end) as targeted_funds_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A60115' then allocated_amt else 0 end) as free_goods_total
		        ,(case when bar_acct = 'A60710' then allocated_amt else 0 end) as cos_freight_outbound
		        ,(case when bar_acct = 'A60114' then allocated_amt else 0 end) as merchandising_cos
		        ,(case when bar_acct = 'A60310' then allocated_amt else 0 end) as rental_cos
			from 	stage.sgm_allocated_data_rule_26 f 
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= f.posting_week_enddate  and 
							dd.range_end_date >= f.posting_week_enddate	
		)as tr
		LEFT OUTER JOIN dw.dim_product dp on lower(tr.product_id) = lower(dp.product_id) 
		LEFT OUTER JOIN dw.dim_business_unit dbu on lower(tr.bar_entity) = lower(dbu.bar_entity)
		LEFT OUTER JOIN dw.dim_dataprocessing_outcome ddo 
			on 	ddo.dataprocessing_outcome_id = tr.dataprocessing_outcome_id and 
				lower(ddo.dataprocessing_phase) = lower(tr.dataprocessing_phase) 
		LEFT OUTER JOIN dw.dim_customer dc on lower(dc.customer_id) = lower(tr.customer_id)
		LEFT OUTER JOIN dw.dim_customer dc_bar on lower(dc_bar.customer_id) = lower(tr.customer_id_bar)
		LEFT OUTER JOIN dw.dim_source_system dss on lower(tr.source_system) = lower(dss.source_system)
	;

	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_commercial 
	where 	mapped_dataprocessing_ruleid = 26 and 
			posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
	INSERT INTO dw.fact_pnl_commercial (
				org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
			    org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
				alloc_bar_product,
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				audit_loadts,
				dim_transactional_attributes_id
		)
		Select	org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
			    org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
				alloc_bar_product,
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				getdate() as audit_loadts,
				dim_transactional_attributes_id
		from 	fact_pnl_commercial_allocation_rule_26 f
	;
exception when others then raise info 'exception occur while ingesting data in fact_pnl_commercial for rule26';
end
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_commercial_allocation_rule_27(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 

	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	--5 Minutes 23 sec
	drop table if exists fact_pnl_commercial_allocation_rule_27
	;
	create temporary table fact_pnl_commercial_allocation_rule_27
	diststyle even
	sortkey (posting_week_enddate)
	as
	Select 	
			 tr.*
		    ,COALESCE(dc.customer_key, dc_bar.customer_key) as customer_key
		    ,COALESCE ( 
		    	dp.product_key,
		    	(
			    	select 	dp_unk.product_key 
			    	from 	dw.dim_product dp_unk
			    	where 	dp_unk.product_id = 'unknown|unknown|unknown'
			    	limit 1
			    )
		    ) as product_key
		    ,NULL AS dim_transactional_attributes_id
		    ,ddo.dataprocessing_outcome_key
		    ,dbu.business_unit_key
		    ,dss.source_system_id 
		    /* agg_measures */
		    ,product_sales + service_and_installation_sales + other_sales_revenue_total as  gross_sales
		    ,allowances_total as allowances
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + allowances_total as invoice_sales
		    ,(rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as sales_deduction
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as net_sales
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) as std_cos
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos as total_cos
		    ,((product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other)) + 
		     ((standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos) as std_gross_margin
	            
	from (
		    Select 
		         f.posting_week_enddate 
		         ,f.fiscal_month_id 
		        ,f.bar_acct
		        ,f.bar_currtype 
		        ,COALESCE( f.bar_entity, 'unknown' ) as bar_entity
		        
		        ,f.org_tranagg_id
				,f.org_dataprocessing_ruleid
				,f.mapped_dataprocessing_ruleid
				,f.dataprocessing_outcome_id
				,f.dataprocessing_phase
				,f.org_bar_brand
				,f.org_bar_custno
				,f.org_bar_product
				,f.mapped_bar_brand
				,f.mapped_bar_custno
				,f.mapped_bar_product
				
		        
		        ,COALESCE( f.org_soldtocust, 'unknown' ) as org_soldtocust
		        ,COALESCE( f.org_shiptocust, 'unknown' ) as org_shiptocust
		        ,COALESCE( f.org_material, 'unknown' ) as org_material
		        ,COALESCE( f.alloc_soldtocust, 'unknown' ) as alloc_soldtocust
		        ,COALESCE( f.alloc_shiptocust, 'unknown' ) as alloc_shiptocust
		        ,COALESCE( f.alloc_material, 'unknown' ) as alloc_material
		        ,COALESCE( f.alloc_bar_product, 'unknown' ) as alloc_bar_product
		        
				,CASE
					WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NULL THEN 'unknown|unknown'
					WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NOT NULL THEN 'BA&R placeholder|' || f.alloc_bar_product
					ELSE f.alloc_material || '|' || f.alloc_bar_product
				 END || '|' || COALESCE( f.mapped_bar_brand, 'unknown' ) as product_id
				,(
					COALESCE( f.alloc_soldtocust, 'unknown' ) || '|' || 
					COALESCE( f.alloc_shiptocust, 'unknown' ) || '|' || 
					COALESCE( f.mapped_bar_custno, 'unknown' )
				 ) as customer_id
				,'unknown|unknown|' || COALESCE( f.mapped_bar_custno, 'unknown' ) as customer_id_bar
		        
		        ,cast(1 as integer) as scenario_id  -- Hard coded to Actuals - other values are future scope
		        ,source_system  -- this is hard coded to C11 for now
		        
		        ,f.allocated_amt as amt
		        ,uom as uom
		        ,f.tran_volume as tran_volume
		        ,f.sales_volume as sales_volume
		        
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) sales_invoiced  
		        ,(case when bar_acct = 'A40116' then allocated_amt else 0 end) sales_freight_income   
		        ,(case when bar_acct = 'A40210' then allocated_amt else 0 end) product_sales_export 
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) as product_sales_domenstic_total 
		        ,(case when bar_acct = 'A40111' then allocated_amt else 0 end) fob_invoice_sale    
		        ,(case when bar_acct = 'A40310' then allocated_amt else 0 end) rental_sales 
		        ,(case when bar_acct = 'A40120' then allocated_amt else 0 end) destroy_in_field  
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40111' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40310' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40120' then allocated_amt else 0 end)  as product_sales 
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) as billable_service_revenue  
		        ,(case when bar_acct = 'A40510' then allocated_amt else 0 end) contract_service_revenue   
		        ,(case when bar_acct = 'A40610' then allocated_amt else 0 end) install_revenue  
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40610' then allocated_amt else 0 end) as service_and_installation_sales   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) franchise_revenue   
		        ,(case when bar_acct = 'A40910' then allocated_amt else 0 end) royalty_revenue   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40910' then allocated_amt else 0 end) as other_sales_revenue_total  
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) as returns_domestic  
		        ,(case when bar_acct = 'A41210' then allocated_amt else 0 end) as returns_export   
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end)  as sales_returns 
		        ,(case when bar_acct = 'A40115' then allocated_amt else 0 end) as rsa_and_price_adjustments
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40115' then allocated_amt else 0 end) as allowances_total
		        ,(case when bar_acct = 'A43112' then allocated_amt else 0 end) as rebates
		        ,(case when bar_acct = 'A43116' then allocated_amt else 0 end) as coop_advertising
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) as cashdiscount_domestic
		        ,(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cashdiscount_export
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) +
		          	(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cash_discount_total 
		        ,(case when bar_acct = 'A43111' then allocated_amt else 0 end) as fillrate_fine
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) as fob_deductions
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) as discounts_allow_domestic
		        ,(case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_allow_export
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		         (case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_and_allowexcl_coop_adv
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) as freight_domestic
		        ,(case when bar_acct = 'A42210' then allocated_amt else 0 end) as freight_export
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) as sales_freight
		        ,(case when bar_acct = 'A43120' then allocated_amt else 0 end) as customer_considerations
		        ,(case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_adjustments_other
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A43120' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_deduction_other
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) as standard_material_cost_domestic
		        ,(case when bar_acct = 'A60111' then allocated_amt else 0 end) as standard_material_cost_fob
		        ,(case when bar_acct = 'A60210' then allocated_amt else 0 end) as standard_material_cost_export
		        ,(case when bar_acct = 'A60112' then allocated_amt else 0 end) as standard_material_cost_serv_install
		        ,(case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost_serv_install_3p
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60111' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60210' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60112' then allocated_amt else 0 end) +
			         (case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) as cos_trd_domestic_labor
		        ,(case when bar_acct = 'A61210' then allocated_amt else 0 end) as cos_trd_export_labor
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) as std_labor_cost_manuf
		        ,(case when bar_acct = 'A60410' then allocated_amt else 0 end) as cos_service
		        ,(case when bar_acct = 'A60510' then allocated_amt else 0 end) as cos_monitoring
		        ,(case when bar_acct = 'A60610' then allocated_amt else 0 end) as cos_installations
		        ,(case when bar_acct = 'A60612' then allocated_amt else 0 end) as std_labor_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos_serv_install_3p
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60410' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60610' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A60612' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) as std_oh_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_oh_cos_serv_install_3p
		        ,(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos_export
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62613' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) as free_goods_cos
		        ,(case when bar_acct = 'A60115' then allocated_amt else 0 end) as targeted_funds_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A60115' then allocated_amt else 0 end) as free_goods_total
		        ,(case when bar_acct = 'A60710' then allocated_amt else 0 end) as cos_freight_outbound
		        ,(case when bar_acct = 'A60114' then allocated_amt else 0 end) as merchandising_cos
		        ,(case when bar_acct = 'A60310' then allocated_amt else 0 end) as rental_cos
			from 	stage.sgm_allocated_data_rule_27 f 
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= f.posting_week_enddate  and 
							dd.range_end_date >= f.posting_week_enddate	
		)as tr
		LEFT OUTER JOIN dw.dim_product dp on lower(tr.product_id) = lower(dp.product_id) 
		LEFT OUTER JOIN dw.dim_business_unit dbu on lower(tr.bar_entity) = lower(dbu.bar_entity)
		LEFT OUTER JOIN dw.dim_dataprocessing_outcome ddo 
			on 	ddo.dataprocessing_outcome_id = tr.dataprocessing_outcome_id and 
				lower(ddo.dataprocessing_phase) = lower(tr.dataprocessing_phase) 
		LEFT OUTER JOIN dw.dim_customer dc on lower(dc.customer_id) = lower(tr.customer_id)
		LEFT OUTER JOIN dw.dim_customer dc_bar on lower(dc_bar.customer_id) = lower(tr.customer_id_bar)
		LEFT OUTER JOIN dw.dim_source_system dss on lower(tr.source_system) = lower(dss.source_system)
	;

--select 	 dc.soldto_number, dc.shipto_number, dc.level11_bar, dc.level10_bar, dc.level09_bar,
--		count(*), sum(amt)
--from 	fact_pnl_commercial_allocation_rule_22 f
--		inner join dw.dim_customer dc on dc.customer_key = f.customer_key
--group by dc.soldto_number, dc.shipto_number, dc.level11_bar, dc.level10_bar, dc.level09_bar
--order by 1,2,3,4,5
--
--select 	dp.material, dp.product_brand, dp.bar_product, dp.level09_bar, dp.level08_bar, dp.level07_bar,
--		count(*), sum(amt)
--from 	fact_pnl_commercial_allocation_rule_22 f
--		inner join dw.dim_product dp on dp.product_key = f.product_key
--group by dp.material, dp.product_brand, dp.bar_product, dp.level09_bar, dp.level08_bar, dp.level07_bar
--order by 1,2,3,4,5

	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_commercial 
	where 	mapped_dataprocessing_ruleid = 27 and 
			posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
	INSERT INTO dw.fact_pnl_commercial (
				org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				
			    org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
				alloc_bar_product,
				
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				audit_loadts,
				dim_transactional_attributes_id
				
		)
		Select	org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				
			    org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
				alloc_bar_product,
				
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				getdate() as audit_loadts,
				dim_transactional_attributes_id
		from 	fact_pnl_commercial_allocation_rule_27 f
	;

exception when others then raise info 'exception occur while ingesting data in fact_pnl_commercial for rule27';
end
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_commercial_allocation_rule_28(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 

	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	--5 Minutes 23 sec
	drop table if exists fact_pnl_commercial_allocation_rule_28
	;
	create temporary table fact_pnl_commercial_allocation_rule_28
	diststyle even
	sortkey (posting_week_enddate)
	as
	Select 	
			 tr.*
		    ,COALESCE(dc.customer_key, dc_bar.customer_key) as customer_key
		    ,COALESCE ( 
		    	dp.product_key,
		    	(
			    	select 	dp_unk.product_key 
			    	from 	dw.dim_product dp_unk
			    	where 	dp_unk.product_id = 'unknown|unknown|unknown'
			    	limit 1
			    )
		    ) as product_key
		    ,NULL AS dim_transactional_attributes_id
		    ,ddo.dataprocessing_outcome_key
		    ,dbu.business_unit_key
		    ,dss.source_system_id 
		    /* agg_measures */
		    ,product_sales + service_and_installation_sales + other_sales_revenue_total as  gross_sales
		    ,allowances_total as allowances
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + allowances_total as invoice_sales
		    ,(rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as sales_deduction
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as net_sales
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) as std_cos
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos as total_cos
		    ,((product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other)) + 
		     ((standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos) as std_gross_margin
	            
	from (
		    Select 
		         f.posting_week_enddate 
		         ,f.fiscal_month_id 
		        ,f.bar_acct
		        ,f.bar_currtype 
		        ,COALESCE( f.bar_entity, 'unknown' ) as bar_entity
		        
		        ,f.org_tranagg_id
				,f.org_dataprocessing_ruleid
				,f.mapped_dataprocessing_ruleid
				,f.dataprocessing_outcome_id
				,f.dataprocessing_phase
				,f.org_bar_brand
				,f.org_bar_custno
				,f.org_bar_product
				,f.mapped_bar_brand
				,f.mapped_bar_custno
				,f.mapped_bar_product
				
		        
		        ,COALESCE( f.org_soldtocust, 'unknown' ) as org_soldtocust
		        ,COALESCE( f.org_shiptocust, 'unknown' ) as org_shiptocust
		        ,COALESCE( f.org_material, 'unknown' ) as org_material
		        ,COALESCE( f.alloc_soldtocust, 'unknown' ) as alloc_soldtocust
		        ,COALESCE( f.alloc_shiptocust, 'unknown' ) as alloc_shiptocust
		        ,COALESCE( f.alloc_material, 'unknown' ) as alloc_material
		        ,COALESCE( f.alloc_bar_product, 'unknown' ) as alloc_bar_product
		        
				,CASE
					WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NULL THEN 'unknown|unknown'
					WHEN f.alloc_material IS NULL AND f.alloc_bar_product IS NOT NULL THEN 'BA&R placeholder|' || f.alloc_bar_product
					ELSE f.alloc_material || '|' || f.alloc_bar_product
				 END || '|' || COALESCE( f.mapped_bar_brand, 'unknown' ) as product_id
				,(
					COALESCE( f.alloc_soldtocust, 'unknown' ) || '|' || 
					COALESCE( f.alloc_shiptocust, 'unknown' ) || '|' || 
					COALESCE( f.mapped_bar_custno, 'unknown' )
				 ) as customer_id
				,'unknown|unknown|' || COALESCE( f.mapped_bar_custno, 'unknown' ) as customer_id_bar
		        
		        ,cast(1 as integer) as scenario_id  -- Hard coded to Actuals - other values are future scope
		        ,source_system  -- this is hard coded to C11 for now
		        
		        ,f.allocated_amt as amt
		        ,uom as uom
		        ,f.tran_volume as tran_volume
		        ,f.sales_volume as sales_volume
		        
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) sales_invoiced  
		        ,(case when bar_acct = 'A40116' then allocated_amt else 0 end) sales_freight_income   
		        ,(case when bar_acct = 'A40210' then allocated_amt else 0 end) product_sales_export 
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) as product_sales_domenstic_total 
		        ,(case when bar_acct = 'A40111' then allocated_amt else 0 end) fob_invoice_sale    
		        ,(case when bar_acct = 'A40310' then allocated_amt else 0 end) rental_sales 
		        ,(case when bar_acct = 'A40120' then allocated_amt else 0 end) destroy_in_field  
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40111' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40310' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40120' then allocated_amt else 0 end)  as product_sales 
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) as billable_service_revenue  
		        ,(case when bar_acct = 'A40510' then allocated_amt else 0 end) contract_service_revenue   
		        ,(case when bar_acct = 'A40610' then allocated_amt else 0 end) install_revenue  
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40610' then allocated_amt else 0 end) as service_and_installation_sales   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) franchise_revenue   
		        ,(case when bar_acct = 'A40910' then allocated_amt else 0 end) royalty_revenue   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40910' then allocated_amt else 0 end) as other_sales_revenue_total  
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) as returns_domestic  
		        ,(case when bar_acct = 'A41210' then allocated_amt else 0 end) as returns_export   
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end)  as sales_returns 
		        ,(case when bar_acct = 'A40115' then allocated_amt else 0 end) as rsa_and_price_adjustments
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40115' then allocated_amt else 0 end) as allowances_total
		        ,(case when bar_acct = 'A43112' then allocated_amt else 0 end) as rebates
		        ,(case when bar_acct = 'A43116' then allocated_amt else 0 end) as coop_advertising
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) as cashdiscount_domestic
		        ,(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cashdiscount_export
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) +
		          	(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cash_discount_total 
		        ,(case when bar_acct = 'A43111' then allocated_amt else 0 end) as fillrate_fine
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) as fob_deductions
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) as discounts_allow_domestic
		        ,(case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_allow_export
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		         (case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_and_allowexcl_coop_adv
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) as freight_domestic
		        ,(case when bar_acct = 'A42210' then allocated_amt else 0 end) as freight_export
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) as sales_freight
		        ,(case when bar_acct = 'A43120' then allocated_amt else 0 end) as customer_considerations
		        ,(case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_adjustments_other
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A43120' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_deduction_other
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) as standard_material_cost_domestic
		        ,(case when bar_acct = 'A60111' then allocated_amt else 0 end) as standard_material_cost_fob
		        ,(case when bar_acct = 'A60210' then allocated_amt else 0 end) as standard_material_cost_export
		        ,(case when bar_acct = 'A60112' then allocated_amt else 0 end) as standard_material_cost_serv_install
		        ,(case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost_serv_install_3p
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60111' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60210' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60112' then allocated_amt else 0 end) +
			         (case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) as cos_trd_domestic_labor
		        ,(case when bar_acct = 'A61210' then allocated_amt else 0 end) as cos_trd_export_labor
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) as std_labor_cost_manuf
		        ,(case when bar_acct = 'A60410' then allocated_amt else 0 end) as cos_service
		        ,(case when bar_acct = 'A60510' then allocated_amt else 0 end) as cos_monitoring
		        ,(case when bar_acct = 'A60610' then allocated_amt else 0 end) as cos_installations
		        ,(case when bar_acct = 'A60612' then allocated_amt else 0 end) as std_labor_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos_serv_install_3p
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60410' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60610' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A60612' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) as std_oh_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_oh_cos_serv_install_3p
		        ,(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos_export
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62613' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) as free_goods_cos
		        ,(case when bar_acct = 'A60115' then allocated_amt else 0 end) as targeted_funds_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A60115' then allocated_amt else 0 end) as free_goods_total
		        ,(case when bar_acct = 'A60710' then allocated_amt else 0 end) as cos_freight_outbound
		        ,(case when bar_acct = 'A60114' then allocated_amt else 0 end) as merchandising_cos
		        ,(case when bar_acct = 'A60310' then allocated_amt else 0 end) as rental_cos
			from 	stage.sgm_allocated_data_rule_28 f 
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= f.posting_week_enddate  and 
							dd.range_end_date >= f.posting_week_enddate	
		)as tr
		LEFT OUTER JOIN dw.dim_product dp on lower(tr.product_id) = lower(dp.product_id) 
		LEFT OUTER JOIN dw.dim_business_unit dbu on lower(tr.bar_entity) = lower(dbu.bar_entity)
		LEFT OUTER JOIN dw.dim_dataprocessing_outcome ddo 
			on 	ddo.dataprocessing_outcome_id = tr.dataprocessing_outcome_id and 
				lower(ddo.dataprocessing_phase) = lower(tr.dataprocessing_phase) 
		LEFT OUTER JOIN dw.dim_customer dc on lower(dc.customer_id) = lower(tr.customer_id)
		LEFT OUTER JOIN dw.dim_customer dc_bar on lower(dc_bar.customer_id) = lower(tr.customer_id_bar)
		LEFT OUTER JOIN dw.dim_source_system dss on lower(tr.source_system) = lower(dss.source_system)
	;

--select 	 dc.soldto_number, dc.shipto_number, dc.level11_bar, dc.level10_bar, dc.level09_bar,
--		count(*), sum(amt)
--from 	fact_pnl_commercial_allocation_rule_22 f
--		inner join dw.dim_customer dc on dc.customer_key = f.customer_key
--group by dc.soldto_number, dc.shipto_number, dc.level11_bar, dc.level10_bar, dc.level09_bar
--order by 1,2,3,4,5
--
--select 	dp.material, dp.product_brand, dp.bar_product, dp.level09_bar, dp.level08_bar, dp.level07_bar,
--		count(*), sum(amt)
--from 	fact_pnl_commercial_allocation_rule_22 f
--		inner join dw.dim_product dp on dp.product_key = f.product_key
--group by dp.material, dp.product_brand, dp.bar_product, dp.level09_bar, dp.level08_bar, dp.level07_bar
--order by 1,2,3,4,5

	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_commercial 
	where 	mapped_dataprocessing_ruleid = 28 and 
			posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
	INSERT INTO dw.fact_pnl_commercial (
				org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				
			    org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
				alloc_bar_product,
				
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				audit_loadts,
				dim_transactional_attributes_id
		)
		Select	org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				
			    org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
				alloc_bar_product,
				
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				getdate() as audit_loadts,
				dim_transactional_attributes_id
		from 	fact_pnl_commercial_allocation_rule_28 f
	;

exception when others then raise info 'exception occur while ingesting data in fact_pnl_commercial for rule28';
end
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_commercial_not_allocated(fmthid integer)
 LANGUAGE plpgsql
AS $$
--DECLARE variables
BEGIN 
	
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	--5 Minutes 23 sec
	drop table if exists fact_pnl_commercial_allocation_rule_catchall
	;
	create temporary table fact_pnl_commercial_allocation_rule_catchall
	diststyle even
	sortkey (posting_week_enddate)
	as
	Select 	
			 tr.*
		    ,COALESCE(dc.customer_key, dc_bar.customer_key) as customer_key
		    ,COALESCE ( 
		    	dp.product_key,
		    	(
			    	select 	dp_unk.product_key 
			    	from 	dw.dim_product dp_unk
			    	where 	dp_unk.product_id = 'unknown|unknown|unknown'
			    	limit 1
			    )
		    ) as product_key
	    	    ,NULL as dim_transactional_attributes_id
		    ,ddo.dataprocessing_outcome_key
		    ,dbu.business_unit_key
		    ,dss.source_system_id 
		    /* agg_measures */
		    ,product_sales + service_and_installation_sales + other_sales_revenue_total as  gross_sales
		    ,allowances_total as allowances
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + allowances_total as invoice_sales
		    ,(rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as sales_deduction
		    ,(product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other) as net_sales
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) as std_cos
		    ,(standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos as total_cos
		    ,((product_sales + service_and_installation_sales + other_sales_revenue_total) + 
		        allowances_total + 
		        (rebates + coop_advertising + cash_discount_total + fillrate_fine + sales_deduction_other)) + 
		     ((standard_material_cost + std_labor_cos + std_oh_cos) + 
		            free_goods_total + cos_freight_outbound + merchandising_cos + rental_cos) as std_gross_margin
	            
	from (
		    Select 
		         f.posting_week_enddate 
		         ,f.fiscal_month_id 
		        ,f.bar_acct
		        ,f.bar_currtype 
		        ,COALESCE( f.bar_entity, 'unknown' ) as bar_entity
		        
		          ,f.org_tranagg_id
				,f.org_dataprocessing_ruleid
				,f.mapped_dataprocessing_ruleid
				---when perfect data then outcome 0 else 2
				,case when mapped_dataprocessing_ruleid =1 then 0 else 2 end as dataprocessing_outcome_id
				,case when mapped_dataprocessing_ruleid =1 then 'phase 0' else 'phase 100' end  as dataprocessing_phase
				,f.org_bar_brand
				,f.org_bar_custno
				,f.org_bar_product
				,f.mapped_bar_brand
				,f.mapped_bar_custno
				,f.mapped_bar_product
       
		        ,COALESCE( f.soldtocust, 'unknown' ) as org_soldtocust
		        ,COALESCE( f.shiptocust, 'unknown' ) as org_shiptocust
		        ,COALESCE( f.material, 'unknown' ) as org_material
		        ,COALESCE( f.soldtocust, 'unknown' ) as alloc_soldtocust
		        ,COALESCE( f.shiptocust, 'unknown' ) as alloc_shiptocust
		        ,COALESCE( f.material, 'unknown' ) as alloc_material
		        ,COALESCE (f.mapped_bar_product,'unknown') as alloc_bar_product
		        
				,CASE
					WHEN f.material IS NULL AND f.mapped_bar_product IS NULL THEN 'unknown|unknown'
					WHEN f.material IS NULL AND f.mapped_bar_product IS NOT NULL THEN 'BA&R placeholder|' || f.mapped_bar_product
					ELSE f.material || '|' || f.mapped_bar_product
				 END || '|' || COALESCE( f.mapped_bar_brand, 'unknown' ) as product_id
				 
				,(
					COALESCE( f.soldtocust, 'unknown' ) || '|' || 
					COALESCE( f.shiptocust, 'unknown' ) || '|' || 
					COALESCE( f.mapped_bar_custno, 'unknown' )
				 ) as customer_id
				,'unknown|unknown|' || COALESCE( f.mapped_bar_custno, 'unknown' ) as customer_id_bar
		        
		        ,cast(1 as integer) as scenario_id  -- Hard coded to Actuals - other values are future scope
		              
		        ,f.allocated_amt as amt
		        ,uom as uom
		        ,tran_volume
		        ,sales_volume
		        ,audit_rec_src
		        
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) sales_invoiced  
		        ,(case when bar_acct = 'A40116' then allocated_amt else 0 end) sales_freight_income   
		        ,(case when bar_acct = 'A40210' then allocated_amt else 0 end) product_sales_export 
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) as product_sales_domenstic_total 
		        ,(case when bar_acct = 'A40111' then allocated_amt else 0 end) fob_invoice_sale    
		        ,(case when bar_acct = 'A40310' then allocated_amt else 0 end) rental_sales 
		        ,(case when bar_acct = 'A40120' then allocated_amt else 0 end) destroy_in_field  
		        ,(case when bar_acct = 'A40110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40116' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40111' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40310' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40120' then allocated_amt else 0 end)  as product_sales 
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) as billable_service_revenue  
		        ,(case when bar_acct = 'A40510' then allocated_amt else 0 end) contract_service_revenue   
		        ,(case when bar_acct = 'A40610' then allocated_amt else 0 end) install_revenue  
		        ,(case when bar_acct = 'A40410' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A40610' then allocated_amt else 0 end) as service_and_installation_sales   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) franchise_revenue   
		        ,(case when bar_acct = 'A40910' then allocated_amt else 0 end) royalty_revenue   
		        ,(case when bar_acct = 'A40710' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40910' then allocated_amt else 0 end) as other_sales_revenue_total  
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) as returns_domestic  
		        ,(case when bar_acct = 'A41210' then allocated_amt else 0 end) as returns_export   
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end)  as sales_returns 
		        ,(case when bar_acct = 'A40115' then allocated_amt else 0 end) as rsa_and_price_adjustments
		        ,(case when bar_acct = 'A41110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A41210' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A40115' then allocated_amt else 0 end) as allowances_total
		        ,(case when bar_acct = 'A43112' then allocated_amt else 0 end) as rebates
		        ,(case when bar_acct = 'A43116' then allocated_amt else 0 end) as coop_advertising
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) as cashdiscount_domestic
		        ,(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cashdiscount_export
		        ,(case when bar_acct = 'A43115' then allocated_amt else 0 end) +
		          	(case when bar_acct = 'A43215' then allocated_amt else 0 end) as cash_discount_total 
		        ,(case when bar_acct = 'A43111' then allocated_amt else 0 end) as fillrate_fine
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) as fob_deductions
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) as discounts_allow_domestic
		        ,(case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_allow_export
		        ,(case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		         (case when bar_acct = 'A43210' then allocated_amt else 0 end) as discounts_and_allowexcl_coop_adv
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) as freight_domestic
		        ,(case when bar_acct = 'A42210' then allocated_amt else 0 end) as freight_export
		        ,(case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) as sales_freight
		        ,(case when bar_acct = 'A43120' then allocated_amt else 0 end) as customer_considerations
		        ,(case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_adjustments_other
		        ,(case when bar_acct = 'A43130' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A42110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A42210' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43120' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A43117' then allocated_amt else 0 end) as sales_deduction_other
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) as standard_material_cost_domestic
		        ,(case when bar_acct = 'A60111' then allocated_amt else 0 end) as standard_material_cost_fob
		        ,(case when bar_acct = 'A60210' then allocated_amt else 0 end) as standard_material_cost_export
		        ,(case when bar_acct = 'A60112' then allocated_amt else 0 end) as standard_material_cost_serv_install
		        ,(case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost_serv_install_3p
		        ,(case when bar_acct = 'A60110' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60111' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60210' then allocated_amt else 0 end) +
			         (case when bar_acct = 'A60112' then allocated_amt else 0 end) +
			         (case when bar_acct = '000000' then allocated_amt else 0 end) as standard_material_cost
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) as cos_trd_domestic_labor
		        ,(case when bar_acct = 'A61210' then allocated_amt else 0 end) as cos_trd_export_labor
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) as std_labor_cost_manuf
		        ,(case when bar_acct = 'A60410' then allocated_amt else 0 end) as cos_service
		        ,(case when bar_acct = 'A60510' then allocated_amt else 0 end) as cos_monitoring
		        ,(case when bar_acct = 'A60610' then allocated_amt else 0 end) as cos_installations
		        ,(case when bar_acct = 'A60612' then allocated_amt else 0 end) as std_labor_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos_serv_install_3p
		        ,(case when bar_acct = 'A61110' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A61210' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60410' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60510' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A60610' then allocated_amt else 0 end) + 
		            (case when bar_acct = 'A60612' then allocated_amt else 0 end) +
		            (case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_labor_cos
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) as std_oh_cos_serv_install
		        ,(case when bar_acct = 'A62613' then allocated_amt else 0 end) as std_oh_cos_serv_install_3p
		        ,(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos_export
		        ,(case when bar_acct = 'A62612' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62613' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A62210' then allocated_amt else 0 end) as std_oh_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) as free_goods_cos
		        ,(case when bar_acct = 'A60115' then allocated_amt else 0 end) as targeted_funds_cos
		        ,(case when bar_acct = 'A60116' then allocated_amt else 0 end) + 
		        	(case when bar_acct = 'A60115' then allocated_amt else 0 end) as free_goods_total
		        ,(case when bar_acct = 'A60710' then allocated_amt else 0 end) as cos_freight_outbound
		        ,(case when bar_acct = 'A60114' then allocated_amt else 0 end) as merchandising_cos
		        ,(case when bar_acct = 'A60310' then allocated_amt else 0 end) as rental_cos
			from (	
					Select *, bar_amt as allocated_amt 
					from stage.bods_core_transaction_agg
					where mapped_dataprocessing_ruleid not in (9,13,21,22,26,23,27,28)
				 ) f 
					inner join vtbl_date_range dd 
						on 	dd.range_start_date <= f.posting_week_enddate  and 
							dd.range_end_date >= f.posting_week_enddate
		)as tr
		LEFT OUTER JOIN dw.dim_product dp on lower(tr.product_id) = lower(dp.product_id) 
		LEFT OUTER JOIN dw.dim_business_unit dbu on lower(tr.bar_entity) = lower(dbu.bar_entity)
		LEFT OUTER JOIN dw.dim_dataprocessing_outcome ddo 
			on 	ddo.dataprocessing_outcome_id = tr.dataprocessing_outcome_id and 
				lower(ddo.dataprocessing_phase) = lower(tr.dataprocessing_phase) 
		LEFT OUTER JOIN dw.dim_customer dc on lower(dc.customer_id) = lower(tr.customer_id)
		LEFT OUTER JOIN dw.dim_customer dc_bar on lower(dc_bar.customer_id) = lower(tr.customer_id_bar)
		LEFT OUTER JOIN dw.dim_source_system dss on lower(tr.audit_rec_src) = lower(dss.source_system) 
	;
---2,181,121,264,785,418.943
--select count(1), sum(bar_amt),tr.customer_id_bar
--from (select *,CASE
--					WHEN f.material IS NULL AND f.mapped_bar_product IS NULL THEN 'unknown|unknown'
--					WHEN f.material IS NULL AND f.mapped_bar_product IS NOT NULL THEN 'BA&R placeholder|' || f.mapped_bar_product
--					ELSE f.material || '|' || f.mapped_bar_product
--				 END || '|' || COALESCE( f.mapped_bar_brand, 'unknown' ) as product_id
--				 
--				,(
--					COALESCE( f.soldtocust, 'unknown' ) || '|' || 
--					COALESCE( f.shiptocust, 'unknown' ) || '|' || 
--					COALESCE( f.mapped_bar_custno, 'unknown' )
--				 ) as customer_id
--				,'unknown|unknown|' || COALESCE( f.mapped_bar_custno, 'unknown' ) as customer_id_bar
--		from stage.bods_core_transaction_agg f
--		where org_tranagg_id = 946839728
--		) tr
--LEFT OUTER JOIN dw.dim_product dp on lower(tr.product_id) = lower(dp.product_id) 
--		LEFT OUTER JOIN dw.dim_business_unit dbu on lower(tr.bar_entity) = lower(dbu.bar_entity)
----		LEFT OUTER JOIN dw.dim_dataprocessing_outcome ddo 
----			on 	ddo.dataprocessing_outcome_id = tr.dataprocessing_outcome_id and 
----				lower(ddo.dataprocessing_phase) = lower(tr.dataprocessing_phase) 
--		LEFT OUTER JOIN dw.dim_customer dc on lower(dc.customer_id) = lower(tr.customer_id)
--		LEFT OUTER JOIN dw.dim_customer dc_bar on lower(dc_bar.customer_id) = lower(tr.customer_id_bar)
--		LEFT OUTER JOIN dw.dim_source_system dss on lower(tr.audit_rec_src) = lower(dss.source_system) 
--group by tr.customer_id_bar

	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_commercial 
	where 	mapped_dataprocessing_ruleid not in  (9,13,21,22,26,23,27,28) and 
			posting_week_enddate between 
			(select range_start_date from vtbl_date_range) and 
			(select range_end_date from vtbl_date_range)
	;
	INSERT INTO dw.fact_pnl_commercial (
				org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
			    alloc_bar_product,
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				audit_loadts,
				dim_transactional_attributes_id
		)
		Select	org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
			    alloc_bar_product,
				amt,
				tran_volume,
				sales_volume,
				uom,
				sales_invoiced,
				sales_freight_income,
				product_sales_export,
				product_sales_domenstic_total,
				fob_invoice_sale,
				rental_sales,
				destroy_in_field,
				product_sales,
				billable_service_revenue,
				contract_service_revenue,
				install_revenue,
				service_and_installation_sales,
				franchise_revenue,
				royalty_revenue,
				other_sales_revenue_total,
				returns_domestic,
				returns_export,
				sales_returns,
				rsa_and_price_adjustments,
				allowances_total,
				rebates,
				coop_advertising,
				cashdiscount_domestic,
				cashdiscount_export,
				cash_discount_total,
				fillrate_fine,
				fob_deductions,
				discounts_allow_domestic,
				discounts_allow_export,
				discounts_and_allowexcl_coop_adv,
				freight_domestic,
				freight_export,
				sales_freight,
				customer_considerations,
				sales_adjustments_other,
				sales_deduction_other,
				standard_material_cost_domestic,
				standard_material_cost_fob,
				standard_material_cost_export,
				standard_material_cost_serv_install,
				standard_material_cost_serv_install_3p,
				standard_material_cost,
				cos_trd_domestic_labor,
				cos_trd_export_labor,
				std_labor_cost_manuf,
				cos_service,
				cos_monitoring,
				cos_installations,
				std_labor_cos_serv_install,
				std_labor_cos_serv_install_3p,
				std_labor_cos,
				std_oh_cos_serv_install,
				std_oh_cos_serv_install_3p,
				std_oh_cos_export,
				std_oh_cos,
				free_goods_cos,
				targeted_funds_cos,
				free_goods_total,
				cos_freight_outbound,
				merchandising_cos,
				rental_cos,
				gross_sales,
				allowances,
				invoice_sales,
				sales_deduction,
				net_sales,
				std_cos,
				total_cos,
				std_gross_margin,
				getdate() as audit_loadts,
				dim_transactional_attributes_id
		from 	fact_pnl_commercial_allocation_rule_catchall f
	;

exception when others then raise info 'exception occur while ingesting data in fact_pnl_commercial not_allocated';
end
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_commercial_orig(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 

	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
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
		where 	lower(rt.to_currtype) = 'usd'
	;
	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_commercial_orig 
	where 	posting_week_enddate between 
			(select range_start_date from vtbl_date_range) and 
			(select range_end_date from vtbl_date_range)
	;
	
	
	INSERT INTO dw.fact_pnl_commercial_orig (
				org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_currtype,
				amt,
				amt_usd,
				tran_volume,
				sales_volume,
				uom
		)
		Select	fpc.org_tranagg_id,
				fpc.posting_week_enddate,
				fpc.fiscal_month_id,
				fpc.bar_currtype,
				fpc.bar_amt as amt,
				case 
					when rt.fxrate is not null then rt.fxrate * fpc.bar_amt 
					else fpc.bar_amt 
				end as amt_usd,
				fpc.tran_volume,
				fpc.sales_volume,
				fpc.uom
		from 	stage.bods_core_transaction_agg fpc 
				left outer join vtbl_exchange_rate rt
					on 	rt.fiscal_month_id = fpc.fiscal_month_id and 
						lower(rt.from_currtype) = lower(fpc.bar_currtype)
		where 	fpc.mapped_dataprocessing_ruleid != 1 and
				fpc.posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
EXCEPTION
	when others then raise info 'exception occur while building fact_pnl_commercial_orig';
END;
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_commercial_stacked(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 

	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date
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
		where 	lower(rt.to_currtype) = 'usd'
	;
	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_commercial_stacked 
	where 	posting_week_enddate between 
			(select range_start_date from vtbl_date_range) and 
			(select range_end_date from vtbl_date_range)
	;
	
	
	INSERT INTO dw.fact_pnl_commercial_stacked (
				org_tranagg_id,
				posting_week_enddate,
				fiscal_month_id,
				bar_acct,
				bar_currtype,
				customer_key,
				product_key,
				org_dataprocessing_ruleid,
				mapped_dataprocessing_ruleid,
				dataprocessing_outcome_key,
				business_unit_key,
				scenario_id,
				source_system_id,
				org_bar_custno,
				org_bar_product,
				org_bar_brand,
				mapped_bar_custno,
				mapped_bar_product,
				mapped_bar_brand,
				org_soldtocust, 
			    org_shiptocust,
			    org_material,
			    alloc_soldtocust, 
			    alloc_shiptocust, 
			    alloc_material,
			    alloc_bar_product,
			    allocated_flag,
				amt,
				amt_usd,
				tran_volume,
				sales_volume,
				uom,
				dim_transactional_attributes_id
		)
		Select	fpc.org_tranagg_id,
				fpc.posting_week_enddate,
				fpc.fiscal_month_id,
				fpc.bar_acct,
				fpc.bar_currtype,
				fpc.customer_key,
			    fpc.product_key,
				fpc.org_dataprocessing_ruleid,
				fpc.mapped_dataprocessing_ruleid,
				fpc.dataprocessing_outcome_key,
				fpc.business_unit_key,
				fpc.scenario_id,
				fpc.source_system_id,
				fpc.org_bar_custno,
				fpc.org_bar_product,
				fpc.org_bar_brand,
				fpc.mapped_bar_custno,
				fpc.mapped_bar_product,
				fpc.mapped_bar_brand,
				fpc.org_soldtocust, 
			    fpc.org_shiptocust,
			    fpc.org_material,
			    fpc.alloc_soldtocust, 
			    fpc.alloc_shiptocust, 
			    fpc.alloc_material,
			    fpc.alloc_bar_product,
			    case when fpc.mapped_dataprocessing_ruleid != 1 
			         then true
			         when fpc.mapped_dataprocessing_ruleid =1 
			     	    and (dc.level11_bar = 'Customer_None' or dp.level09_bar = 'Product_None' )
			    	    then true
			    	else false
			    	end as allocated_flag,
				fpc.amt,
				case 
					when rt.fxrate is not null then rt.fxrate * fpc.amt 
					else fpc.amt 
				end as amt_usd,
				fpc.tran_volume,
				fpc.sales_volume,
				fpc.uom,
				fpc.dim_transactional_attributes_id
		from 	dw.fact_pnl_commercial fpc 
				left outer join vtbl_exchange_rate rt
					on 	rt.fiscal_month_id = fpc.fiscal_month_id and 
						lower(rt.from_currtype) = lower(fpc.bar_currtype)
				left join dw.dim_customer dc on fpc.customer_key = dc.customer_key 
				left join dw.dim_product dp on fpc.product_key = dp.product_key 
		where 	fpc.posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
EXCEPTION
	when others then raise info 'exception occur while building fact_pnl_commercial_stacked';
END;
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_ocos_allocation_rule_100(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 
/*
 * 		call dw.p_build_fact_pnl_ocos_allocation_rule_100(202101)
 * 		grant execute on procedure dw.p_build_fact_pnl_ocos_allocation_rule_100(fmthid integer) to group "g-ada-rsabible-sb-ro";
 * 		select count(*) from dw.fact_pnl_ocos where dataprocessing_ruleid = 100;
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
	drop table if exists tmp_fact_pnl_ocos_allocation_rule_100
	;
	create temporary table tmp_fact_pnl_ocos_allocation_rule_100
	diststyle even
	sortkey (posting_week_enddate)
	AS
		SELECT 	tr.posting_week_enddate,
				tr.fiscal_month_id, 
				tr.bar_acct,
				tr.bar_currtype,
				tr.bar_entity,
				tr.dataprocessing_ruleid,
				tr.dataprocessing_outcome_id,
				tr.dataprocessing_phase,
				tr.material,
				tr.bar_product,
				tr.bar_brand,
				tr.soldtocust,
				tr.shiptocust,
				tr.bar_custno,
				tr.product_id,
				tr.cost_pool,
				tr.super_sbu,
				tr.customer_id,
				tr.scenario_id,
				tr.amt,
				tr.amt_usd,
				
				dp.product_key,
				dc.customer_key,
				dbu.business_unit_key,
				ddo.dataprocessing_outcome_key,
				dss.source_system_id,
				
				tr.amt as reported_inventory_adjustment,
				0 as reported_warranty_cost,
				0 as reported_duty_tariffs,
				0 as reported_freight,
				0 as reported_ppv,
				0 as reported_labor_overhead,
				
				0 as tran_volume,
				0 as sales_volume,
				null as uom
		FROM 	(
					SELECT	 f.posting_week_enddate 
							,f.fiscal_month_id 
							,f.bar_acct
							,f.bar_currtype 
							,f.bar_entity				
							
							,f.dataprocessing_ruleid
							,f.dataprocessing_outcome_id
							,f.dataprocessing_phase
							
							,f.material
							,f.bar_product
							,f.bar_brand
							
							,f.soldtocust
							,f.shiptocust
							,f.bar_custno
							
							,f.cost_pool
							,f.super_sbu
							
							,f.material || '|' || f.bar_product || '|' || f.bar_brand as product_id
							,f.soldtocust || '|' || f.shiptocust || '|' || f.bar_custno as customer_id
							
							,cast(1 as integer) as scenario_id  -- Hard coded to Actuals
							,f.source_system
							
							,f.allocated_amt as amt
							,f.allocated_amt_usd as amt_usd
							
					from 	stage.agm_allocated_data_rule_100 f 
							inner join vtbl_date_range dd 
								on 	dd.fiscal_month_id = f.fiscal_month_id
				) as tr
				LEFT OUTER JOIN dw.dim_product dp on lower(tr.product_id) = lower(dp.product_id)
				LEFT OUTER JOIN dw.dim_business_unit dbu on lower(tr.bar_entity) = lower(dbu.bar_entity)
				LEFT OUTER JOIN dw.dim_dataprocessing_outcome ddo 
					on 	ddo.dataprocessing_outcome_id = tr.dataprocessing_outcome_id and 
						lower(ddo.dataprocessing_phase) = lower(tr.dataprocessing_phase) 
				LEFT OUTER JOIN dw.dim_customer dc on lower(dc.customer_id) = lower(tr.customer_id)
				LEFT OUTER JOIN dw.dim_source_system dss on lower(tr.source_system) = lower(dss.source_system)
	;
--select 	bar_acct,
--		count(*) row_count,
--		sum(case when customer_key is null then 1 else 0 end) as missing_cust_key,
--		sum(case when product_key is null then 1 else 0 end) as missing_prod_key,
--		sum(case when business_unit_key is null then 1 else 0 end) as missing_bu_key,
--		sum(case when dataprocessing_outcome_key is null then 1 else 0 end) as missing_outcome_key,
--		sum(case when source_system_id is null then 1 else 0 end) as missing_source_key
--from 	tmp_fact_pnl_ocos_allocation_rule_100
--group by bar_acct
--;

	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_ocos 
	where 	dataprocessing_ruleid  = 100 and 
			posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
	/* insert statement */
	insert into dw.fact_pnl_ocos (
				org_tranagg_id,
				dataprocessing_ruleid,
				dataprocessing_outcome_key,
				
				bar_acct,
				bar_currtype,
				
				posting_week_enddate,
				fiscal_month_id,
				
				scenario_id,
				source_system_id,
				business_unit_key,
				customer_key,
				product_key,
				
				soldtocust, 
			    shiptocust,
			    bar_custno,
				
				material,
				bar_product,
				bar_brand,
				
				cost_pool,
				super_sbu,
				
				amt,
				amt_usd,
				
				reported_inventory_adjustment,
				reported_warranty_cost,
				reported_duty_tariffs,
				reported_freight,
				reported_ppv,
				reported_labor_overhead,
				
				tran_volume,
				sales_volume,
				uom,
				audit_loadts
		)
		select	-1 as org_tranagg_id,
				tmp.dataprocessing_ruleid,
				tmp.dataprocessing_outcome_key,
				
				tmp.bar_acct,
				tmp.bar_currtype,
				
				tmp.posting_week_enddate,
				tmp.fiscal_month_id,
				
				tmp.scenario_id,
				tmp.source_system_id,
				tmp.business_unit_key,
				tmp.customer_key,
				tmp.product_key,
				
				tmp.soldtocust, 
			    tmp.shiptocust,
			    tmp.bar_custno,
				
				tmp.material,
				tmp.bar_product,
				tmp.bar_brand,
				
				tmp.cost_pool,
				tmp.super_sbu,
				
				tmp.amt,
				tmp.amt_usd,
				
				tmp.reported_inventory_adjustment,
				tmp.reported_warranty_cost,
				tmp.reported_duty_tariffs,
				tmp.reported_freight,
				tmp.reported_ppv,
				tmp.reported_labor_overhead,
				
				tmp.tran_volume,
				tmp.sales_volume,
				tmp.uom,
				getdate() as audit_loadts
		from 	tmp_fact_pnl_ocos_allocation_rule_100 tmp
	;
exception when others then raise info 'exception occur while ingesting data in fact_pnl_commercial for rule13';
end
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_ocos_allocation_rule_101(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 
/*
 * 		call dw.p_build_fact_pnl_ocos_allocation_rule_101(202101)
 * 		grant execute on procedure dw.p_build_fact_pnl_ocos_allocation_rule_101(fmthid integer) to group "g-ada-rsabible-sb-ro";
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
	drop table if exists tmp_fact_pnl_ocos_allocation_rule_101
	;
	create temporary table tmp_fact_pnl_ocos_allocation_rule_101
	diststyle even
	sortkey (posting_week_enddate)
	AS
		SELECT 	tr.posting_week_enddate,
				tr.fiscal_month_id, 
				tr.bar_acct,
				tr.bar_currtype,
				tr.bar_entity,
				tr.dataprocessing_ruleid,
				tr.dataprocessing_outcome_id,
				tr.dataprocessing_phase,
				tr.material,
				tr.bar_product,
				tr.bar_brand,
				tr.soldtocust,
				tr.shiptocust,
				tr.bar_custno,
				tr.cost_pool,
				tr.super_sbu,
				tr.product_id,
				tr.customer_id,
				tr.scenario_id,
				tr.amt,
				tr.amt_usd,
				dp.product_key,
				dc.customer_key,
				dbu.business_unit_key,
				ddo.dataprocessing_outcome_key,
				dss.source_system_id,
				
				0 as reported_inventory_adjustment,
				tr.amt as reported_warranty_cost,
				0 as reported_duty_tariffs,
				0 as reported_freight,
				0 as reported_ppv,
				0 as reported_labor_overhead,
				
				0 as tran_volume,
				0 as sales_volume,
				null as uom
				
		FROM 	(
					SELECT	 f.posting_week_enddate 
							,f.fiscal_month_id 
							,f.bar_acct
							,f.bar_currtype 
							,f.bar_entity				
							
							,f.dataprocessing_ruleid
							,f.dataprocessing_outcome_id
							,f.dataprocessing_phase
							
							,f.material
							,f.bar_product
							,f.bar_brand
							
							,f.soldtocust
							,f.shiptocust
							,f.bar_custno
							,f.cost_pool
							,f.super_sbu
							
							,f.material || '|' || f.bar_product || '|' || f.bar_brand as product_id
							,f.soldtocust || '|' || f.shiptocust || '|' || f.bar_custno as customer_id
							
							,cast(1 as integer) as scenario_id  -- Hard coded to Actuals
							,f.source_system
							
							,f.allocated_amt as amt
							,f.allocated_amt_usd as amt_usd
							
					from 	stage.agm_allocated_data_rule_101 f 
							inner join vtbl_date_range dd 
								on 	dd.fiscal_month_id = f.fiscal_month_id
				) as tr
				LEFT OUTER JOIN dw.dim_product dp on lower(tr.product_id) = lower(dp.product_id) 
				LEFT OUTER JOIN dw.dim_business_unit dbu on lower(tr.bar_entity) = lower(dbu.bar_entity)
				LEFT OUTER JOIN dw.dim_dataprocessing_outcome ddo 
					on 	ddo.dataprocessing_outcome_id = tr.dataprocessing_outcome_id and 
						lower(ddo.dataprocessing_phase) = lower(tr.dataprocessing_phase) 
				LEFT OUTER JOIN dw.dim_customer dc on lower(dc.customer_id) = lower(tr.customer_id)
				LEFT OUTER JOIN dw.dim_source_system dss on lower(tr.source_system) = lower(dss.source_system)
	;

--select 	bar_acct,
--		count(*) row_count,
--		sum(case when customer_key is null then 1 else 0 end) as missing_cust_key,
--		sum(case when product_key is null then 1 else 0 end) as missing_prod_key,
--		sum(case when business_unit_key is null then 1 else 0 end) as missing_bu_key,
--		sum(case when dataprocessing_outcome_key is null then 1 else 0 end) as missing_outcome_key,
--		sum(case when source_system_id is null then 1 else 0 end) as missing_source_key
--from 	tmp_fact_pnl_ocos_allocation_rule_13
--group by bar_acct
--;

	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_ocos 
	where 	dataprocessing_ruleid  = 101 and 
			posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
	/* insert statement */
	insert into dw.fact_pnl_ocos (
				org_tranagg_id,
				dataprocessing_ruleid,
				dataprocessing_outcome_key,
				
				bar_acct,
				bar_currtype,
				
				posting_week_enddate,
				fiscal_month_id,
				
				scenario_id,
				source_system_id,
				business_unit_key,
				customer_key,
				product_key,
				
				soldtocust, 
			    shiptocust,
			    bar_custno,
			    cost_pool,
			    super_sbu,
				
				material,
				bar_product,
				bar_brand,
				
				amt,
				amt_usd,
				
				reported_inventory_adjustment,
				reported_warranty_cost,
				reported_duty_tariffs,
				reported_freight,
				reported_ppv,
				reported_labor_overhead,
				
				tran_volume,
				sales_volume,
				uom,
				audit_loadts
		)
		select	-1 as org_tranagg_id,
				tmp.dataprocessing_ruleid,
				tmp.dataprocessing_outcome_key,
				
				tmp.bar_acct,
				tmp.bar_currtype,
				
				tmp.posting_week_enddate,
				tmp.fiscal_month_id,
				
				tmp.scenario_id,
				tmp.source_system_id,
				tmp.business_unit_key,
				tmp.customer_key,
				tmp.product_key,
				
				tmp.soldtocust, 
			     tmp.shiptocust,
			     tmp.bar_custno,
			     tmp.cost_pool,
			     tmp.super_sbu,
				
				tmp.material,
				tmp.bar_product,
				tmp.bar_brand,
				
				tmp.amt,
				tmp.amt_usd,
				
				tmp.reported_inventory_adjustment,
				tmp.reported_warranty_cost,
				tmp.reported_duty_tariffs,
				tmp.reported_freight,
				tmp.reported_ppv,
				tmp.reported_labor_overhead,
				
				tmp.tran_volume,
				tmp.sales_volume,
				tmp.uom,
				getdate() as audit_loadts
		from 	tmp_fact_pnl_ocos_allocation_rule_101 tmp
	;
exception when others then raise info 'exception occur while ingesting data in fact_pnl_commercial for rule_101';
end
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_ocos_allocation_rule_102_104_gap(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 
/*
 * 		call dw.p_build_fact_pnl_ocos_allocation_rule_100(202101)
 * 		grant execute on procedure dw.p_build_fact_pnl_ocos_allocation_rule_100(fmthid integer) to group "g-ada-rsabible-sb-ro";
 * 		select count(*) from dw.fact_pnl_ocos where dataprocessing_ruleid = 100;
 */
	
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				max(dt.fmth_id) AS fiscal_month_id
		from 	ref_data.calendar dt
		where 	dt.fmth_id =  fmthid
		
	;
	drop table if exists tmp_fact_pnl_ocos_allocation_rule_102_104
	;
	create temporary table tmp_fact_pnl_ocos_allocation_rule_102_104
	diststyle even
	sortkey (posting_week_enddate)
	AS (
		SELECT 	tr.posting_week_enddate,
				tr.fiscal_month_id, 
				tr.bar_acct,
				tr.bar_currtype,
				tr.bar_entity,
				tr.dataprocessing_ruleid,
				tr.dataprocessing_outcome_id,
				tr.dataprocessing_phase,
				tr.material,
				tr.bar_product,
				tr.bar_brand,
				tr.soldtocust,
				tr.shiptocust,
				tr.bar_custno,
				tr.product_id,
				tr.cost_pool,
				tr.super_sbu,
				tr.customer_id,
				tr.scenario_id,
				tr.amt,
				tr.amt_usd,
				
				dp.product_key,
				dc.customer_key,
				dbu.business_unit_key,
				ddo.dataprocessing_outcome_key,
				dss.source_system_id,
				
				tr.amt as reported_inventory_adjustment,
				0 as reported_warranty_cost,
				0 as reported_duty_tariffs,
				0 as reported_freight,
				0 as reported_ppv,
				0 as reported_labor_overhead,
				
				0 as tran_volume,
				0 as sales_volume,
				null as uom
		FROM 	(
					SELECT	 f.posting_week_enddate 
							,f.fiscal_month_id 
							,f.bar_acct
							,f.bar_currtype 
							,f.bar_entity				
							
							,f.dataprocessing_ruleid
							,f.dataprocessing_outcome_id
							,f.dataprocessing_phase
							
							,f.material
							,f.bar_product
							,f.bar_brand
							
							,f.soldtocust
							,f.shiptocust
							,f.bar_custno
							
							,f.cost_pool
							,f.super_sbu
							
							,f.material || '|' || f.bar_product || '|' || f.bar_brand as product_id
							,f.soldtocust || '|' || f.shiptocust || '|' || f.bar_custno as customer_id
							
							,cast(1 as integer) as scenario_id  -- Hard coded to Actuals
							,f.source_system
							
							,f.allocated_amt as amt
							,f.allocated_amt_usd as amt_usd
				
					from 	stage.agm_allocated_data_rule_102_104 f 
							inner join vtbl_date_range dd 
								on 	dd.fiscal_month_id = f.fiscal_month_id
				) as tr
				LEFT OUTER JOIN dw.dim_product dp on lower(tr.product_id) = lower(dp.product_id)
				LEFT OUTER JOIN dw.dim_business_unit dbu on lower(tr.bar_entity) = lower(dbu.bar_entity)
				LEFT OUTER JOIN dw.dim_dataprocessing_outcome ddo 
					on 	ddo.dataprocessing_outcome_id = tr.dataprocessing_outcome_id and 
						lower(ddo.dataprocessing_phase) = lower(tr.dataprocessing_phase) 
				LEFT OUTER JOIN dw.dim_customer dc on lower(dc.customer_id) = lower(tr.customer_id)
				LEFT OUTER JOIN dw.dim_source_system dss on lower(tr.source_system) = lower(dss.source_system)
				)
	;
--select 	bar_acct,
--		count(*) row_count,
--		sum(case when customer_key is null then 1 else 0 end) as missing_cust_key,
--		sum(case when product_key is null then 1 else 0 end) as missing_prod_key,
--		sum(case when business_unit_key is null then 1 else 0 end) as missing_bu_key,
--		sum(case when dataprocessing_outcome_key is null then 1 else 0 end) as missing_outcome_key,
--		sum(case when source_system_id is null then 1 else 0 end) as missing_source_key
--from 	tmp_fact_pnl_ocos_allocation_rule_100
--group by bar_acct
--;
--select count(*) from tmp_fact_pnl_ocos_allocation_rule_102_104

	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_ocos 
	where 	dataprocessing_ruleid  in (102,103, 104)
			
			and 	posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;

	/* insert statement */
	insert into dw.fact_pnl_ocos (
				org_tranagg_id,
				dataprocessing_ruleid,
				dataprocessing_outcome_key,
				
				bar_acct,
				bar_currtype,
				
				posting_week_enddate,
				fiscal_month_id,
				
				scenario_id,
				source_system_id,
				business_unit_key,
				customer_key,
				product_key,
				
				soldtocust, 
			    shiptocust,
			    bar_custno,
				
				material,
				bar_product,
				bar_brand,
				
				cost_pool,
				super_sbu,
				
				amt,
				amt_usd,
				
				reported_inventory_adjustment,
				reported_warranty_cost,
				reported_duty_tariffs,
				reported_freight,
				reported_ppv,
				reported_labor_overhead,
				
				tran_volume,
				sales_volume,
				uom,
				audit_loadts
		)
		select	-1 as org_tranagg_id,
				tmp.dataprocessing_ruleid,
				tmp.dataprocessing_outcome_key,
				
				tmp.bar_acct,
				tmp.bar_currtype,
				
				tmp.posting_week_enddate,
				tmp.fiscal_month_id,
				
				tmp.scenario_id,
				tmp.source_system_id,
				tmp.business_unit_key,
				tmp.customer_key,
				tmp.product_key,
				
				tmp.soldtocust, 
			    tmp.shiptocust,
			    tmp.bar_custno,
				
				tmp.material,
				tmp.bar_product,
				tmp.bar_brand,
				
				tmp.cost_pool,
				tmp.super_sbu,
				
				tmp.amt,
				tmp.amt_usd,
				
				tmp.reported_inventory_adjustment,
				tmp.reported_warranty_cost,
				tmp.reported_duty_tariffs,
				tmp.reported_freight,
				tmp.reported_ppv,
				tmp.reported_labor_overhead,
				
				tmp.tran_volume,
				tmp.sales_volume,
				tmp.uom,
				getdate() as audit_loadts
		from 	tmp_fact_pnl_ocos_allocation_rule_102_104 tmp
	;
exception when others then raise info 'exception occur while ingesting data in fact_pnl_commercial for rule13';
end
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_ocos_allocation_rule_105(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 
/*
 * 		call dw.p_build_fact_pnl_ocos_allocation_rule_105(202101)
 * 		grant execute on procedure dw.p_build_fact_pnl_ocos_allocation_rule_105(fmthid integer) to group "g-ada-rsabible-sb-ro";
 * 		select count(*) from dw.fact_pnl_ocos where dataprocessing_ruleid = 105;
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
	drop table if exists tmp_fact_pnl_ocos_allocation_rule_105
	;
	create temporary table tmp_fact_pnl_ocos_allocation_rule_105
	diststyle even
	sortkey (posting_week_enddate)
	AS
		SELECT 	tr.posting_week_enddate,
				tr.fiscal_month_id, 
				tr.bar_acct,
				tr.bar_currtype,
				tr.bar_entity,
				tr.dataprocessing_ruleid,
				tr.dataprocessing_outcome_id,
				tr.dataprocessing_phase,
				tr.material,
				tr.bar_product,
				tr.bar_brand,
				tr.soldtocust,
				tr.shiptocust,
				tr.bar_custno,
				tr.product_id,
				tr.cost_pool,
				tr.super_sbu,
				tr.customer_id,
				tr.scenario_id,
				tr.amt,
				tr.amt_usd,
				
				dp.product_key,
				dc.customer_key,
				dbu.business_unit_key,
				ddo.dataprocessing_outcome_key,
				dss.source_system_id,
				
				tr.amt as reported_inventory_adjustment,
				0 as reported_warranty_cost,
				0 as reported_duty_tariffs,
				0 as reported_freight,
				0 as reported_ppv,
				0 as reported_labor_overhead,
				
				0 as tran_volume,
				0 as sales_volume,
				null as uom
		FROM 	(
					SELECT	 f.posting_week_enddate 
							,f.fiscal_month_id 
							,f.bar_acct
							,f.bar_currtype 
							,f.bar_entity				
							
							,f.dataprocessing_ruleid
							,f.dataprocessing_outcome_id
							,f.dataprocessing_phase
							
							,f.material
							,f.bar_product
							,f.bar_brand
							
							,f.soldtocust
							,f.shiptocust
							,f.bar_custno
							
							,f.cost_pool
							,f.super_sbu
							
							,f.material || '|' || f.bar_product || '|' || f.bar_brand as product_id
							,f.soldtocust || '|' || f.shiptocust || '|' || f.bar_custno as customer_id
							
							,cast(1 as integer) as scenario_id  -- Hard coded to Actuals
							,f.source_system
							
							,f.allocated_amt as amt
							,f.allocated_amt_usd as amt_usd
							
					from 	stage.agm_allocated_data_rule_105 f 
							inner join vtbl_date_range dd 
								on 	dd.fiscal_month_id = f.fiscal_month_id
				) as tr
				LEFT OUTER JOIN dw.dim_product dp on lower(tr.product_id) = lower(dp.product_id)
				LEFT OUTER JOIN dw.dim_business_unit dbu on lower(tr.bar_entity) = lower(dbu.bar_entity)
				LEFT OUTER JOIN dw.dim_dataprocessing_outcome ddo 
					on 	ddo.dataprocessing_outcome_id = tr.dataprocessing_outcome_id and 
						lower(ddo.dataprocessing_phase) = lower(tr.dataprocessing_phase) 
				LEFT OUTER JOIN dw.dim_customer dc on lower(dc.customer_id) = lower(tr.customer_id)
				LEFT OUTER JOIN dw.dim_source_system dss on lower(tr.source_system) = lower(dss.source_system)
	;
--select 	bar_acct,
--		count(*) row_count,
--		sum(case when customer_key is null then 1 else 0 end) as missing_cust_key,
--		sum(case when product_key is null then 1 else 0 end) as missing_prod_key,
--		sum(case when business_unit_key is null then 1 else 0 end) as missing_bu_key,
--		sum(case when dataprocessing_outcome_key is null then 1 else 0 end) as missing_outcome_key,
--		sum(case when source_system_id is null then 1 else 0 end) as missing_source_key
--from 	tmp_fact_pnl_ocos_allocation_rule_105
--group by bar_acct
--;

	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_ocos 
	where 	dataprocessing_ruleid  = 105 and 
			posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
	/* insert statement */
	insert into dw.fact_pnl_ocos (
				org_tranagg_id,
				dataprocessing_ruleid,
				dataprocessing_outcome_key,
				
				bar_acct,
				bar_currtype,
				
				posting_week_enddate,
				fiscal_month_id,
				
				scenario_id,
				source_system_id,
				business_unit_key,
				customer_key,
				product_key,
				
				soldtocust, 
			    shiptocust,
			    bar_custno,
				
				material,
				bar_product,
				bar_brand,
				
				cost_pool,
				super_sbu,
				
				amt,
				amt_usd,
				
				reported_inventory_adjustment,
				reported_warranty_cost,
				reported_duty_tariffs,
				reported_freight,
				reported_ppv,
				reported_labor_overhead,
				
				tran_volume,
				sales_volume,
				uom,
				audit_loadts
		)
		select	-1 as org_tranagg_id,
				tmp.dataprocessing_ruleid,
				tmp.dataprocessing_outcome_key,
				
				tmp.bar_acct,
				tmp.bar_currtype,
				
				tmp.posting_week_enddate,
				tmp.fiscal_month_id,
				
				tmp.scenario_id,
				tmp.source_system_id,
				tmp.business_unit_key,
				tmp.customer_key,
				tmp.product_key,
				
				tmp.soldtocust, 
			    tmp.shiptocust,
			    tmp.bar_custno,
				
				tmp.material,
				tmp.bar_product,
				tmp.bar_brand,
				
				tmp.cost_pool,
				tmp.super_sbu,
				
				tmp.amt,
				tmp.amt_usd,
				
				tmp.reported_inventory_adjustment,
				tmp.reported_warranty_cost,
				tmp.reported_duty_tariffs,
				tmp.reported_freight,
				tmp.reported_ppv,
				tmp.reported_labor_overhead,
				
				tmp.tran_volume,
				tmp.sales_volume,
				tmp.uom,
				getdate() as audit_loadts
		from 	tmp_fact_pnl_ocos_allocation_rule_105 tmp
	;
exception when others then raise info 'exception occur while ingesting data in fact_pnl_commercial for rule13';
end
$$
;

CREATE OR REPLACE PROCEDURE dw.p_build_fact_pnl_ocos_stacked(fmthid integer)
 LANGUAGE plpgsql
AS $$
BEGIN 
/*
 * 		call dw.p_build_fact_pnl_ocos_stacked(202101)
 * 		select count(*) from dw.fact_pnl_ocos_stacked where dataprocessing_ruleid = 100;
 * 		grant execute on procedure dw.p_build_fact_pnl_ocos_stacked(fmthid integer) to group "g-ada-rsabible-sb-ro";
 */
	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				max(dt.fmth_id) as fiscal_month_id
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
	;
	/* remove any existing transactions for the current batch being processed */
	delete 
	from 	dw.fact_pnl_ocos_stacked 
	where 	posting_week_enddate between 
			(select range_start_date from vtbl_date_range) and 
			(select range_end_date from vtbl_date_range)
	;
	
	
	INSERT INTO dw.fact_pnl_ocos_stacked (
				org_tranagg_id,
				dataprocessing_ruleid,
				dataprocessing_outcome_key,
				
				bar_acct,
				bar_currtype,
				
				posting_week_enddate,
				fiscal_month_id,
				
				scenario_id,
				source_system_id,
				business_unit_key,
				customer_key,
				product_key,
				
				soldtocust, 
			    shiptocust,
			    bar_custno,
				
				material,
				bar_product,
				bar_brand,
				
				cost_pool,
				super_sbu,
				
				amt,
				amt_usd,
				
				tran_volume,
				sales_volume,
				uom
		)
		select	fpo.org_tranagg_id,
				fpo.dataprocessing_ruleid,
				fpo.dataprocessing_outcome_key,
				
				fpo.bar_acct,
				fpo.bar_currtype,
				
				fpo.posting_week_enddate,
				fpo.fiscal_month_id,
				
				fpo.scenario_id,
				fpo.source_system_id,
				fpo.business_unit_key,
				fpo.customer_key,
				fpo.product_key,
				
				fpo.soldtocust, 
			    fpo.shiptocust,
			    fpo.bar_custno,
				
				fpo.material,
				fpo.bar_product,
				fpo.bar_brand,
				
				fpo.cost_pool,
				fpo.super_sbu,
				
				fpo.amt,
				fpo.amt_usd,
				
				fpo.tran_volume,
				fpo.sales_volume,
				fpo.uom
		from 	dw.fact_pnl_ocos fpo 
		where 	fpo.posting_week_enddate between 
				(select range_start_date from vtbl_date_range) and 
				(select range_end_date from vtbl_date_range)
	;
EXCEPTION
	when others then raise info 'exception occur while building fact_pnl_commercial_stacked';
END;
$$
;

CREATE OR REPLACE PROCEDURE dw.run_umm_agm_anton(fmthid integer)
 LANGUAGE plpgsql
AS $$
DECLARE
  rec RECORD;
  query text;
BEGIN
  query := 'SELECT distinct fmth_id as fmthid FROM dw.dim_date where fmth_id between ' || fmthid || ' and 201901';
  FOR rec IN EXECUTE query
  loop
  
	call   stage.p_allocate_data_rule_agm_100 (rec.fmthid);
	call   stage.p_allocate_data_rule_agm_105 (rec.fmthid);
	call   stage.p_allocate_data_rule_101 (rec.fmthid);
	call   stage.p_allocate_data_rule_101_bnr_gap (rec.fmthid);
	call   stage.p_build_source_1070_Costing  (rec.fmthid);
	call   stage.p_build_allocation_rule_102_104 (rec.fmthid);
	call   stage.p_build_allocation_rule_102_104_gap (rec.fmthid);
	call   dw.p_build_fact_pnl_ocos_allocation_rule_100 (rec.fmthid);
	call   dw.p_build_fact_pnl_ocos_allocation_rule_101  (rec.fmthid);
	call   dw.p_build_fact_pnl_ocos_allocation_rule_105 (rec.fmthid);
	call   dw.p_build_fact_pnl_ocos_allocation_rule_102_104_gap (rec.fmthid);
	call   dw.p_build_fact_pnl_ocos_stacked (rec.fmthid);
  
--	RAISE INFO 'BEGIN processing fiscal month : %', rec.fmthid;
--	call stage.p_allocate_data_rule_agm_100(rec.fmthid);
--	call dw.p_build_fact_pnl_ocos_allocation_rule_100(rec.fmthid);
--	call dw.p_build_fact_pnl_ocos_stacked(rec.fmthid);
--	RAISE INFO 'END processing fiscal month : %', rec.fmthid;
  END LOOP;
END;
$$
;

CREATE OR REPLACE PROCEDURE dw.run_umm_commercial(fmthid integer)
 LANGUAGE plpgsql
AS $$
DECLARE
  rec RECORD;
  query text;
BEGIN
  query := 'SELECT distinct fmth_id as fmthid FROM dw.dim_date where fmth_id between ' || fmthid || ' and 202109';
  FOR rec IN EXECUTE query
  LOOP
  	RAISE INFO 'begin processing fiscal month : %', rec.fmthid;
--	call stage.p_build_source_core_tran_delta_c11(rec.fmthid); ---fiscalmonthid
--	call stage.p_build_source_core_tran_delta_P10(rec.fmthid); ---fiscalmonthid
--	call stage.p_build_source_core_tran_delta_Lawson(rec.fmthid); ---fiscalmonthid
--	call stage.p_build_source_core_tran_delta_hfm(rec.fmthid); ---fiscalmonthid
--	call stage.p_build_source_core_tran_delta_c11_fob(rec.fmthid);
--	call stage.p_build_source_core_tran_delta_c11_stdcost(rec.fmthid);
--	RAISE INFO 'end processing delta for fiscal month  : %', rec.fmthid;
--	
--	call stage.p_build_source_core_tran_delta_cleansed(rec.fmthid); ---fiscalmonthid
--	call stage.p_build_source_core_tran_delta_agg(rec.fmthid);  --data_source
--	
--	RAISE INFO 'end processing delta agg for fiscal month  : %', rec.fmthid;
--
--	call stage.p_build_stage_rate_base (rec.fmthid);
--
--	RAISE INFO 'end processing base distribution for fiscal month  : %', rec.fmthid;
--
--	--allocation rules : needs revision, data analysis and rule optimization
--	call stage.p_allocate_data_rule_09(rec.fmthid);
--	RAISE INFO 'end processingp_allocate_data_rule_09 for fiscal month  : %', rec.fmthid;
--	call stage.p_allocate_data_rule_13(rec.fmthid); 
--	RAISE INFO 'end p_allocate_data_rule_13 for fiscal month  : %', rec.fmthid;
--	call stage.p_allocate_data_rule_22 (rec.fmthid);
--	RAISE INFO 'end p_allocate_data_rule_22 for fiscal month  : %', rec.fmthid;
	call stage.p_allocate_data_rule_21_c11(rec.fmthid);
	RAISE INFO 'end p_allocate_data_rule_21_c11 for fiscal month  : %', rec.fmthid;
	call stage.p_allocate_data_rule_21_hfm (rec.fmthid);
	RAISE INFO 'end p_allocate_data_rule_21_hfm for fiscal month  : %', rec.fmthid;
	call stage.p_allocate_data_rule_26_c11(rec.fmthid);
	RAISE INFO 'end p_allocate_data_rule_26_c11 for fiscal month  : %', rec.fmthid;
	call stage.p_allocate_data_rule_26_hfm (rec.fmthid);
	RAISE INFO 'end p_allocate_data_rule_26_hfm for fiscal month  : %', rec.fmthid;
--	call stage.p_allocate_data_rule_23 (rec.fmthid);
--	RAISE INFO 'end p_allocate_data_rule_23 for fiscal month  : %', rec.fmthid;
--	call stage.p_allocate_data_rule_27 (rec.fmthid);
--	RAISE INFO 'end p_allocate_data_rule_27 for fiscal month  : %', rec.fmthid;
--	call stage.p_allocate_data_rule_28 (rec.fmthid);
	RAISE INFO 'end p_allocate_data_rule_28 for fiscal month  : %', rec.fmthid;
	-------dimension procedures
	--incremental load 
	call dw.p_build_dim_customer (2); --flag_reload: 1 = kill-n-fill, 2 = incremental
	call dw.p_build_dim_product (2); --flag_reload: 1 = kill-n-fill, 2 = incremental
--	call dw.p_build_dim_dataprocessing_outcome ();
	RAISE INFO 'end processing dimensions for fiscal month  : %', rec.fmthid;
	-----------execute fact procedures : 
	call dw.p_build_fact_pnl_commercial_allocation_rule_22 (rec.fmthid); --(fmthid integer)
	call dw.p_build_fact_pnl_commercial_allocation_rule_13 (rec.fmthid);  --(fmthid integer)
	call dw.p_build_fact_pnl_commercial_allocation_rule_09 (rec.fmthid); --(fmthid integer)
	call dw.p_build_fact_pnl_commercial_allocation_rule_21 (rec.fmthid); --(fmthid integer)
	call dw.p_build_fact_pnl_commercial_allocation_rule_23 (rec.fmthid);
	call dw.p_build_fact_pnl_commercial_allocation_rule_26 (rec.fmthid); --(fmthid integer)
	call dw.p_build_fact_pnl_commercial_allocation_rule_27 (rec.fmthid);
	call dw.p_build_fact_pnl_commercial_allocation_rule_28 (rec.fmthid);
	call dw.p_build_fact_pnl_commercial_not_allocated (rec.fmthid); --(fmthid integer)
	
	RAISE INFO 'end processing fact_pnl_commercial for fiscal month  : %', rec.fmthid;
	call dw.p_build_fact_pnl_commercial_stacked  (rec.fmthid);--(fmthid integer)
	RAISE INFO 'end processing fact_pnl_commercial_stacked for fiscal month  : %', rec.fmthid;
	call dw.p_build_fact_pnl_commercial_orig  (rec.fmthid);
	
 	
    	RAISE INFO 'end processing fiscal month : %', rec.fmthid;
  END LOOP;
END;
$$
;

CREATE OR REPLACE PROCEDURE dw.run_umm_sgm_agm(fmthid integer)
 LANGUAGE plpgsql
AS $$
DECLARE
  rec RECORD;
  query text;
BEGIN
  query := 'SELECT distinct fmth_id as fmthid FROM dw.dim_date where fmth_id between ' || fmthid || ' and 202111';
  FOR rec IN EXECUTE query
  LOOP
	RAISE INFO 'BEGIN processing fiscal month : %', rec.fmthid;

	/* ============================================================
	 * 		PROCESSING SGM
	 * ============================================================
	 */
	RAISE INFO '>> BEGIN processing sgm fiscal month : %', rec.fmthid;	
  	
	RAISE INFO '>>>>> BEGIN processing sgm-delta/agg (fiscal month : %)', rec.fmthid;
  	call stage.p_build_source_core_tran_delta_c11(rec.fmthid);
	call stage.p_build_source_core_tran_delta_P10(rec.fmthid);
	call stage.p_build_source_core_tran_delta_Lawson(rec.fmthid);
	call stage.p_build_source_core_tran_delta_hfm(rec.fmthid);
	call stage.p_build_source_core_tran_delta_c11_fob(rec.fmthid);
	call stage.p_build_source_core_tran_delta_c11_stdcost(rec.fmthid);
	call stage.p_build_source_core_tran_delta_cleansed(rec.fmthid);
	call stage.p_build_source_core_tran_delta_agg(rec.fmthid);
	call stage.p_build_stage_rate_base (rec.fmthid);
	RAISE INFO '>>>>> END processing sgm-delta/agg (fiscal month : %)', rec.fmthid;
	RAISE INFO '>>>>> BEGIN processing sgm-allocations (fiscal month : %)', rec.fmthid;
	call stage.p_allocate_data_rule_09(rec.fmthid);
	call stage.p_allocate_data_rule_13(rec.fmthid); 
	call stage.p_allocate_data_rule_22 (rec.fmthid);
	call stage.p_allocate_data_rule_21_c11(rec.fmthid);
	call stage.p_allocate_data_rule_21_hfm (rec.fmthid);
	call stage.p_allocate_data_rule_26_c11(rec.fmthid);
	call stage.p_allocate_data_rule_26_hfm (rec.fmthid);
	call stage.p_allocate_data_rule_23 (rec.fmthid);
	call stage.p_allocate_data_rule_27 (rec.fmthid);
	call stage.p_allocate_data_rule_28 (rec.fmthid);
	RAISE INFO '>>>>> END processing sgm-allocations (fiscal month : %)', rec.fmthid;
	
	RAISE INFO '>>>>> BEGIN processing Dimensions (fiscal month : %)', rec.fmthid;
	call dw.p_build_dim_business_unit(2);
	call dw.p_build_dim_customer (2);
	call dw.p_build_dim_product (2);
	call dw.p_build_dim_dataprocessing_rule(2);
	call dw.p_build_dim_dataprocessing_outcome (2);
	call dw.p_build_dim_date(2);
	call dw.p_build_dim_scenario(2);
	call dw.p_build_dim_source_system(2);
	call dw.p_build_dim_currency(2);
	call dw.p_build_dim_transactional_attributes(2);
	RAISE INFO '>>>>> END processing Dimensions (fiscal month : %)', rec.fmthid;
		
	RAISE INFO '>>>>> BEGIN processing sgm-fact_commercial (fiscal month : %)', rec.fmthid;
	call dw.p_build_fact_pnl_commercial_allocation_rule_22 (rec.fmthid);
	call dw.p_build_fact_pnl_commercial_allocation_rule_13 (rec.fmthid);
	call dw.p_build_fact_pnl_commercial_allocation_rule_09 (rec.fmthid);
	call dw.p_build_fact_pnl_commercial_allocation_rule_21 (rec.fmthid);
	call dw.p_build_fact_pnl_commercial_allocation_rule_23 (rec.fmthid);
	call dw.p_build_fact_pnl_commercial_allocation_rule_26 (rec.fmthid);
	call dw.p_build_fact_pnl_commercial_allocation_rule_27 (rec.fmthid);
	call dw.p_build_fact_pnl_commercial_allocation_rule_28 (rec.fmthid);
	call dw.p_build_fact_pnl_commercial_not_allocated (rec.fmthid);
	call dw.p_build_fact_pnl_commercial_stacked  (rec.fmthid);
	call dw.p_build_fact_pnl_commercial_orig  (rec.fmthid);
	RAISE INFO '>>>>> END processing sgm-fact_commercial (fiscal month : %)', rec.fmthid;
	RAISE INFO '>> END processing sgm fiscal month : %', rec.fmthid;

	/* ============================================================
	 * 		PROCESSING AGM
	 * ============================================================
	 */
	RAISE INFO '>> BEGIN processing agm fiscal month : %', rec.fmthid;		
	call ref_data.p_build_sku_barbrand_mapping_sgm(rec.fmthid); 
	call ref_data.p_build_sku_gpp_mapping_sgm(rec.fmthid);  
	call ref_data.p_build_ref_data_ptg_accruals_agm (rec.fmthid);   

	RAISE INFO '>>>>> BEGIN processing agm-delta/agg (fiscal month : %)', rec.fmthid;
	call stage.p_build_source_core_tran_delta_c11_agm(rec.fmthid);  
	call stage.p_build_source_core_tran_delta_hfm_agm(rec.fmthid);  
	call stage.p_build_source_core_tran_delta_lawson_agm(rec.fmthid); 
	call stage.p_build_source_core_tran_delta_p10_agm(rec.fmthid);
	call stage.p_build_source_core_tran_delta_agg_agm(rec.fmthid);
	call stage.p_build_stage_rate_base_cogs (rec.fmthid);
	RAISE INFO '>>>>> END processing agm-delta/agg (fiscal month : %)', rec.fmthid;
	RAISE INFO '>>>>> BEGIN processing agm-allocations (fiscal month : %)', rec.fmthid;
	call stage.p_allocate_data_rule_agm_100(rec.fmthid);
	call stage.p_allocate_data_rule_agm_105(rec.fmthid);
	call stage.p_allocate_data_rule_101(rec.fmthid);
	call stage.p_allocate_data_rule_101_bnr_gap(rec.fmthid);
-- 	!!!!!!!!!IMPORTANT!!!!!!
-- 	leave out until Keko is loaded
--	/*call stage.p_build_source_1070_Costing (rec.fmthid);*/
-- 	!!!!!!!!!IMPORTANT!!!!!!
	call stage.p_build_allocation_rule_102_104(rec.fmthid);
	call stage.p_build_allocation_rule_102_104_gap (rec.fmthid);
	RAISE INFO '>>>>> END processing agm-allocations (fiscal month : %)', rec.fmthid;
	RAISE INFO '>>>>> BEGIN processing agm-fact_ocos (fiscal month : %)', rec.fmthid;
	call dw.p_build_fact_pnl_ocos_allocation_rule_100(rec.fmthid);
	call dw.p_build_fact_pnl_ocos_allocation_rule_101 (rec.fmthid); 
	call dw.p_build_fact_pnl_ocos_allocation_rule_105 (rec.fmthid); 
	call dw.p_build_fact_pnl_ocos_allocation_rule_102_104_gap(rec.fmthid);
	call dw.p_build_fact_pnl_ocos_stacked(rec.fmthid);
	RAISE INFO '>>>>> END processing agm-fact_ocos (fiscal month : %)', rec.fmthid;
	RAISE INFO '>>>>> BEGIN processing restatement cust/prod (fiscal month : %)', rec.fmthid;
	call dw.p_build_dim_product_restatement();
	call dw.p_build_dim_customer_restatement();
	RAISE INFO '>>>>> END processing restatement cust/prod (fiscal month : %)', rec.fmthid;
 	
	RAISE INFO '>> END processing agm fiscal month : %', rec.fmthid;
	RAISE INFO 'END processing fiscal month : %', rec.fmthid;
  END LOOP;
END;
$$
;
