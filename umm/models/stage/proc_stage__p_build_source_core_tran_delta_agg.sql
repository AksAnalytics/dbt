
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
from 	{{ source('bods', 'drm_product') }}
where 	loaddts = ( select max(loaddts) from {{ source('bods', 'drm_product') }} dpc );		
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
		from {{ source('sapc11', 'marm') }} mc 
		inner join (
		Select matnr ,meinh,max(loaddts) as max_loadtime
		from {{ source('sapc11', 'marm') }}
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