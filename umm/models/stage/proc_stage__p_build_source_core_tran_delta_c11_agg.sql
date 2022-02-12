
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
from 	{{ source('bods', 'drm_product') }}
where 	loaddts = ( select max(loaddts) from {{ source('bods', 'drm_product') }} dpc );		

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
		from {{ source('sapc11', 'marm') }} mc 
		inner join (
		Select matnr ,max(row_sqn) as max_row_sqn
		from {{ source('sapc11', 'marm') }}
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
inner join {{ source('ref_data', 'hfmfxrates') }} cr on cast(cr.bar_year as integer) = c.fyr_id 
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
--		from {{ source('sapc11', 'marm') }} mc 
--		inner join (
--		Select matnr ,max(row_sqn) as max_row_sqn
--		from {{ source('sapc11', 'marm') }}
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