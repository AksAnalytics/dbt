
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