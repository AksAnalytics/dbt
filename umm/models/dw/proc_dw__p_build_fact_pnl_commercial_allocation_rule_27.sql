
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