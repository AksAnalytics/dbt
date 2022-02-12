
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