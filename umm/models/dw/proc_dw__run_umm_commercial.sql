
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