
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