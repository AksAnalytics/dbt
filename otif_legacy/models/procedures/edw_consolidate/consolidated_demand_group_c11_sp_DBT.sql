-- Import

WITH base_cs AS (

    SELECT * FROM edw.edw_consolidated.consolidated_customer_sales
),

base_transcust AS (

    SELECT * FROM jda.udtdfutranscust_current
),

final AS (

    SELECT 
      'C11' as Source_Sys,
	  cs.CUSTOMER_CONS as customer_cons,
 	  cs.salesorg_cons as salesorg_cons,
 	  NULL as material_cons,
 	  TRANSCUST.U_CUST_DMDGROUP as demand_group,
 	  v_job_name as ETL_CRTE_USER,
 	  CURRENT_TIMESTAMP ETL_CRTE_TS,
	  NULL as ETL_UPDT_USER,
	  NULL ETL_UPDT_TS
 	FROM base_cs AS cs
 	JOIN base_transcust AS TRANSCUST
	  ON cs.CUSTOMER_CONS = TRANSCUST.U_CUST_CODE
	 AND cs.SALESORG_CONS = TRANSCUST.U_CUST_SALESORG
	WHERE cs.source_sys='C11';
)

SELECT * FROM final
	