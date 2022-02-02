-- Import

-- Add reference
WITH a AS (

    SELECT * FROM edw.edw_stage.demand_group_e03_determination_stg1 
), 

b AS (

    SELECT * FROM edw.edw_stage.demand_group_e03_determination_stg 
),

final AS (

    SELECT DISTINCT 
      ((CASE WHEN a.kunnr IS NULL THEN '' ELSE coalesce(b.kunnr,'') END)
        || (CASE WHEN a.prodhl1_e03 IS NULL THEN '' ELSE coalesce(b.prodhl1_e03,'') END)
        || (CASE WHEN a.prodhl2_e03 IS NULL THEN '' ELSE coalesce(substring(b.prodhl2_e03,2,2),'') END)
        || (CASE WHEN a.brand_cons IS NULL THEN '' ELSE coalesce(b.brand_cons,'') END)
        || (CASE WHEN a.country_cons IS NULL THEN '' ELSE coalesce(b.country_cons,'') END)
        || (CASE WHEN a.sales_office_cons IS NULL THEN '' ELSE coalesce(b.sales_office_cons,'') END)
        || (CASE WHEN a.industkey_cons IS NULL THEN '' ELSE coalesce(b.industkey_cons,'') END)
      ) AS zdmdgrp_factors_trans,
      b.material_cons,
      b.customer, b.salesorg_cons,
      b.kunnr AS kunnr_trans,
      a.*, 
      b.hityp,
      b.hierarchy_level,
      b.demand_group_logic_level
    FROM a 
    JOIN b 
      on (a.kunnr=b.kunnr OR a.kunnr is null) 
     AND (a.VKORG=b.salesorg_cons)		        
),

SELECT * FROM final
        