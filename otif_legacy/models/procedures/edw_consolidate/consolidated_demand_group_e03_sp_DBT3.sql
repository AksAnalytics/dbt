WITH a AS(

  SELECT  * FROM edw.edw_stage.demand_group_e03_determination_stg1 a

),

b AS (

    SELECT  *  FROM edw.edw_stage.demand_group_e03_determination_stg b
),
		
dmnd_grp AS (

SELECT DISTINCT 
    ( ( CASE WHEN  a.kunnr IS NULL THEN '' ELSE  COALESCE(b.PARENT_CONS,'')  END)
        || ( CASE WHEN a.prodhl1_e03 IS NULL THEN '' ELSE COALESCE(b.prodhl1_e03,'') END )
        || ( CASE WHEN a.prodhl2_e03 IS NULL THEN '' ELSE COALESCE(substring(b.prodhl2_e03,2,2),'') END )
        || ( CASE WHEN a.brand_cons IS NULL THEN '' ELSE COALESCE(b.brand_cons,'') END )
        || ( CASE WHEN a.country_cons IS NULL THEN '' ELSE COALESCE(b.country_cons,'') END )
        || ( CASE WHEN a.sales_office_cons IS NULL THEN '' ELSE COALESCE(b.sales_office_cons,'') END )
        || ( CASE WHEN a.industkey_cons IS NULL THEN '' ELSE COALESCE(b.industkey_cons,'') END )
    ) AS zdmdgrp_factors_trans,

    b.material_cons,
    b.CUSTOMER_CONS, 
    b.salesorg_cons,
    b.PARENT_CONS AS kunnr_trans,
    a.*,
    b.hier_typ,
    b.HIER_LEVEL,
    b.demand_group_logic_level
FROM a 
JOIN b 
    ON (a.kunnr = b.PARENT_CONS  OR a.kunnr IS NULL) 
    AND (a.VKORG = b.salesorg_cons )		
),

dmnd_grp_filter AS  (

    SELECT 
      *,
      MIN(rn+demand_group_logic_level) OVER (PARTITION BY customer_cons, material_cons, salesorg_cons, hier_typ) AS dgrp
    FROM dmnd_grp
    WHERE zdmdgrp_factors = zdmdgrp_factors_trans 
),

min_dgrp AS (

  SELECT * 
  FROM dmnd_grp_filter
  WHERE dgrp = (rn+demand_group_logic_level)
),

min_dgrp_filter AS (

    SELECT 
      *,
      ROW_NUMBER() OVER (PARTITION BY customer_cons, material_cons, salesorg_cons, hier_typ ORDER BY demand_group_logic_level ASC) AS rownum
	FROM min_dgrp
),

final AS (
    
    SELECT * 
    FROM min_dgrp_filter
    WHERE rownum = 1	
		
)

SELECT * FROM final