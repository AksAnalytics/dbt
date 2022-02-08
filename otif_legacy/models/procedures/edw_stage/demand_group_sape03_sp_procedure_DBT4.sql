-- Import

-- Add reference
WITH source AS (

    SELECT * FROM edw.edw_stage.demand_group_e03_determination_stg2
),

inner_query AS (

    SELECT 
      source.*,
	  MIN(rn+demand_group_logic_level) OVER (PARTITION BY customer, material_cons, salesorg_cons, hityp) AS dgrp
	FROM source
	WHERE zdmdgrp_factors = zdmdgrp_factors_trans 
),


final AS (
    SELECT * FROM inner_query
    WHERE dgrp = (rn+demand_group_logic_level);

)

SELECT * FROM final