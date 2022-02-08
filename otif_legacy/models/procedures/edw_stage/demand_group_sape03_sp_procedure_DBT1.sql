-- Staging

-- Add reference model
WITH co AS (
    SELECT DISTINCT 
      source_sys, 
      salesorg_cons, 
      soldto_cons, 
      material_cons
    FROM edw.edw_consolidated.consolidated_orders  
    WHERE source_sys  = 'E03'
    
),

-- Add reference model
cc AS (
    SELECT DISTINCT
      customer_cons,
      country_cons, 
      industkey_cons
    FROM edw.edw_consolidated.consolidated_customer   
    WHERE source_sys  = 'E03'
    
),

-- Add reference model
ccs AS (
    SELECT DISTINCT
      customer_cons,
      sales_office_cons, 
      salesdiv_cons, 
      salesdist_cons, 
      salesorg_cons
    FROM edw.edw_consolidated.consolidated_customer_sales   
    WHERE source_sys  = 'E03'
    
),

-- Add reference model
h AS (
    SELECT DISTINCT *
    FROM edw.edw_stage.consolidated_customer_hierarchy_dmdgroup
    WHERE source_sys  = 'E03'
      AND hityp IN ('B','D')
),

-- Add reference model
m AS (
    SELECT DISTINCT
      material_cons, 
      prodhl1_e03, 
      prodhl2_e03, 
      brand_cons
    FROM edw.edw_consolidated.consolidated_material cm 
    WHERE source_sys  = 'E03'   
),

final AS (

    SELECT DISTINCT 
    co.salesorg_cons, 
    co.soldto_cons,
    h.*,
    cc.country_cons,
    cc.industkey_cons, 
    ccs.sales_office_cons,
    m.prodhl1_e03,
    m.prodhl2_e03,
    m.brand_cons, 
    m.material_cons
    FROM co 
    JOIN cc 
    ON (co.soldto_cons = cc.customer_cons)
    JOIN ccs 
    ON (co.soldto_cons = ccs.customer_cons)
    JOIN ccs as ccs_salesorg 
    ON (co.soldto_cons = ccs_salesorg.customer_cons 
    AND co.salesorg_cons = ccs_salesorg.salesorg_cons)
    JOIN h 
    ON (co.soldto_cons = h.customer)
    JOIN m 
    ON (co.material_cons = m.material_cons)
)

SELECT * FROM final