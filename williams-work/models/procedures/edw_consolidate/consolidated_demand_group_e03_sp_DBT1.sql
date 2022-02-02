WITH co AS (

    SELECT DISTINCT 
      source_sys, 
      salesorg_cons, 
      soldto_cons, 
      material_cons
    FROM edw.edw_consolidated.consolidated_orders  
    WHERE source_sys  = 'E03'
),

cc AS (

    SELECT DISTINCT 
      customer_cons,
      country_cons, 
      industkey_cons
    FROM edw.edw_consolidated.consolidated_customer   
    WHERE source_sys  = 'E03'
),

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

h AS (
    SELECT 
      *
    FROM edw.edw_consolidated.consolidated_customer_hierarchy
    WHERE source_sys  = 'E03'
      AND hier_typ ='B'
),

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
      h.customer_cons,
      h.HIER_TYP,
      h.PARENT_NAME,
      h.PARENT_CONS,
      h.ACCT_GRP,
      h.HIER_LEVEL,
      h.demAND_group_logic_level,
      cc.country_cons,
      cc.industkey_cons,
      ccs.sales_office_cons,
      m.prodhl1_e03,
      m.prodhl2_e03,
      m.brAND_cons,
      m.material_cons
    FROM co 
    JOIN cc 
      ON co.soldto_cons = cc.customer_cons
    JOIN ccs 
      ON co.soldto_cons = ccs.customer_cons
    JOIN ccs AS ccs_salesorg 
      ON co.soldto_cons = ccs_salesorg.customer_cons 
     AND co.salesorg_cons = ccs_salesorg.salesorg_cons
    JOIN h 
      ON co.soldto_cons = h.customer_cons 
     AND co.salesorg_cons = h.salesorg_cons
    JOIN m 
      ON co.material_cons = m.material_cons
)

SELECT * FROM final
		
	