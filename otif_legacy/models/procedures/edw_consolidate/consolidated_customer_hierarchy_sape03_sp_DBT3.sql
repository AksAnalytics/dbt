-- I didn't want to mess the recursion so I didn't use imports

WITH RECURSIVE p(source_sys,customer ,kunnr, hityp, name1,ktokd, vkorg , hkunnr, level, vtweg,spart) AS (
    SELECT 
      source_sys,
      customer,
      kunnr, 
      hityp, 
      name1,
      ktokd, 
      vkorg, 
      hkunnr, 
      1 AS level, 
      vtweg,
      spart
    FROM edw.edw_stage.consolidated_customer_hierarchy_stg_e03 
    WHERE hityp='D'

    UNION ALL

    SELECT 
      p.source_sys,
      p.customer,
      p.hkunnr, 
      c.hityp, 
      c.name1,
      c.ktokd, 
      c.vkorg, 
      c.hkunnr, 
      level+1, 
      c.vtweg, 
      c.spart
    FROM edw.edw_stage.consolidated_customer_hierarchy_stg_e03 c, p
    WHERE c.kunnr = p.hkunnr 
      AND c.hityp = p.hityp 
      AND c.vtweg = p.vtweg 
      AND c.spart = p.spart  
      AND c.source_sys = p.source_sys 
      AND level <=7 
      AND c.hityp='D'
)

SELECT DISTINCT 
  source_sys,customer AS CUSTOMER_CONS,
  kunnr AS PARENT_CONS, 
  hityp AS HIER_TYP,
  name1 AS PARENT_NAME,
  ktokd AS ACCT_GRP, 
  vkorg AS SALESORG_CONS,
  ((MAX(level+1) OVER (PARTITION BY source_sys,hityp,customer))-level) AS HIER_LEVEL,
  level AS demAND_group_logic_level,
  vtweg AS SALESDIST_CONS, 
  spart AS SALESDIV_CONS,
  'ETL_USER' AS ETL_CRTE_USER,
  current_timestamp AS ETL_CRTE_TS,
  NULL AS ETL_UPDT_USER,
  NULL AS ETL_UPDT_TS
FROM p 
ORDER BY customer, hityp
     
   