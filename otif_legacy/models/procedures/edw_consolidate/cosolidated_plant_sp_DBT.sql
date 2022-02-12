WITH base_t001w AS (

    SELECT * FROM sapc11.t001w_current
),

base_pr AS (

    SELECT * FROM edw.edw_stage.plant_regions
),

union_table AS (

    select
	'C11' as SOURCE_SYS, t001w.WERKS as PLANT, t001w.NAME1 as PLANT_TXT, t001w.LAND1 as PLANT_COUNTRY
	from sapc11.t001w_current  as t001w
	
	UNION ALL
	
	select
	'E03' as SOURCE_SYS, t001w.WERKS as PLANT, t001w.NAME1 as PLANT_TXT,t001w.LAND1 as PLANT_COUNTRY
	from sape03.t001w_current  as t001w

),

final AS (
    
    SELECT distinct 
      t.source_sys, 
      t.plant, 
      t.plant_txt, 
      t.plant_country, 
      
      CASE WHEN t.plant  = '10US' THEN 'LAG' ELSE pr.region END AS region 
      
    FROM union_table AS t    
    JOIN base_pr AS pr
      ON t.PLANT_COUNTRY = pr.country 
)

SELECT * FROM final 

    
