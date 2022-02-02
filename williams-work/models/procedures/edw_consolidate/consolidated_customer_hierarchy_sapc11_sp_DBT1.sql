-- Imports

-- Add reference
WITH base_knvh AS (

    SELECT * FROM edw.sapc11.knvh_current
),

-- Add reference
base_kna1 AS (

    SELECT * FROM edw.sapc11.kna1_current
),

knvh AS (

    SELECT DISTINCT 
      kunnr, 
      hkunnr,
      hityp, 
      vkorg, 
      vtweg, 
      spart 
    FROM base_knvh AS knvh
    WHERE CURRENT_DATE BETWEEN to_date(knvh.datab, 'YYYYMMDD') AND to_date(knvh.datbi, 'YYYYMMDD')                   
),

src AS (

    SELECT DISTINCT  
      kna1.kunnr AS customer,
      knvh.kunnr, 
      knvh.hityp, 
      name1,ktokd, 
      knvh.vkorg, 
      knvh.hkunnr,  
      vtweg, 
      spart
    FROM knvh
    JOIN base_kna1 AS kna1
      ON (knvh.hunnr = kna1.kunnr)            
),

final AS (

    SELECT 
      'C11' AS source_sys, 
      * 
    FROM  src;

)

SELECT * FROM final