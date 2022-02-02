-- Imports

-- Add reference
WITH base_knvh AS (

    SELECT * FROM edw.sape03.knvh_current
),

-- Add reference
base_kna1 AS (

    SELECT * FROM edw.sape03.kna1_current
),

knvh AS (
    SELECT DISTINCT 
      kunnr, 
      hkunnr,
      hityp, 
      vkorg, 
      vtweg, 
      spart
    FROM base_knvh knvh
    WHERE CURRENT_DATE BETWEEN to_date(knvh.datab, 'YYYYMMDD') AND to_date(knvh.datbi, 'YYYYMMDD')                   
),

src AS (
    SELECT DISTINCT  
      kna1.kunnr AS customer,
      knvh.kunn, 
      knvh.hityp, 
      name1,ktokd, 
      knvh.vkorg, 
      knvh.hkunnr,
      vtweg, 
      spart,
    FROM knvh
    JOIN base_kna1 AS kna1
    ON (knvh.kunnr = kna1.kunnr)            
)

SELECT 
  'E03' AS source_sys, 
  * 
FROM  src;


	