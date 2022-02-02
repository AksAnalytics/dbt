-- Import

WITH hfm_vw_hfm_actual_trans_current AS (
     SELECT * FROM hfm_vw_hfm_actual_trans_current
),

year_period_selector AS (
    SELECT DISTINCT 
      year, 
      period,
       
      year || 
      (CASE 
        WHEN period = 'Jan' THEN '01' 
        when period = 'Feb' THEN '02'
        when period = 'Mar' THEN '03'
        when period = 'Apr' THEN '04'
        when period = 'May' THEN '05'
        when period = 'Jun' THEN '06'
        when period = 'Jul' THEN '07'
        when period = 'Aug' THEN '08'
        when period = 'Sep' THEN '09'
        when period = 'Oct' THEN '10'
        when period = 'Nov' THEN '11'
        when period = 'Dec' THEN '12'
       END) AS year_period, 
     
       acct, 
       custom1, 
       custom2, 
       amt
     
    FROM hfm_vw_hfm_actual_trans_current
    WHERE bar_acct ='PLRATE' 

),

union_one AS (
    
    SELECT * 
    FROM year_period_selector
    WHERE year = to_char(current_date, 'YYYY') 
      AND year_period < (to_char(current_date, 'YYYY') || to_char(current_date, 'MM')) 

)

union_two AS (
    SELECT * FROM year_period_selector
    WHERE year <> to_char(current_date, 'YYYY')  
),

union_all AS (
    
    SELECT * FROM year_period_selector
    UNION
    SELECT * FROM union_one
    UNION 
    SELECT * FROM union_two
),


final AS (

    SELECT DISTINCT 
    year,
    period,
    year_period,

    CASE
      WHEN custom1 = 'CNY' THEN 'RMB'
      WHEN custom1 = 'MXP' THEN 'MXN' 
      ELSE custom1
    END AS from_curr,

    custom2 AS to_curr,
    MAX(amt) AS amt

    FROM union_all
    GROUP BY 
      year,
      period,
      year_period,
      from_curr,
      to_curr 
)

SELECT * FROM final