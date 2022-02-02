-- Imports

-- Add reference
WITH zc AS (

    SELECT * FROM edw.sape03.zdspdmseqmch_current
),

final AS (

    SELECT 
    MANDT,
    zseqid AS SEQID,
    zseqmch AS SEQMCHID,
    VKORG,
    VTWEG,
    zbrand AS brand_cons,
    land1 AS country_cons,
    VKBUR AS industkey_cons,
    BRSCH AS sales_office_cons,
    kunag AS KUNNR,
    zphsbu AS prodhl1_e03,
    zphdivi AS prodhl2_e03,
    zzdemandgrp AS ZDMDGRP,

    coalesce(kunnr, '') 
        || coalesce(prodhl1_e03, '') 
        || coalesce(prodhl2_e03, '')
        || coalesce(brand_cons, '') 
        || coalesce(country_cons, '')
        || coalesce(sales_office_cons, '') 
        || coalesce(industkey_cons, '') 
    AS zdmdgrp_factors,

    ROW_NUMBER() OVER (PARTITION BY kunag ORDER BY zseqid, zseqmch) AS rn
    FROM zc
)
			
SELECT * FROM final
