Select * FROM (
SELECT 1 AS QRY_NBR, *,
 '0' as "HEADER_CHANGE_SEQ",
'C' as "HEADER_CHANGE_OPER",
NULL as "HEADER_CHANGE_MASK",
NULL as "HEADER_STREAM_POSITION",
NULL as "HEADER_OPERATION",
NULL as "HEADER_TRANSACTION_ID",
NULL as "HEADER_TIMESTAMP"
FROM {{ source('SAP_C11', 'VW_TVKO') }}
--WHERE CAST(EVENTDTS AS DATE) <=  TO_DATE('2021-11-30')
UNION ALL
SELECT 2 AS QRY_NBR, *
FROM {{ source('SAP_C11', 'VW_TVKO_CHANGES') }}
--WHERE CAST(EVENDTS AS DATE) <= TO_DATE('2021-11-30')
) x
QUALIFY
RANK() OVER (PARTITION BY MANDT,VKORG,{{ var('src_grain') }} ORDER BY QRY_NBR ASC, HEADER_CHANGE_SEQ DESC) = 1