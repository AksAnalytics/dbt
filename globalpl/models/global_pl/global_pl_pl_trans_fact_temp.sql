{{config(
    materialized = 'table',
    schema = 'global_pl'
)}}

SELECT * FROM {{ ref('sp_sbd_dm_trans_daily_insert') }}
UNION ALL
SELECT * FROM {{ ref('sp_sbd_dm_trans_monthly_insert')}}