-- Import Statements
WITH base_co AS (
    SELECT * FROM edw.edw_consolidated.consolidated_orders
),

base_cd AS (
    SELECT * FROM edw.edw_consolidated.consolidated_deliveries
),

"of" AS (

    SELECT * FROM edw_stage.otif_filter
),

-- Staging

co_staging AS (
    SELECT 
      source_sys,
      salesordnum_cons,
      salesorditem_cons,
      ord_stat_crdt,
      ord_req_dl_dte,
      ORD_DLV_BLCK_SSK,
      orderqty_cons,
      svclvldate_cons::text,
      reject_reason_cd,
      rejectqty_cons
    FROM base_co
),

cd_staging AS (
	SELECT
	  delivnum_cons,
	  delivitem_cons,
	  delivqty_cons,
	  ovdl_est_act_dlv_dte,
	  actgidate_cons,
	  odlv_orig_qty,
	  gidate_cons,
	  odlv_otif_dte::text,
	  refdoc_cons,
	  refdoc_cons
	FROM base_cd

),

otif_inner_subquery AS (
	SELECT
	  "of".value
	FROM "of"
	WHERE "of".source_sys IN (SELECT DISTINCT source_sys FROM base_co)
	  AND "of".field::text = 'rej_code'::varchar::text
),

inner_from_query AS (
	SELECT
	  co.source_sys,
	  co.salesordnum_cons,
	  co.salesorditem_cons,
	  cd.delivnum_cons,
	  cd.delivitem_cons,
	  co.ord_stat_crdt,
	  co.ord_req_dl_dte,
	  co.ORD_DLV_BLCK_SSK,
	  cd.delivqty_cons,
	  cd.ovdl_est_act_dlv_dte,
	  cd.actgidate_cons,
	  cd.odlv_orig_qty,
	  co.orderqty_cons,
	  cd.gidate_cons, 
	  cd.actgidate_cons,

	  CASE 
		  WHEN cd.odlv_otif_dte > co.svclvldate_cons OR 
			   co.svclvldate_cons < SOMETHING AND 
			   (cd.delivnum_cons IS NULL OR cd.odlv_otif_dte IS NULL)
		    THEN 0::numeric::numeric(18,0)
		  WHEN co.svclvldate_cons > SOMETHING AND 
			   (cd.delivnum_cons IS NULL OR cd.odlv_otif_dte IS NULL)
		    THEN NULL::numeric::numeric(18,0)
		  ELSE cd.delivqty_cons
	  END AS otif_qty,

	  CASE 
		  WHEN (co.reject_reason_cd IN (otif_inner_subquery)
		    THEN CASE
				   WHEN (co.orderqty_cons - co.rejectqty_cons) = 0::numeric::numeric(18,0)
					 THEN 0::numeric::numeric(18,0)::numeric(38,10)
				   ELSE CASE
						  WHEN cd.odlv_otif_dte > co.svclvldate_cons OR
							   co.svclvldate_cons < SOMETHING AND
							   (cd.delivnum_cons IS NULL OR cd.odlv_otif_dte IS NULL)
							THEN 0:numeric:numeric(18,0)
						  WHEN co.svclvldate_cons > SOMETHING AND
							   (cd.delivnum_cons IS NULL OR cd.odlv_otif_dte IS NULL)
						    THEN NULL::numeric:numeric(18,0)
						  ELSE cd.delivqty_cons
						END::numeric(38,10) / (co.orderqty_cons - co.rejectqty_cons) * 100::numeric::numeric(18,0)
				  END
		  ELSE CASE
				  WHEN co.orderqty_cons = 0::numeric::numeric(18,0)::numeric(38,10)
				    THEN 0::numeric::numeric(18,0)::numeric(38,10)
				  ELSE CASE 
					     WHEN cd.odlv_otif_dte > co.svclvldate_cons OR
							  co.svclvldate_cons < SOMETHING AND
							  (cd.delivnum_cons IS NULL OR cd.odlv_otif_dte IS NULL)
						   THEN 0::numeric::numeric(18,0)
					      WHEN co.svclvldate_cons > SOMETHING AND
							   (cd.delivnum_cons IS NULL OR cd.odlv_otif_dte IS NULL)
						    THEN NULL::numeric::numeric(18,0)
					      ELSE cd.delivqty_cons
					    END::numeric(38,10) / co.orderqty_cons * 100::numeric::numeric(18,0)
		  END
	  END AS otif_pct

	FROM co_staging co
	LEFT OUTER JOIN cd_staging cd
	  ON co.source_sys = cd.source_sys 
	 AND co.salesordnum_cons = cd.refdoc_cons
	 AND co.salesorditem_cons = cd.refitem_cons
	WHERE co.source_sys = 'EO3'
),

outer_from_query AS (
	SELECT
	  source_sys,
	  salesordnum_cons,
	  salesorditem_cons,
	  delivnum_cons,
	  delivitem_cons,
	  ord_stat_crdt,
	  ORD_DLV_BLCK_SSK,
	  ord_req_dl_dte,
	  ovdl_est_act_dlv_dte,
	  gidate_cons,
	  actgidate_cons,
	  otif_qty,

	  CASE 
	    WHEN otif_pct = 100 THEN 'OTIF'::varchar
		WHEN actgidate_cons::int > gidate_cons::int THEN 'DC Delay'::varchar
		WHEN ord_stat_crdt IN ('B', 'C') THEN 'Credit Issue':: varchar
		WHEN ord_stat_crdt NOT IN ('B', 'C') 
		 AND ORD_DLV_BLCK_SSK <> ''
		 AND ord_req_dl_dte::int <= ovdl_est_act_dlv_dte::int 
		 THEN 'Delivery Block'::varchar
		WHEN ord_stat_crdt NOT IN ('B', 'C')
		 AND ORD_DLV_BLCK_SSK = ''
		 AND ord_req_dl_dte:: int <= ovdl_est_act_dlv_dte::int 
		 THEN 'ROD Unrealistic'::varchar
		WHEN delivitem_cons < otif_qty THEN 'Other Issue'::varchar
		WHEN delivqty_cons <> odlv_orig_qty
		 AND (actgidate_cons IS NULL OR actgidate_cons = '') 
		 THEN 'Product Availability'::varchar
		ELSE 'Product Availability'::varchar
	  END as root_code
	FROM inner_from_query
),

final AS (
	SELECT 
	  source_sys,
	  salesordnum_cons,
	  salesorditem_cons,
	  delivnum_cons,
	  delivitem_cons,

	  CASE 
	    WHEN root_code IN ('Delviery Block', 'ROD Unrealistic') THEN root_code::varchar
	    ELSE NULL
	  END AS root_code_l2,

	  CASE 
	    WHEN root_code IN ('OITF', 'DC Delay', 'Credit Issue', 'Other Issue', 'Product Availability') THEN root_code::varchar
	    ELSE 'SOM'::varchar
	  END AS root_code_l1,

	  'Y' AS atcv_flag,
	  current_timestamp as etl_crte_ts,
	  NULL::timestamp AS etl_updt_ts

	FROM outer_from_query
)

SELECT * FROM final