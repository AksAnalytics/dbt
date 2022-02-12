
CREATE OR REPLACE PROCEDURE stage.p_build_source_core_tran_delta_agg_agm(fmthid integer)
 LANGUAGE plpgsql
AS $$
	/*
	 * 		
	 *		call stage.p_build_source_core_tran_delta_agg_agm (202101)
	 * 		select count(*) from stage.bods_core_transaction_agg_agm;
	 * 		select 	dataprocessing_ruleid, count(*) from stage.bods_core_transaction_agg_agm group by dataprocessing_ruleid
	 * 		grant execute on procedure stage.p_build_source_core_tran_delta_agg_agm(fmthid integer) to group "g-ada-rsabible-sb-ro";
	 * 
	 * 		TODO:
	 * 			Super Accounts (ref_data.pnl_acct_agm)
	 *        06/02 : sk : added bar_amt_usd column
	 * 
	 */
BEGIN   
	
	
	DROP TABLE IF EXISTS stage_bods_core_transaction_agg_agm
	;
	CREATE TEMPORARY TABLE stage_bods_core_transaction_agg_agm
	(
		org_tranagg_agm_id 		bigint NOT NULL DEFAULT "identity"(200247, 0, '1,1'::character varying::text),
		audit_rec_src 			varchar(10) NOT NULL,
		fiscal_month_id 		int4 NOT NULL,
		bar_entity 				varchar(5) NOT NULL,
		bar_acct_category		varchar(50) NULL,
		bar_acct 				varchar(10) NOT NULL,
		shiptocust 				varchar(50) NULL,
		soldtocust 				varchar(50) NULL,
		bar_custno 				varchar(50) NULL,
		material 				varchar(50) NULL,
		bar_product 			varchar(50) NULL,
		bar_brand 				varchar(50) NULL,
		bar_amt 				numeric(38,8) NOT NULL,
		bar_amt_usd				numeric(38,8) NOT NULL,
		bar_currtype 			varchar(10) NOT NULL,
		tran_volume 			numeric(38,8) NOT NULL,
		uom 					varchar(20) NULL,
		posting_week_enddate 	date NOT NULL,
		dataprocessing_ruleid 	int4 NOT NULL,
		audit_loadts 			date NOT NULL DEFAULT getdate()
	)
	DISTSTYLE KEY
	DISTKEY (org_tranagg_agm_id)
	SORTKEY (posting_week_enddate)
	;

	/* create temp table for exchange_rate */
	drop table if exists vtbl_exchange_rate
	;
	create temporary table vtbl_exchange_rate as 
		select 	rt.fiscal_month_id, 
				rt.from_currtype,
				rt.fxrate
		from 	{{ source('ref_data', 'hfmfxrates') }} rt
		where 	lower(rt.to_currtype) = 'usd'
				AND fiscal_month_id = fmthid ;
	INSERT INTO stage_bods_core_transaction_agg_agm ( 
				audit_rec_src,
				fiscal_month_id,
				posting_week_enddate,
				bar_entity,
				bar_acct_category,
				bar_acct,
				shiptocust,
				soldtocust,
				bar_custno,
				material,
				bar_product,
				bar_brand,
				bar_currtype,
				uom,
				bar_amt,
				bar_amt_usd,
				tran_volume,
				dataprocessing_ruleid,
				audit_loadts
		)
		SELECT	src.audit_rec_src,
				src.fiscal_month_id,
				src.posting_week_enddate, 
				src.bar_entity,
				agm_acct.acct_category as bar_acct_category,
				src.bar_acct,
				src.shiptocust,
				src.soldtocust, 
				src.bar_custno,
				src.material, 
				src.bar_product,
				src.bar_brand,
				src.bar_currtype,
	   		 	lower(src.quanunit) as uom,
				sum(cast(src.bar_amt as numeric(19,6))) * -1 as bar_amt,
				sum(case 
					when rt.fxrate is not null then rt.fxrate * cast(src.bar_amt as numeric(19,6)) 
					else cast(src.bar_amt as numeric(19,6))
				end) * -1 as bar_amt_usd,
	 			sum(cast(isnull(src.quantity,0) as decimal(38,8))) as tran_volume,
				dpr_agm.data_processing_ruleid AS dataprocessing_ruleid,
				getdate() as audit_loadts
		from 	stage.core_tran_delta_agm as src
				inner join ref_data.pnl_acct_agm as agm_acct on agm_acct.bar_acct = src.bar_acct 
				inner join ref_data.data_processing_rule_agm as dpr_agm on dpr_agm.bar_acct_category = agm_acct.acct_category
				left join vtbl_exchange_rate rt on src.fiscal_month_id = rt.fiscal_month_id and 
						lower(rt.from_currtype) = lower(src.bar_currtype)
		where 	src.fiscal_month_id = fmthid
		group by src.audit_rec_src,
				src.fiscal_month_id,
				src.posting_week_enddate, 
				src.bar_entity,
				agm_acct.acct_category,
				src.bar_acct,
				src.shiptocust,
				src.soldtocust, 
				src.bar_custno,
				src.material, 
				src.bar_product,
				src.bar_brand,
				src.bar_currtype,
	   		 	lower(src.quanunit),
	   		 	dpr_agm.data_processing_ruleid
	;
	/* delete from bods_core_transaction_agg_agm */
	delete 
	from 	stage.bods_core_transaction_agg_agm
	where 	0=0 and 
			fiscal_month_id = fmthid
	;
	INSERT INTO stage.bods_core_transaction_agg_agm ( 
				audit_rec_src,
				fiscal_month_id,
				posting_week_enddate,
				bar_acct_category,
				bar_entity,
				bar_acct,
				shiptocust,
				soldtocust,
				bar_custno,
				material,
				bar_product,
				bar_brand,
				bar_currtype,
				uom,
				bar_amt,
				bar_amt_usd,
				tran_volume,
				dataprocessing_ruleid,
				audit_loadts
		)
		SELECT	stg.audit_rec_src,
				stg.fiscal_month_id,
				stg.posting_week_enddate, 
				stg.bar_acct_category,
				stg.bar_entity,
				stg.bar_acct,
				stg.shiptocust,
				stg.soldtocust, 
				stg.bar_custno,
				stg.material, 
				stg.bar_product,
				stg.bar_brand,
				stg.bar_currtype,
	   		 	stg.uom,
				stg.bar_amt,
				stg.bar_amt_usd,
	 			(case when stg.audit_rec_src = 'sap_lawson' then 1 else -1 end) * stg.tran_volume as tran_volume,
				stg.dataprocessing_ruleid,
				stg.audit_loadts
		from 	stage_bods_core_transaction_agg_agm stg
	;
exception
when others then raise info 'exception occur while ingesting data in stage.bods_core_transaction_agg_agm';
end;
$$
;