
CREATE OR REPLACE PROCEDURE stage.p_build_allocation_rule_102_104(fmthid integer)
 LANGUAGE plpgsql
AS $$
declare
current_posting_week date;
calendar_posting_week date;
begin
	
---
/*
 
 Get records from agg table, rolled up to super SBU, Sku, Week
 
DONE
 
 insert records from this result joined with costing table into results
 
 
 Calculate the average % of net sales for PPV, Duty and Freight, by super sbu
 
 apply these averages to records that do not join to the costing table
 
 insert these records into results table 
 
 allocate from there
 
 * */	 
	 
	 
	/* create temp table for selected period */
	drop table if exists vtbl_date_range ;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				max(dt.fmth_id) AS fiscal_month_id
		from 	ref_data.calendar dt
	where 	dt.fmth_id = fmthid
	--where dt.fmth_id = 202101
	
	;
	 
 ---map _dim_prod_sku_to_super_sbu_map
     drop table if exists _dim_prod_sku_to_super_sbu_map
    ;
    create temporary table _dim_prod_sku_to_super_sbu_map as
    with
        cte_base as (
            select     dp.material,
                    dp.level04_bar as super_sbu,
                    sum(f.amt_usd) as amt_usd
            from     dw.fact_pnl_commercial_stacked f
                    inner join dw.dim_product dp on dp.product_key = f.product_key
            where     f.bar_acct = 'A40110' and
                    lower(dp.level04_bar) != 'unknown'
            group by dp.material,
                    dp.level04_bar
        ),
        cte_rnk as (
            select     base.material,
                    base.super_sbu,
                    base.amt_usd,
                    rank() over(partition by material order by amt_usd desc) as rnk
            from     cte_base as base
        )
        select     rnk.material,
                rnk.super_sbu
        from     cte_rnk as rnk
        where     rnk.rnk = 1
    ;
    
   
    drop table if exists invoice_mat; 
   
create temporary table invoice_mat as (
 select 	a.fiscal_month_id , 
 		max(a.posting_week_enddate) as posting_week_enddate , 
 		a.alloc_material as material, 
 		COALESCE (sbu.super_sbu,'unknown') as super_sbu,
	    sum(a.tran_volume  )  as total_qty, 
	    sum(a.amt_usd) as amt_usd
 from dw.fact_pnl_commercial_stacked a
 inner join _dim_prod_sku_to_super_sbu_map sbu on lower(sbu.material) = lower(a.alloc_material)  
	 inner join vtbl_date_range dt on a.fiscal_month_id  = dt.fiscal_month_id
	 inner join dw.dim_business_unit bu on a.business_unit_key  = bu.business_unit_key 
	 inner join ref_data.data_processing_rule as dpr on dpr.data_processing_ruleid = a.mapped_dataprocessing_ruleid
	 inner join dw.dim_source_system dss on a.source_system_id  = dss.source_system_id 
	where
	  a.bar_acct   = 'A40110' -- Sales Invoice
	  and dpr.dataprocessing_group = 'perfect-data'
	 group by a.fiscal_month_id, a.alloc_material, COALESCE (sbu.super_sbu,'unknown')
	 having sum(a.tran_volume) > 0 
	 	   and sum(amt_usd)>0 
	 );
	

delete from  stage.agm_costing_variance where fiscal_month_id  in (select fiscal_month_id from vtbl_date_range);
insert into stage.agm_costing_variance (sku_match, fiscal_month_id,posting_week_enddate, material, super_sbu, total_qty, ppv_var, duty_var, frght_var )
 select 1 as sku_match,  
	i.fiscal_month_id, 
	i.posting_week_enddate, 
	i.material, 
	i.super_sbu, 
	i.total_qty, 
	i.total_qty * avg_ppv_var as ppv_var,
	i.total_qty * avg_duty_var as duty_var,
	i.total_qty * avg_fgt_abs_var frght_var
from invoice_mat i inner join stage.agm_1070_costing c on i.material = c.matnr and i.fiscal_month_id = c.fiscal_month_id
;


--
--
--
--Select sum(total_qty) volume, sum(ppv_var) ppv_var, material
--from stage.agm_costing_variance
----where lower(material) = 'dck277c2'
--group by material
-- select     dp.material,
--            sum(f.invoice_sales) as  invoice_sales, 
--            sum(net_sales) as net_sales, 
--            sum(tran_volume*-1) as volume
--  from     dw.fact_pnl_commercial f
--  inner join dw.dim_product dp on dp.product_key = f.product_key
--where 	lower(material) = 'dck277c2'
--and fiscal_month_id = 202101 
--group by dp.material
	--Calc Percent of net sales
	--cost/bar_amt by super SBU
	
	drop table if exists pct_net_sales; 
	
 	create temporary table pct_net_sales as 
 	(
	SELECT cv.super_sbu, inv.total_amt, cv.total_ppv, cv.total_duty, cv.total_frght,total_qty,
		cv.total_ppv/inv.total_amt as ppv_pct_net_sales,
		cv.total_duty/inv.total_amt as duty_pct_net_sales,
		cv.total_frght/inv.total_amt as frght_pct_net_sales
	from 
	(
	select fiscal_month_id,super_sbu, sum(ppv_var) total_ppv, sum(duty_var) as total_duty, sum(frght_var) as total_frght,sum(total_qty) as total_qty
	from stage.agm_costing_variance
	group by super_sbu, fiscal_month_id
	) cv inner JOIN 
	(
	SELECT fiscal_month_id,super_sbu, sum( amt_usd) as total_amt
	from  invoice_mat i
	where exists (select 1 from stage.agm_costing_variance cv where  i.material = cv.material and sku_match=1 and i.fiscal_month_id = cv.fiscal_month_id)
	 group by super_sbu, fiscal_month_id
	 ) inv on cv.super_sbu = inv.super_sbu and cv.fiscal_month_id = inv.fiscal_month_id
	cross join vtbl_date_range dt 
	where cv.fiscal_month_id = dt.fiscal_month_id and inv.fiscal_month_id = dt.fiscal_month_id
	);

--
--
--select *
--from pct_net_sales
--Select *
--from stage.agm_costing_variance
--limit 10;
--
--
--Select sum(ppv_var), sum(total_qty),sku_match,super_sbu
--from stage.agm_costing_variance 
--group by sku_match,super_sbu
--order by sku_match
--
--Select sum(ppv_var),sum(ppv_var_1),super_sbu,sum(total_qty),avg(ppv_pct_net_sales)
--from (	select 0 as sku_match,  
--	i.fiscal_month_id, i.posting_week_enddate, i.material, i.super_sbu, i.total_qty, ppv_pct_net_sales,
--	i.total_qty * avg_ppv_per_qty as ppv_var,
--	i.amt_usd * ppv_pct_net_sales as ppv_var_1
----	i.total_qty * duty_pct_net_sales as duty_var,
----	i.total_qty * frght_pct_net_sales as frght_var
--	from invoice_mat i 
--	left join stage.agm_1070_costing c on i.material = c.matnr 
--	left join pct_net_sales r on i.super_sbu = r.super_sbu
--	where c.matnr  is null 
--	) 
--group by super_sbu
----SK chnaged to i.amt_usd * ppv_pct_net_sales as ppv_var
	delete from  stage.agm_costing_variance where sku_match = 0 and fiscal_month_id  in (select fiscal_month_id from vtbl_date_range);

	insert into stage.agm_costing_variance (sku_match,fiscal_month_id,posting_week_enddate,material,super_sbu,total_qty, ppv_var, duty_var,frght_var)
	select 0 as sku_match,  i.fiscal_month_id, i.posting_week_enddate, i.material, i.super_sbu, i.total_qty, 
	i.amt_usd * ppv_pct_net_sales as ppv_var,
	i.amt_usd * duty_pct_net_sales as duty_var,
	i.amt_usd * frght_pct_net_sales as frght_var
	from 
	invoice_mat i 
	left join (
			select distinct material from  stage.agm_costing_variance  c
									inner join vtbl_date_range dt on c.fiscal_month_id  = dt.fiscal_month_id
									where sku_match  =1
									)  c on i.material = c.material 
	left join pct_net_sales r on i.super_sbu = r.super_sbu 
	where c.material  is null 
	;

/*
--debug
	select super_sbu , sku_match,  count(*) as rec_count , sum(total_qty) total_qty,  sum(ppv_var) total_ppv, sum(frght_var) total_frght, sum(duty_var) total_duty
	from stage.agm_costing_variance
	group by super_sbu, sku_match
	order by super_sbu, sku_match
Select sum(ppv_var), sum(duty_var), sum(frght_var) 
from stage.agm_costing_variance
select sku_match, count(*) 
from  stage.agm_costing_variance
group by  sku_match

select *
from stage.agm_costing_variance
limit 10

	*/
----allocate data at transaction level : has leakage issues
/************************************************************************************************************************************/

  /* +ve sales with negative cogs for processing month */ 
       drop table if exists stage_sales_by_sku;
       create temporary table stage_sales_by_sku 
       as 
       Select sum(amt_usd) as invoice_sales, dp.material,acv.super_sbu 
       from dw.fact_pnl_commercial_stacked fpcs 
       inner join dw.dim_product dp on fpcs.product_key = dp.product_key 
       inner join ( select sum(total_bar_amt) as total_bar_amt, material
                           from stage.rate_base_cogs rb 
                           cross join vtbl_date_range dt 
                           where rb.fiscal_month_id = dt.fiscal_month_id 
                    group by material
                    having sum(total_bar_amt) < 0
                    ) rb on dp.material = rb.material 
       inner join stage.agm_costing_variance acv on fpcs.alloc_material = acv.material and fpcs.fiscal_month_id = acv.fiscal_month_id 
       cross join  vtbl_date_range dt 
       where fpcs.fiscal_month_id = dt.fiscal_month_id 
       and bar_acct  in ('A40110')
       ---and lower(material) = '00 20 06 us2'
       group by dp.material,acv.super_sbu
       having sum(amt) > 0;
---502361.74717423
-- Select *
-- from stage_sales_by_sku
-- where material = '3007272L';
--
--Select *
--from stage.agm_costing_variance
--where material = '3007272L'--and fiscal_month_id  = 202101;
--drop table if exists stage.rate_base_cogs_pct_of_total_test; 
--
--create table stage.rate_base_cogs_pct_of_total_test 
--as 
--select *
--from rate_base_cogs_pct_of_total;

       /* rate table based on standard cost */
       drop table if exists rate_base_cogs_pct_of_total;
       create temporary table rate_base_cogs_pct_of_total as 
             with
                    cte_rate_base_cogs as (
                           select       rb.audit_rec_src,
                           			rb.fiscal_month_id,
                                        rb.bar_entity,
                                        rb.soldtocust,
                                        rb.shiptocust,
                                        rb.bar_custno,
                                        rb.material,
                                        rb.bar_product,
                                        rb.bar_brand,
                                        rb.super_sbu,
                                        cast(rb.total_bar_amt as decimal(38,12)) as total_bar_amt,
                                        rb.cost_pool,
                                        rb.bar_currtype,
                                        cast(sum(rb.total_bar_amt) over( partition by rb.material,rb.fiscal_month_id, rb.super_sbu ) as decimal(38,18)) as total_bar_amt_partition,
                                        cast(rb.total_bar_amt as decimal(38,12)) / cast(sum(rb.total_bar_amt) over( partition by rb.material,rb.fiscal_month_id, rb.super_sbu ) as decimal(38,18)) 
                                        as pct_of_total
                           from   stage.rate_base_cogs rb
                           cross join vtbl_date_range dt 
                           inner join stage_sales_by_sku s on rb.material = s.material and s.super_sbu =rb.super_sbu 
                           where rb.fiscal_month_id = dt.fiscal_month_id
                           and rb.total_bar_amt < 0
                    ),cte_rate_base_cogs_1 as (                    
             			select  cte_rb.audit_rec_src,
             			  cte_rb.fiscal_month_id,
                           cte_rb.bar_entity,
                           cte_rb.soldtocust,
                           cte_rb.shiptocust,
                           cte_rb.bar_custno,
                           cte_rb.material,
                           cte_rb.bar_product,
                           cte_rb.bar_brand,
                           cte_rb.super_sbu,
                           cte_rb.total_bar_amt,
                           cte_rb.total_bar_amt_partition,
                           cte_rb.cost_pool,
                           cte_rb.bar_currtype,
                           cte_rb.pct_of_total*cte_rb.total_bar_amt as total_bar_amt_1,
                           cast(sum(cte_rb.pct_of_total*cte_rb.total_bar_amt) over( partition by cte_rb.material,cte_rb.fiscal_month_id, cte_rb.super_sbu ) as decimal(38,18)) as total_bar_amt_partition_1
           	from   cte_rate_base_cogs cte_rb
             where total_bar_amt_partition != 0
             )select        cte_rb.audit_rec_src,
             			  cte_rb.fiscal_month_id,
                           cte_rb.bar_entity,
                           cte_rb.soldtocust,
                           cte_rb.shiptocust,
                           cte_rb.bar_custno,
                           cte_rb.material,
                           cte_rb.bar_product,
                           cte_rb.bar_brand,
                           cte_rb.super_sbu,
                           cte_rb.total_bar_amt_1 as total_bar_amt,
                           cte_rb.cost_pool,
                           cte_rb.bar_currtype,
                           total_bar_amt_partition_1,
                           cast(cte_rb.total_bar_amt_1
                                 / cast(total_bar_amt_partition_1 as decimal(38,8)) as decimal(38,8)) as pct_of_total
             from   cte_rate_base_cogs_1 cte_rb
             where total_bar_amt_partition != 0 ;
          
--Select sum(pct_of_total)
--from rate_base_cogs_pct_of_total
--where material ='20566618R';
--
--select sum(pct_of_total)
--from stage.rate_base_cogs_pct_of_total_test 
--where material ='20566618R';
            
/* debug : should be 1 */ 
            /*
Select sum(pct_of_total),material,fiscal_month_id, super_sbu,avg(total_bar_amt)
from rate_base_cogs_pct_of_total
group by material,fiscal_month_id, super_sbu
having sum(pct_of_total)<1
order by 1
*/
       /* create temp table for exchange_rate */
       drop table if exists vtbl_exchange_rate
       ;
       create temporary table vtbl_exchange_rate as 
             select       rt.fiscal_month_id, 
                           rt.from_currtype,
                           rt.fxrate
             from   {{ source('ref_data', 'hfmfxrates') }} rt
                           inner join vtbl_date_range dt
                                 on     dt.fiscal_month_id = rt.fiscal_month_id 
             where lower(rt.to_currtype) = 'usd'
       ;   

delete from stage.agm_allocated_data_rule_102_104 where fiscal_month_id  = (select fiscal_month_id from vtbl_date_range)
--and bar_acct  in ('AGM-ADJ-DUTY', 'AGM-ADJ-FRGHT','AGM-ADJ-PPV')
and dataprocessing_phase in ('phase 22', 'phase 23', 'phase 24', 'phase 32', 'phase 33', 'phase 34');

/* if dd.range_end_date > calendar_posting_week, then (processing for current month) 
 * posting week = calendar_posting_week - 7 days else 
 * dd.range_end_date  (processing for previous months) 
 */
select distinct cast(wk_end_dte as date) 
into calendar_posting_week
from ref_data.calendar c 
where dy_dte = cast(getdate() as date);
select dd.range_end_date
into current_posting_week
from vtbl_date_range dd;
if current_posting_week >= calendar_posting_week
then 
   current_posting_week = calendar_posting_week - 7; 
end if;
INSERT INTO stage.agm_allocated_data_rule_102_104
(
  source_system,
  fiscal_month_id,
  posting_week_enddate,
  bar_entity,
  bar_acct,
  material,
  bar_product,
  bar_brand,
  soldtocust,
  shiptocust,
  bar_custno,
  dataprocessing_ruleid,
  dataprocessing_outcome_id,
  dataprocessing_phase,
  bar_currtype,
  super_sbu,
  cost_pool,
  allocated_amt,
  allocated_amt_usd,
  audit_loadts
)
SELECT stg.audit_rec_src as source_system,
       stg.fiscal_month_id,
       current_posting_week as posting_week_enddate,
       stg.bar_entity,
       cast('AGM-ADJ-DUTY' as varchar(20)) as bar_acct,
       stg.material,
       stg.bar_product,
       COALESCE(stg.bar_brand, 'unknown') as bar_brand,
       stg.soldtocust,
       stg.shiptocust,
       stg.bar_custno,
       cast(102 as integer) dataprocessing_ruleid,
       cast(1 as integer) dataprocessing_outcome_id,
        case when acv.sku_match = 1 then  cast('phase 22' as varchar(10)) else cast('phase 32' as varchar(10)) end  as dataprocessing_phase,
       stg.bar_currtype,
       stg.super_sbu,
       case when lower(stg.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(acv.duty_var as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12)))* (1/CAST(fx.fxrate as decimal(10,6))) 
       end allocated_amt,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(acv.duty_var as numeric(38,20)) / (1 / cast(stg.pct_of_total as numeric(38,20)))
       end allocated_amt_usd,
       cast(getdate() as timestamp) as audit_loadts
FROM rate_base_cogs_pct_of_total stg 
cross join vtbl_date_range dd 
inner join stage.agm_costing_variance acv 
                                        on  lower(stg.super_sbu) = lower(acv.super_sbu)
                                        and lower(stg.material) = lower(acv.material)
                                        and stg.fiscal_month_id = acv.fiscal_month_id
left outer join vtbl_exchange_rate as fx
                                 on     fx.fiscal_month_id = stg.fiscal_month_id and 
                                        lower(fx.from_currtype) = lower(stg.bar_currtype)
where cast(stg.pct_of_total as numeric(38,12)) !=0
union all 
SELECT stg.audit_rec_src as source_system,
       stg.fiscal_month_id,
       current_posting_week as posting_week_enddate,
       stg.bar_entity,
       cast('AGM-ADJ-FRGHT' as varchar(20)) as bar_acct,
       stg.material,
       stg.bar_product,
       COALESCE(stg.bar_brand, 'unknown') as bar_brand,
       stg.soldtocust,
       stg.shiptocust,
       stg.bar_custno,
       cast(103 as integer) dataprocessing_ruleid,
       cast(1 as integer) dataprocessing_outcome_id,
        case when acv.sku_match = 1 then  cast('phase 23' as varchar(10)) else cast('phase 33' as varchar(10)) end  as dataprocessing_phase,
       stg.bar_currtype,
       stg.super_sbu,
         case when lower(stg.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(acv.frght_var as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12)))* (1/CAST(fx.fxrate as decimal(10,6))) 
       end allocated_amt,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(acv.frght_var as numeric(38,20)) / (1 / cast(stg.pct_of_total as numeric(38,20)))
       end allocated_amt_usd,
       cast(getdate() as timestamp) as audit_loadts
FROM rate_base_cogs_pct_of_total stg 
cross join vtbl_date_range dd 
inner join stage.agm_costing_variance acv 
                                        on  lower(stg.super_sbu) = lower(acv.super_sbu)
                                        and lower(stg.material) = lower(acv.material)
                                        and stg.fiscal_month_id = acv.fiscal_month_id
left outer join vtbl_exchange_rate as fx
                                 on     fx.fiscal_month_id = stg.fiscal_month_id and 
                                        lower(fx.from_currtype) = lower(stg.bar_currtype)
where cast(stg.pct_of_total as numeric(38,12)) !=0
union all 
SELECT stg.audit_rec_src as source_system,
       stg.fiscal_month_id,
       current_posting_week as posting_week_enddate,
       stg.bar_entity,
       cast('AGM-ADJ-PPV' as varchar(20)) as bar_acct,
       stg.material,
       stg.bar_product,
       COALESCE(stg.bar_brand, 'unknown') as bar_brand,
       stg.soldtocust,
       stg.shiptocust,
       stg.bar_custno,
       cast(104  as integer) dataprocessing_ruleid,
       cast(1 as integer) dataprocessing_outcome_id,
       case when acv.sku_match = 1 then  cast('phase 24' as varchar(10)) else cast('phase 34' as varchar(10)) end  as dataprocessing_phase,
       stg.bar_currtype,
       stg.super_sbu,
       case when lower(stg.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(acv.ppv_var as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12)))* (1/CAST(fx.fxrate as decimal(10,6))) 
       end allocated_amt,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(acv.ppv_var as numeric(38,20)) / (1 / cast(stg.pct_of_total as numeric(38,20)))
       
       end allocated_amt_usd,
       cast(getdate() as timestamp) as audit_loadts
FROM rate_base_cogs_pct_of_total stg 
cross join vtbl_date_range dd 
inner join stage.agm_costing_variance acv 
                                        on  lower(stg.super_sbu) = lower(acv.super_sbu)
                                        and lower(stg.material) = lower(acv.material)
                                        and stg.fiscal_month_id = acv.fiscal_month_id
left outer join vtbl_exchange_rate as fx
                                 on     fx.fiscal_month_id = stg.fiscal_month_id and 
                                        lower(fx.from_currtype) = lower(stg.bar_currtype)
where cast(stg.pct_of_total as numeric(38,12)) !=0
;


	end;
  $$
;

CREATE OR REPLACE PROCEDURE stage.p_build_allocation_rule_102_104_gap(fmthid integer)
 LANGUAGE plpgsql
AS $$
 BEGIN 
	 
	 
	 /*
	  * Step 1  Bods transactions
	  * Some bods transactions do not have material.. 
	  		spread these values portortionally based on bods transactiosn that do have material
	  * 
	  * call stage.p_build_allocation_rule_102_104_gap (202101)
	  */
	 
	 

	 	/* create temp table for selected period */
	drop table if exists vtbl_date_range
	;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				max(dt.fmth_id) AS fiscal_month_id
		from 	ref_data.calendar dt
		where 	dt.fmth_id = fmthid
		--where dt.fmth_id  = 202101
	;
	 
	 
	 drop table if exists _dim_prod_sku_to_super_sbu_map ;
	
    create temporary table _dim_prod_sku_to_super_sbu_map as
    with
        cte_base as (
            select     dp.material,
                    dp.level04_bar as super_sbu,
                    sum(f.amt_usd) as amt_usd
            from     dw.fact_pnl_commercial_stacked f
                    inner join dw.dim_product dp on dp.product_key = f.product_key
            where     f.bar_acct = 'A40110' and
                    lower(dp.level04_bar) != 'unknown'
            group by dp.material,
                    dp.level04_bar
        ),
        cte_rnk as (
            select     base.material,
                    base.super_sbu,
                    base.amt_usd,
                    rank() over(partition by material order by amt_usd desc) as rnk
            from     cte_base as base
        )
        select     rnk.material,
                rnk.super_sbu
        from     cte_rnk as rnk
        where     rnk.rnk = 1;
    
	
       
      drop  table if exists calculate_costs ;
      create temporary table calculate_costs 
      as (
	
		Select super_sbu,
			  case when bar_acct = 'AGM-ADJ-DUTY' then 'Reported Duty / Tariffs'
			  	  when  bar_acct = 'AGM-ADJ-FRGHT' then 'Reported Freight'
			  	  when  bar_acct = 'AGM-ADJ-PPV' then 'Reported PPV'
			  end as acct_category, 
			  sum(allocated_amt_usd) cost_var
		from stage.agm_allocated_data_rule_102_104 aadr 
		inner join vtbl_date_range  dd on 	dd.fiscal_month_id = aadr.fiscal_month_id
		--where aadr.dataprocessing_phase in ('phase 22','phase 23','phase 24')
		where aadr.dataprocessing_phase in ('phase 22','phase 23','phase 24', 'phase 32', 'phase 33', 'phase 34')
		group by super_sbu,
			  case when bar_acct = 'AGM-ADJ-DUTY' then 'Reported Duty / Tariffs'
			  	  when  bar_acct = 'AGM-ADJ-FRGHT' then 'Reported Freight'
			  	  when  bar_acct = 'AGM-ADJ-PPV' then 'Reported PPV'
			  end
	
	) ;

	/*--
	select * from calculate_costs
	*/

 -- Now get Percent of variance by Super SBU
	drop table if exists bods_calc_rate;
	create temporary table  bods_calc_rate as
	(
	SELECT 
		a.acct_category, a.super_sbu, a.cost_var, a.cost_var/ b.bods_calc_var_category prcnt_of_gap
	from 
	calculate_costs a 
	inner join 
	(select acct_category, sum(cost_var) as bods_calc_var_category from calculate_costs group by acct_category) b on a.acct_category = b.acct_category
	);
	
/*
	-- should equal 100%..
	select acct_category, sum(prcnt_of_gap) from bods_calc_rate group by acct_category
	select acct_category, sum(prcnt_of_gap), super_sbu from bods_calc_rate group by acct_category, super_sbu
	
	Reported Duty / Tariffs	0.9998
	Reported PPV	1.0000
	Reported Freight	0.9999
*/
	drop table if exists bar_bods_var;
	-- get the BAR and Bods VARIANCE(
	create TEMPORARY table bar_bods_var as 
	(
	select bar.acct_category, bar.bar_cost,  bods.calc_cost,  bar.bar_cost -  COALESCE(bods.calc_cost,0) as bar_calc_var
	from 
	(
	
	select b.acct_category , 
		  sum(a.amt_reported*multiplication_factor) bar_cost
	from ref_data.agm_bnr_financials_extract a 
	inner join ref_data.pnl_acct_agm b on a.account = b.bar_acct
	inner join vtbl_date_range  dd on 	dd.fiscal_month_id = a.fiscal_month_id
	inner join (select distinct name, level4 from ref_data.entity) as rbh on   a.entity = rbh.name
	WHERE 	b.acct_category  in ('Reported Duty / Tariffs', 'Reported Freight','Reported PPV')
		and  a.scenario = 'Actual_Ledger'
    	and rbh.level4 = 'GTS_NA'
	group by  b.acct_category
	) bar
	LEFT JOIN 
	(
	select acct_category, sum(cost_var) calc_cost
	from calculate_costs
	group by acct_category
	) bods on bods.acct_category = bar.acct_category
	
	);
  /*
   select * from bar_bods_var
   */
  
	drop table if exists final_gap;
	create temporary table  final_gap as (
	SELECT b.acct_category, r.super_sbu, b.bar_cost,  b.calc_cost,   cast(b.bar_calc_var as decimal(20,8))  as bar_bods , r.prcnt_of_gap, 
	( cast(b.bar_calc_var as decimal(20,8))  *  r.prcnt_of_gap) gap_to_alloc
	from 
	bar_bods_var b 
	inner join bods_calc_rate r on b.acct_category = r.acct_category 
	);
	delete from  stage.agm_cost_variance_gap_final_gap_transient;
	insert into stage.agm_cost_variance_gap_final_gap_transient(acct_category, super_sbu, bar_cost, calc_cost, bar_bods, prcnt_of_gap, gap_to_alloc)
	select acct_category, super_sbu, bar_cost, calc_cost, bar_bods, prcnt_of_gap, gap_to_alloc
	from final_gap;

/*
	select *
	from final_gap
	where acct_category = 'Reported PPV'
	group by acct_category
	
	select * 
	from stage.agm_cost_variance_gap_final_gap_transient
	order by acct_category, super_sbu
	
	select acct_category,sum(prcnt_of_gap)
	from final_gap
	group by acct_category
	
*/
/*  BEGIN ALLOCATION BY % Cogs  
 */

   /* +ve sales with negative cogs for processing month */ 
       drop table if exists stage_sales_by_sku;
       create temporary table stage_sales_by_sku 
       as 
       Select sum(amt_usd) as invoice_sales, dp.material
       from dw.fact_pnl_commercial_stacked fpcs 
       inner join dw.dim_product dp on fpcs.product_key = dp.product_key 
       inner join ( select sum(total_bar_amt) as total_bar_amt, material
                           from stage.rate_base_cogs rb 
                           cross join vtbl_date_range dt 
                           where rb.fiscal_month_id = dt.fiscal_month_id 
                    group by material
                    having sum(total_bar_amt) < 0
                    ) rb on dp.material = rb.material 
       cross join  vtbl_date_range dt 
       where fpcs.fiscal_month_id = dt.fiscal_month_id 
       and bar_acct  in ('A40110')
       ---and lower(material) = '00 20 06 us2'
       group by dp.material
       having sum(amt) > 0;
      
       /* rate table based on standard cost */
       drop table if exists rate_base_cogs_pct_of_total;
       create temporary table rate_base_cogs_pct_of_total as 
             with
                    cte_rate_base_cogs as (
                           select       rb.fiscal_month_id,
                                        rb.bar_entity,
                                        rb.soldtocust,
                                        rb.shiptocust,
                                        rb.bar_custno,
                                        rb.material,
                                        rb.bar_product,
                                        rb.bar_brand,
                                        rb.super_sbu,
                                        rb.total_bar_amt,
                                        rb.cost_pool,
                                        rb.bar_currtype,
                                        sum(rb.total_bar_amt) over( partition by rb.fiscal_month_id, rb.super_sbu ) as total_bar_amt_partition
                           from   stage.rate_base_cogs rb
                           cross join vtbl_date_range dt 
                           
                           inner join stage_sales_by_sku s on rb.material = s.material 
                           
                           where rb.fiscal_month_id = dt.fiscal_month_id
                    )
             select       cte_rb.fiscal_month_id,
                           cte_rb.bar_entity,
                           cte_rb.soldtocust,
                           cte_rb.shiptocust,
                           cte_rb.bar_custno,
                           cte_rb.material,
                           cte_rb.bar_product,
                           cte_rb.bar_brand,
                           cte_rb.super_sbu,
                           cte_rb.total_bar_amt,
                           cte_rb.total_bar_amt_partition,
                           cte_rb.cost_pool,
                           cte_rb.bar_currtype,
                           CAST(cte_rb.total_bar_amt as decimal(20,8))
                                 / CAST(cte_rb.total_bar_amt_partition as decimal(20,8)) as pct_of_total
             from   cte_rate_base_cogs cte_rb
             where total_bar_amt_partition != 0
       ;
      /*
  --DEBUG
Select sum(pct_of_total), super_sbu
from rate_base_cogs_pct_of_total
group by super_sbu
select *
from rate_base_cogs_pct_of_total 
where super_sbu = 'PTG'
limit 100

*/
      
drop table if exists bnr_gap_to_allocate_for_cv; 
create temporary table bnr_gap_to_allocate_for_cv 
diststyle all 
as 
select super_sbu, 
             sum(case when acct_category = 'Reported Duty / Tariffs' then  gap_to_alloc else 0 end) as gap_to_allocate_duty,
             sum(case when acct_category = 'Reported Freight' then  gap_to_alloc else 0 end) as gap_to_allocate_freight,
             sum(case when acct_category = 'Reported PPV' then  gap_to_alloc else 0 end) as gap_to_allocate_ppv
    from stage.agm_cost_variance_gap_final_gap_transient
    group by  super_sbu;
   
   
   
--select * from  bnr_gap_to_allocate_for_cv
       /* create temp table for exchange_rate */
       drop table if exists vtbl_exchange_rate
       ;
       create temporary table vtbl_exchange_rate as 
             select       rt.fiscal_month_id, 
                           rt.from_currtype,
                           rt.fxrate
             from   {{ source('ref_data', 'hfmfxrates') }} rt
                           inner join vtbl_date_range dt
                                 on     dt.fiscal_month_id = rt.fiscal_month_id 
             where lower(rt.to_currtype) = 'usd'
       ;   
   

      
delete from stage.agm_allocated_data_rule_102_104 where fiscal_month_id  = (select fiscal_month_id from vtbl_date_range)
	and dataprocessing_phase in  ('phase 92', 'phase 93', 'phase 94') ;

INSERT INTO stage.agm_allocated_data_rule_102_104
(
  source_system,
  fiscal_month_id,
  posting_week_enddate,
  bar_entity,
  bar_acct,
  material,
  bar_product,
  bar_brand,
  soldtocust,
  shiptocust,
  bar_custno,
  dataprocessing_ruleid,
  dataprocessing_outcome_id,
  dataprocessing_phase,
  bar_currtype,
  super_sbu,
  cost_pool,
  allocated_amt,
  allocated_amt_usd,
  audit_loadts
)
SELECT cast('adj-cv-tran-gap' as varchar(20)) as source_system,
       dd.fiscal_month_id,
       dd.range_end_date as posting_week_enddate,
       stg.bar_entity,
       cast('AGM-ADJ-DUTY-GAP' as varchar(20)) as bar_acct,
       stg.material,
       stg.bar_product,
       COALESCE( stg.bar_brand, 'unknown') as bar_brand,
       stg.soldtocust,
       stg.shiptocust,
       stg.bar_custno,
       cast(102 as integer) dataprocessing_ruleid,
       cast(1 as integer) dataprocessing_outcome_id,
       cast('phase 92' as varchar(10)) as dataprocessing_phase,
       stg.bar_currtype,
       stg.super_sbu,
      -- cast(stg.pct_of_total as numeric(38,12)) as pct_of_total,
       case when lower(stg.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(cv.gap_to_allocate_duty as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12)))* (1/CAST(fx.fxrate as decimal(10,6))) 
       end allocated_amt,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(cv.gap_to_allocate_duty as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12))) 
       end allocated_amt_usd,
       cast(getdate() as timestamp) as audit_loadts
FROM rate_base_cogs_pct_of_total stg 
cross join vtbl_date_range dd 
inner join bnr_gap_to_allocate_for_cv cv on  lower(stg.super_sbu) = lower(cv.super_sbu)
left outer join vtbl_exchange_rate as fx
                                 on     fx.fiscal_month_id = stg.fiscal_month_id and 
                                        lower(fx.from_currtype) = lower(stg.bar_currtype)
where cast(stg.pct_of_total as numeric(38,12)) !=0
;
INSERT INTO stage.agm_allocated_data_rule_102_104
(
  source_system,
  fiscal_month_id,
  posting_week_enddate,
  bar_entity,
  bar_acct,
  material,
  bar_product,
  bar_brand,
  soldtocust,
  shiptocust,
  bar_custno,
  dataprocessing_ruleid,
  dataprocessing_outcome_id,
  dataprocessing_phase,
  bar_currtype,
  super_sbu,
  cost_pool,
  allocated_amt,
  allocated_amt_usd,
  audit_loadts
)
SELECT cast('adj-cv-tran-gap' as varchar(20)) as source_system,
        dd.fiscal_month_id,
       dd.range_end_date as posting_week_enddate,
       stg.bar_entity,
       cast('AGM-ADJ-PPV-GAP' as varchar(20)) as bar_acct,
       stg.material,
       stg.bar_product,
       COALESCE( stg.bar_brand, 'unknown') as bar_brand,
       stg.soldtocust,
       stg.shiptocust,
       stg.bar_custno,
       cast(104 as integer) dataprocessing_ruleid,
       cast(1 as integer) dataprocessing_outcome_id,
       cast('phase 94' as varchar(10)) as dataprocessing_phase,
       stg.bar_currtype,
       stg.super_sbu,
       --cast(stg.pct_of_total as numeric(38,12)) as pct_of_total,
       case when lower(stg.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(cv.gap_to_allocate_ppv as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12)))* (1/CAST(fx.fxrate as decimal(10,6))) 
       end allocated_amt,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(cv.gap_to_allocate_ppv as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12))) 
       end allocated_amt_usd,
       cast(getdate() as timestamp) as audit_loadts
FROM rate_base_cogs_pct_of_total stg 
cross join vtbl_date_range dd
inner join bnr_gap_to_allocate_for_cv cv on  lower(stg.super_sbu) = lower(cv.super_sbu)
left outer join vtbl_exchange_rate as fx
                                 on     fx.fiscal_month_id = stg.fiscal_month_id and 
                                        lower(fx.from_currtype) = lower(stg.bar_currtype)
where cast(stg.pct_of_total as numeric(38,12)) !=0
;
INSERT INTO stage.agm_allocated_data_rule_102_104
(
  source_system,
  fiscal_month_id,
  posting_week_enddate,
  bar_entity,
  bar_acct,
  material,
  bar_product,
  bar_brand,
  soldtocust,
  shiptocust,
  bar_custno,
  dataprocessing_ruleid,
  dataprocessing_outcome_id,
  dataprocessing_phase,
  bar_currtype,
  super_sbu,
  cost_pool,
  allocated_amt,
  allocated_amt_usd,
  audit_loadts
)
SELECT cast('adj-cv-tran-gap' as varchar(20)) as source_system,
       dd.fiscal_month_id,
       dd.range_end_date as posting_week_enddate,
       stg.bar_entity,
       cast('AGM-ADJ-FRGHT-GAP' as varchar(20)) as bar_acct,
       stg.material,
       stg.bar_product,
       COALESCE( stg.bar_brand, 'unknown') as bar_brand,
       stg.soldtocust,
       stg.shiptocust,
       stg.bar_custno,
       cast(103 as integer) dataprocessing_ruleid,
       cast(1 as integer) dataprocessing_outcome_id,
       cast('phase 93' as varchar(10)) as dataprocessing_phase,
       stg.bar_currtype,
       stg.super_sbu,
       --cast(stg.pct_of_total as numeric(38,12)) as pct_of_total,
       case when lower(stg.super_sbu) = 'ptg' then 'PTG' else 'Non-PTG' end as cost_pool,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(cv.gap_to_allocate_freight as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12)))* (1/CAST(fx.fxrate as decimal(10,6))) 
       end allocated_amt,
       case when cast(stg.pct_of_total as numeric(38,12)) =0 
               then 0 
               else cast(cv.gap_to_allocate_freight as numeric(38,12)) / (1 / cast(stg.pct_of_total as numeric(38,12))) 
       end allocated_amt_usd,
       cast(getdate() as timestamp) as audit_loadts
FROM rate_base_cogs_pct_of_total stg 
cross join vtbl_date_range dd
inner join bnr_gap_to_allocate_for_cv cv
                                        on  lower(stg.super_sbu) = lower(cv.super_sbu)
left outer join vtbl_exchange_rate as fx
                                 on     fx.fiscal_month_id = stg.fiscal_month_id and 
                                        lower(fx.from_currtype) = lower(stg.bar_currtype)
where cast(stg.pct_of_total as numeric(38,12)) !=0
;
	
END; 
  $$
;

CREATE OR REPLACE PROCEDURE stage.p_build_source_1070_costing(fmthid integer)
 LANGUAGE plpgsql
AS $$
 BEGIN 
	
/*
	 --Join for date Context
Invoices
inner join standard on material = material and period_enddate between FromDate and ToDate
inner join current on standard.material = current.material and current.fromdate between standard.fromdate and Todate
WHERE 
current.fromdate is the latest before i.period_endDate
	 
	 
*/
	 
--Invoices are used to limit the list of sku's needed for costing. 
	/* create temp table for selected period */
	drop table if exists vtbl_date_range ;
	create temporary table vtbl_date_range as 
		select 	cast(min(dt.dy_dte) as date) as range_start_date ,
				cast(max(dt.dy_dte) as date) as range_end_date,
				cast(max(dt.fmth_begin_dte) as date) as first_of_month,
				max(dt.fmth_id) AS fiscal_month_id
		from 	ref_data.calendar dt
	where 	dt.fmth_id = fmthid
	
;
	
 ---map _dim_prod_sku_to_super_sbu_map
     drop table if exists _dim_prod_sku_to_super_sbu_map
    ;
    create temporary table _dim_prod_sku_to_super_sbu_map as
    with
        cte_base as (
            select     dp.material,
                    dp.level04_bar as super_sbu,
                    sum(f.amt_usd) as amt_usd
            from     dw.fact_pnl_commercial_stacked f
                    inner join dw.dim_product dp on dp.product_key = f.product_key
            where     f.bar_acct = 'A40110' and
                    lower(dp.level04_bar) != 'unknown'
            group by dp.material,
                    dp.level04_bar
        ),
        cte_rnk as (
            select     base.material,
                    base.super_sbu,
                    base.amt_usd,
                    rank() over(partition by material order by amt_usd desc) as rnk
            from     cte_base as base
        )
        select     rnk.material,
                rnk.super_sbu
        from     cte_rnk as rnk
        where     rnk.rnk = 1
    ;
    
   
    drop table if exists invoice; 
   
create temporary table invoice as (
 select 	a.fiscal_month_id , 
 		max(a.posting_week_enddate) as posting_week_enddate , 
 		a.alloc_material as material, 
 		COALESCE (sbu.super_sbu,'unknown') as super_sbu,
	    sum(a.tran_volume  )  as total_qty, 
	    sum(a.amt_usd) as amt_usd
 from dw.fact_pnl_commercial_stacked a
 inner join _dim_prod_sku_to_super_sbu_map sbu on lower(sbu.material) = lower(a.alloc_material)  
	 inner join vtbl_date_range dt on a.fiscal_month_id  = dt.fiscal_month_id
	 inner join dw.dim_business_unit bu on a.business_unit_key  = bu.business_unit_key 
	 inner join ref_data.data_processing_rule as dpr on dpr.data_processing_ruleid = a.mapped_dataprocessing_ruleid
	 inner join dw.dim_source_system dss on a.source_system_id  = dss.source_system_id 
	where
	  a.bar_acct   = 'A40110' -- Sales Invoice
	  and dpr.dataprocessing_group = 'perfect-data'
	    and sbu.super_sbu <> 'Product_None'
	 group by a.fiscal_month_id, a.alloc_material, COALESCE (sbu.super_sbu,'unknown')
	 having sum(a.tran_volume) > 0 
	 	   and sum(amt_usd)>0 
	 );
	

	 --standard Costs
	 drop table if exists vtbl_keko_standard;
	 
	--125 sec was  Now is 2 minutes
	 create temporary table vtbl_keko_standard  as (
	
	with vkeko as (
	 Select kalnr, matnr, bwkey
	,werks, klvar, kadat as fromdate , bidat as todate,  tvers, feh_sta  ,losgr 
	,bzobj
	,kalka, kadky
	 ,kkzma
	 ,bwvar
	from {{ source('sapc11', 'keko') }} k
	inner join vtbl_date_range dt on dt.first_of_month between cast(k.kadat as date) and cast(k.bidat as date)
	where 
		 --ZPC7 Std cost   
		klvar = 'ZPC7' 
		and feh_sta = 'FR'
	    and tvers  = '01' -- harded coded value should always be 1 for ZPC7 Records
       and trim(kkzma) = ''   -- Extended manually
       and bzobj = 0  -- Ref. object
      and k.matnr  in (select distinct material from invoice)  -- limit to materials with sales
	)	
        
   	select  a.matnr , b.klvar , a.bwkey as plant,  b.kalka, b.bwvar ,b.losgr,  cast(b.fromdate as date) fromdate, cast(b.todate as date) todate, g.waers as from_currtype,
	        	
	       ((c.kst001 + c.kst003 + c.kst005 + c.kst007 + c.kst009 + c.kst025 + c.kst027 + c.kst029+ c.kst031 + c.kst035) - (c.kst002 + c.kst004 + c.kst006 + c.kst008 + c.kst010 + c.kst026 + c.kst028 +  c.kst030+ c.kst032 + c.kst036) )/b.losgr as standard_tot_matl,
        	( c.kst005 - c.kst006)/ b.losgr as standard_fgt_abs,
	        ( c.kst035 - c.kst036)/ b.losgr as standard_duty
	        -- ,row_number() over (partition by a.matnr, a.bwkey order by b.fromdate desc  ) as row_nbr
       	FROM {{ source('sapc11', 'mbew') }}  a
		    inner JOIN vkeko  b ON  a.kaln1 = cast(b.kalnr as int) AND a.matnr = b.matnr AND  a.bwkey = b.werks 
		   left join {{ source('sapc11', 'keph') }} c on b.bzobj = coalesce(c.bzobj,'') and b.kalnr = c.kalnr and b.kalka = c.kalka and b.kadky = c.kadky and b.tvers = c.tvers  and b.kkzma = c.kkzma and b.bwvar = c.bwvar 
	       left join  {{ source('sapc11', 't001k') }} f on a.bwkey = f.bwkey 
			left join {{ source('sapc11', 't001') }} g on f.bukrs = g.bukrs 
    	where    
	        --these fields defaulted from ABAP program -- keeping them to keep logic same
	       COALESCE (trim(c.kkzst),'')  = '' -- Lower levels
        	and COALESCE (trim(c.losfx),'') = ''
	     	and COALESCE (trim(c.kkzmm),'') = ''
	        and coalesce (a.matnr,'') <> ''       
);  

-- WE now have standard costs for the materials of the month
-- get Current Costs
     --2m 1
     drop table if exists vtbl_keko_current;
	 
	 create temporary table vtbl_keko_current  as (  
     
     with vkeko as 
     	(
	    Select c.kalnr, c.matnr, c.bwkey
			,c.werks, c.klvar, c.kadat as fromdate , c.bidat as todate,  c.tvers, c.feh_sta, c.losgr 
			,c.bzobj
			,c.kalka
			,c.kadky
			 ,c.kkzma
			 ,c.bwvar
		 from {{ source('sapc11', 'keko') }} c
	 --	inner join vtbl_keko_standard s on c.matnr = s.matnr and c.werks = s.plant and cast(c.kadat as date) between s.fromdate and s.todate
	 	--Check this Join with Stan and Ken -- do we want the current cssts that belong to this Standard cost.. if so keep this join
	  --	inner join vtbl_date_range dt on  dt.first_of_month >= cast(c.kadat as date) 
	  	inner join vtbl_date_range dt on dt.first_of_month between cast(c.kadat as date) and cast(c.bidat as date)
	  -- Limit to CC's with start date prior to first day of month
	  	where 
			 --ZPC8 Current cost   
			c.klvar = 'ZPC8' 
			and c.feh_sta = 'KA'
	        and trim(c.kkzma) = ''   -- Extended manually
	        and c.bzobj = 0  -- Ref. object   
	        and cast(c.kadat as date) >= dateadd(year,-2, dt.first_of_month)
	  		and c.matnr in (select matnr from vtbl_keko_standard group by  matnr) -- limit to materials that have a Standard Cost
   )
    select 
    a.matnr , b.klvar , a.bwkey as plant,  b.kalka, b.bwvar ,b.losgr,  cast(b.fromdate as date) fromdate, cast(b.todate as date) todate, g.waers as from_currtype,
	        	
	       ((c.kst001 + c.kst003 + c.kst005 + c.kst007 + c.kst009 + c.kst025 + c.kst027 + c.kst029+ c.kst031 + c.kst035) - (c.kst002 + c.kst004 + c.kst006 + c.kst008 + c.kst010 + c.kst026 + c.kst028 +  c.kst030+ c.kst032 + c.kst036) )/b.losgr as current_tot_matl,
        	( c.kst005 - c.kst006)/ b.losgr as current_fgt_abs,
	        ( c.kst035 - c.kst036)/ b.losgr as current_duty
   -- ,row_number() over (partition by a.matnr, a.bwkey order by b.fromdate desc) as row_count
    FROM {{ source('sapc11', 'mbew') }}  a
	inner JOIN vkeko  b ON  a.kaln1 = cast(b.kalnr as int) AND a.matnr = b.matnr AND  a.bwkey = b.werks 
	left join {{ source('sapc11', 'keph') }} c on b.bzobj = coalesce(c.bzobj,'') and b.kalnr = c.kalnr and b.kalka = c.kalka and b.kadky = c.kadky and b.tvers = c.tvers  and b.kkzma = c.kkzma and b.bwvar = c.bwvar 
	left join  {{ source('sapc11', 't001k') }} f on a.bwkey = f.bwkey 
	left join {{ source('sapc11', 't001') }} g on f.bukrs = g.bukrs    	
    where    
        --these fields defaulted from ABAP program -- keeping them to keep logic same
         COALESCE (trim(c.kkzst),'')  = '' -- Lower levels
    	and COALESCE (trim(c.losfx),'') = ''
        and COALESCE (trim(c.kkzmm),'') = ''
        and coalesce (a.matnr,'') <> ''  
    
    );

   
	    /* create temp table for exchange_rate */
       drop table if exists vtbl_exchange_rate
       ;
       create temporary table vtbl_exchange_rate as 
             select       rt.fiscal_month_id, 
                           rt.from_currtype,
                           rt.fxrate
             from   {{ source('ref_data', 'hfmfxrates') }} rt
                           inner join vtbl_date_range dt
                                 on     dt.fiscal_month_id = rt.fiscal_month_id 
             where lower(rt.to_currtype) = 'usd'
       ;   
  
 delete from stage.agm_1070_costing_base where fiscal_month_id = fmthid;     
      
insert into stage.agm_1070_costing_base (
	matnr, plant, st_fromdate, st_todate, from_currtype,standard_tot_matl, standard_fgt_abs, standard_duty,
	  cc_fromdate, cc_todate,  current_tot_matl,  current_fgt_abs,  current_duty,
   	standard_pp, current_pp, fgt_abs_var, duty_var, ppv_var,fiscal_month_id
    )
 --create transient table     
with cc_base as (
	select  matnr,plant, fromdate, todate, from_currtype, current_tot_matl, current_fgt_abs, current_duty
	, ROW_NUMBER () over (Partition by matnr, plant order by fromdate desc) as row_nbr
	from vtbl_keko_current
	)
	
   select 
   	s.matnr, s.plant, s.fromdate st_fromdate, s.todate st_todate, s.from_currtype,
	  standard_tot_matl *  coalesce(fx.fxrate,1) as standard_tot_matl,
	  standard_fgt_abs *  coalesce(fx.fxrate,1) as standard_fgt_abs,
	  standard_duty *  coalesce(fx.fxrate,1) as standard_duty,
	  c.fromdate as cc_fromdate, c.todate as cc_todate,
	  current_tot_matl *  coalesce(fx.fxrate,1) as current_tot_matl,
	  current_fgt_abs * coalesce(fx.fxrate,1) as current_fgt_abs,
	  current_duty *  coalesce(fx.fxrate,1) as current_duty,
   	(standard_tot_matl - (standard_fgt_abs + standard_duty)) * coalesce(fx.fxrate,1) as standard_pp,
   	(current_tot_matl - (current_fgt_abs + current_duty)) * coalesce(fx.fxrate,1) as current_pp,
   	(standard_fgt_abs -  current_fgt_abs) * coalesce(fx.fxrate,1) as fgt_abs_var,
   	(standard_duty  - current_duty) *  coalesce(fx.fxrate,1) as duty_var,
   	((standard_tot_matl - (standard_fgt_abs + standard_duty))  - (current_tot_matl - (current_fgt_abs + current_duty)))   * coalesce(fx.fxrate,1) ppv_var,
    fmthid as fiscal_month_id
   	
    from vtbl_keko_standard s 
    inner join cc_base c on s.matnr = c.matnr and s.plant = c.plant 
    	--WARD   this is the line to decide on
    	--and c.fromdate >= s.fromdate
	left join vtbl_exchange_rate as fx on  lower(fx.from_currtype) = lower(s.from_currtype)
	where c.row_nbr = 1
--order by s.matnr , s.plant
;

delete from stage.agm_1070_costing where fiscal_month_id = fmthid;
insert into stage.agm_1070_costing(
	matnr, fiscal_month_id,
	avg_standard_tot_matl,
	avg_standard_fgt_abs,
	avg_standard_duty,
	avg_current_tot_matl,
	avg_current_fgt_abs,
	avg_current_duty,
	avg_standard_pp, 
	avg_current_pp,
	avg_fgt_abs_var, 
	avg_duty_var, 
	avg_ppv_var
)
select matnr, fiscal_month_id,
	avg(standard_tot_matl) avg_standard_tot_matl,
	avg(standard_fgt_abs) avg_standard_fgt_abs,
	avg(standard_duty) avg_standard_duty,
	avg(current_tot_matl ) avg_current_tot_matl,
	avg(current_fgt_abs) avg_current_fgt_abs,
	avg(current_duty) avg_current_duty,
	avg(standard_pp) avg_standard_pp, 
	avg(current_pp) avg_current_pp,
	avg(fgt_abs_var) avg_fgt_abs_var, 
	avg(duty_var) avg_duty_var, 
	avg(ppv_var) avg_ppv_var
from stage.agm_1070_costing_base
where fiscal_month_id = fmthid
group by matnr, fiscal_month_id
;
end;
$$
;