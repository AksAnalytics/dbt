CREATE VIEW dw.vw_dim_customer AS SELECT dc.customer_key, dc.customer_id, dc.soldto_number, dc.shipto_number, dc.soldto_name, dc.shipto_name, COALESCE(restate.base_customer, dc.base_customer) AS base_customer, COALESCE(restate.base_customer_desc, dc.base_customer_desc) AS base_customer_desc, COALESCE(restate.level01_bar, dc.level01_bar) AS level01_bar, COALESCE(restate.level02_bar, dc.level02_bar) AS level02_bar, COALESCE(restate.level03_bar, dc.level03_bar) AS level03_bar, COALESCE(restate.level04_bar, dc.level04_bar) AS level04_bar, COALESCE(restate.level05_bar, dc.level05_bar) AS level05_bar, COALESCE(restate.level06_bar, dc.level06_bar) AS level06_bar, COALESCE(restate.level07_bar, dc.level07_bar) AS level07_bar, COALESCE(restate.level08_bar, dc.level08_bar) AS level08_bar, COALESCE(restate.level09_bar, dc.level09_bar) AS level09_bar, COALESCE(restate.level10_bar, dc.level10_bar) AS level10_bar, COALESCE(restate.level11_bar, dc.level11_bar) AS level11_bar, dc.membertype, dc.generation, dc.ragged_level01_bar, dc.ragged_level02_bar, dc.ragged_level03_bar, dc.ragged_level04_bar, dc.ragged_level05_bar, dc.ragged_level06_bar, dc.ragged_level07_bar, dc.ragged_level08_bar, dc.ragged_level09_bar, dc.ragged_level10_bar, dc.ragged_level11_bar, COALESCE(restate.demand_group, dc.demand_group) AS demand_group, COALESCE(restate.a2, dc.a2) AS a2, COALESCE(restate.a1, dc.a1) AS a1, COALESCE(restate.a2_desc, dc.a2_desc) AS a2_desc, COALESCE(restate.a1_desc, dc.a1_desc) AS a1_desc, dc.start_date, dc.end_date, dc.audit_loadts, COALESCE(restate.level01_commercial, dc.level01_commercial) AS level01_commercial, COALESCE(restate.level02_commercial, dc.level02_commercial) AS level02_commercial, COALESCE(restate.level03_commercial, dc.level03_commercial) AS level03_commercial, COALESCE(restate.level04_commercial, dc.level04_commercial) AS level04_commercial, COALESCE(restate.level05_commercial, dc.level05_commercial) AS level05_commercial, COALESCE(restate.level06_commercial, dc.level06_commercial) AS level06_commercial, dc.hierarchy_b_id, dc.hierarchy_b_desc, COALESCE(restate.hierarchy_c_id, dc.hierarchy_c_id) AS hierarchy_c_id, COALESCE(restate.hierarchy_c_desc, dc.hierarchy_c_desc) AS hierarchy_c_desc, CASE WHEN (restate.soldto_number IS NULL) THEN 'N'::text ELSE 'Y'::text END AS flg_restated FROM (dw.dim_customer dc LEFT JOIN dw.dim_customer_restatement restate ON (((lower((dc.soldto_number)::text) = lower((restate.soldto_number)::text)) AND (lower((dc.base_customer)::text) = lower((restate.base_customer)::text)))));