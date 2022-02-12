
CREATE OR REPLACE PROCEDURE ref_data.p_build_ref_data_pnl_acct()
 LANGUAGE plpgsql
AS $$
BEGIN 
	
delete from ref_data.pnl_acct;

INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A40110','sales_invoiced','sales'),
	 ('A40111','fob_invoice_sale','sales'),
	 ('A40115','rsa_and_price_adjustments', 'cost'),
	 ('A40116','sales_freight_income','sales'),
	 ('A40120','destroy_in_field','sales');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A40210','product_sales_export','sales'),
	 ('A40310','rental_sales','sales'),
	 ('A40410','billable_service_revenue', ''),
	 ('A40510','contract_service_revenue',''),
	 ('A40610','install_revenue','sales');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A40710','franchise_revenue', 'sales'),
	 ('A40910','royalty_revenue','sales'),
	 ('A41110','returns_domestic','cost'),
	 ('A41210','returns_export','cost');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A42110','freight_domestic',''),
	 ('A42210','freight_export',''),
	 ('A43110','discounts_allow_domestic','cost'),
	 ('A43111','fillrate_fine','cost'),
	 ('A43112','rebates', 'cost');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A43115','cashdiscount_domestic', 'cost'),
	 ('A43116','coop_advertising', 'cost'),
	 ('A43117','sales_adjustments_other',''),
	 ('A43120','customer_considerations', 'cost'),
	 ('A43130','fob_deductions',''),
	 ('A43210','discounts_allow_export', 'cost'),
	 ('A43215','cash_discount_total', 'cost');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A60110','standard_material_cost_domestic', 'cost'),
	 ('A60111','standard_material_cost_fob', 'cost'),
	 ('A60112','standard_material_cost_serv_install',''),
	 ('A60113','standard_material_cost_serv_install_3p',''),
	 ('A60114','merchandising_cos', 'cost');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A60115','targeted_funds_cos',''),
	 ('A60116','free_goods_cos', 'cost'),
	 ('A60210','standard_material_cost_export', 'cost'),
	 ('A60310','rental_cos',''),
	 ('A60410','cos_service', 'A60410'),
	 ('A60510','cos_monitoring','');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A60610','cos_installations', 'cost'),
	 ('A60612','std_labor_cos_serv_install',''),
	 ('A60710','cos_freight_outbound', 'cost'),
	 ('A61110','cos_trd_domestic_labor', 'cost'),
	 ('A61210','cos_trd_export_labor','');
INSERT INTO ref_data.pnl_acct (bar_acct,bar_acct_desc,acct_type) VALUES
	 ('A62210','std_oh_cos_export',''),
	 ('A62612','std_oh_cos_serv_install', 'cost'),
	 ('A62613','std_oh_cos_serv_install_3p','');
end
$$
;