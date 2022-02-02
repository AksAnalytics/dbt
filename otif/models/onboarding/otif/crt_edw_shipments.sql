-- Copy the following text into a new crt_{table}.sql file
-- Fill in the appropriate data in the table_metadata

{% set table_metadata = {
    "table_definition": "
        CREATE TABLE IF NOT EXISTS otif.edw_shipments
        (
            source_schema VARCHAR(256)   
            ,order_number VARCHAR(256)   
            ,order_class VARCHAR(256)   
            ,order_status VARCHAR(256)   
            ,cancelled VARCHAR(256)   
            ,ship_complete VARCHAR(256)   
            ,order_window VARCHAR(256)   
            ,ctrl_user_id VARCHAR(256)   
            ,ctrl_dt VARCHAR(256)   
            ,delivery_appt_reqd VARCHAR(256)   
            ,special_label_flag VARCHAR(256)   
            ,single_pack_flag VARCHAR(256)   
            ,pack_slip_sort_by VARCHAR(256)   
            ,pack_slip_qty VARCHAR(256)   
            ,pack_slip_handling VARCHAR(256)   
            ,pallet_pack_slip VARCHAR(256)   
            ,repack_pack_slip VARCHAR(256)   
            ,bol_number VARCHAR(256)   
            ,scac VARCHAR(256)   
            ,carrier_class VARCHAR(256)   
            ,load_id VARCHAR(256)   
            ,cancel_type VARCHAR(256)   
            ,wave_dt VARCHAR(256)   
            ,wave_number VARCHAR(256)   
            ,depart_dt VARCHAR(256)   
            ,completion_dt VARCHAR(256)   
            ,resultant_sku VARCHAR(256)   
            ,expected_qty VARCHAR(256)   
            ,account_code VARCHAR(256)   
            ,early_delivery_dt VARCHAR(256)   
            ,exclusion_start_dt VARCHAR(256)   
            ,exclusion_end_dt VARCHAR(256)   
            ,contact_name VARCHAR(256)   
            ,contact_phone VARCHAR(256)   
            ,appt_notice_hours VARCHAR(256)   
            ,customer_number VARCHAR(256)   
            ,sold_to_name VARCHAR(256)   
            ,retail_store_number VARCHAR(256)   
            ,chain_number VARCHAR(256)   
            ,dept_number VARCHAR(256)   
            ,po_number VARCHAR(256)   
            ,cancel_by_dt VARCHAR(256)   
            ,freight_allowed_cd VARCHAR(256)   
            ,custom_pallet_number VARCHAR(256)   
            ,pack_slip_fax VARCHAR(256)   
            ,receiving_from_tm VARCHAR(256)   
            ,receiving_to_tm VARCHAR(256)   
            ,address1 VARCHAR(256)   
            ,address2 VARCHAR(256)   
            ,address3 VARCHAR(256)   
            ,address4 VARCHAR(256)   
            ,zip_code VARCHAR(256)   
            ,freight_cost_tot VARCHAR(256)   
            ,tariff_used VARCHAR(256)   
            ,tariff_category VARCHAR(256)   
            ,manifest_number VARCHAR(256)   
            ,shipping_zone_code VARCHAR(256)   
            ,order_entry_dt VARCHAR(256)   
            ,bundling_slot VARCHAR(256)   
            ,business_unit VARCHAR(256)   
            ,combination_group VARCHAR(256)   
            ,pallet_override VARCHAR(256)   
            ,like_product_flag VARCHAR(256)   
            ,full_pallet_flag VARCHAR(256)   
            ,appt_number VARCHAR(256)   
            ,appt_dt VARCHAR(256)   
            ,cust_order_type VARCHAR(256)   
            ,ship_or_cancel_flag VARCHAR(256)   
            ,sap_delivery VARCHAR(256)   
            ,sap_delivery_type VARCHAR(256)   
            ,sap_plant VARCHAR(256)   
            ,sap_route VARCHAR(256)   
            ,sap_createdondate VARCHAR(256)   
            ,sap_createdontime VARCHAR(256)   
            ,sap_plandate VARCHAR(256)   
            ,sap_pickingdate VARCHAR(256)   
            ,sap_loadingdate VARCHAR(256)   
            ,sap_deliverydate VARCHAR(256)   
            ,sap_transpdate VARCHAR(256)   
            ,sap_actgoodsissue VARCHAR(256)   
            ,sap_order VARCHAR(256)   
            ,line_number VARCHAR(256)   
            ,sku VARCHAR(256)   
            ,top_level_flag VARCHAR(256)   
            ,optimize_flag VARCHAR(256)   
            ,inventory_item VARCHAR(256)   
            ,order_type VARCHAR(256)   
            ,order_qty VARCHAR(256)   
            ,allocated_qty VARCHAR(256)   
            ,picked_qty VARCHAR(256)   
            ,od_ctrl_user_id VARCHAR(256)   
            ,od_ctrl_dt VARCHAR(256)   
            ,status_cd VARCHAR(256)   
            ,starting_date_code VARCHAR(256)   
            ,ending_date_code VARCHAR(256)   
            ,freight_group_cd VARCHAR(256)   
            ,customer_sku VARCHAR(256)   
            ,backorder_qty VARCHAR(256)   
            ,cancelled_qty VARCHAR(256)   
            ,customer_retail_price VARCHAR(256)   
            ,committed_qty VARCHAR(256)   
            ,unit_gsv VARCHAR(256)   
            ,committed_unit_gsv VARCHAR(256)   
            ,pi_code VARCHAR(256)   
            ,wo_use_qty VARCHAR(256)   
            ,pick_conv_as_nonconv VARCHAR(256)   
            ,wo_comp_units VARCHAR(256)   
            ,sales_order_flag VARCHAR(256)   
            ,description VARCHAR(256)   
            ,product_type_code VARCHAR(256)   
            ,pallet_config_id VARCHAR(256)   
            ,pallet_config_qty VARCHAR(256)   
            ,plan_uom VARCHAR(256)   
            ,customer_line_number VARCHAR(256)   
            ,sp_loc VARCHAR(256)   
            ,customer_material_desc VARCHAR(256)   
            ,inner_pack_qty VARCHAR(256)   
            ,outer_pack_qty VARCHAR(256)   
            ,customer_putaway_location VARCHAR(256)   
            ,chase_flag VARCHAR(256)   
            ,material_availability_date VARCHAR(256)   
            ,customers_customer_item_number VARCHAR(256)   
            ,unit_gsv_per VARCHAR(256)   
            ,unit_price_per VARCHAR(256)   
            ,component_line_number VARCHAR(256)   
            ,orig_committed_qty VARCHAR(256)   
            ,work_order_number VARCHAR(256)   
            ,mto_weight VARCHAR(256)   
            ,mto_cube VARCHAR(256)   
            ,orig_chase_flag VARCHAR(256)   
            ,multiply_divide_ind VARCHAR(256)   
            ,multiply_divide_qty VARCHAR(256)   
            ,facility_number VARCHAR(256)   
            ,po_type VARCHAR(256)   
            ,planned_dt VARCHAR(256)   
            ,staged_dt VARCHAR(256)   
            ,loaded_dt VARCHAR(256)   
            ,documented_dt VARCHAR(256)   
            ,ptp_released_dt VARCHAR(256)   
            ,ptp_first_picked_dt VARCHAR(256)   
            ,ptp_last_picked_dt VARCHAR(256)   
            ,ptp_first_palletize_dt VARCHAR(256)   
            ,ptp_last_palletize_dt VARCHAR(256)   
            ,ptp_first_staged_dt VARCHAR(256)   
            ,ptp_last_staged_dt VARCHAR(256)   
            ,ptp_first_loaded_dt VARCHAR(256)   
            ,ptp_last_loaded_dt VARCHAR(256)   
            ,plt_released_dt VARCHAR(256)   
            ,plt_first_picked_dt VARCHAR(256)   
            ,plt_last_picked_dt VARCHAR(256)   
            ,plt_first_palletize_dt VARCHAR(256)   
            ,plt_last_palletize_dt VARCHAR(256)   
            ,plt_first_staged_dt VARCHAR(256)   
            ,plt_last_staged_dt VARCHAR(256)   
            ,plt_first_loaded_dt VARCHAR(256)   
            ,plt_last_loaded_dt VARCHAR(256)   
            ,rpk_released_dt VARCHAR(256)   
            ,rpk_first_picked_dt VARCHAR(256)   
            ,rpk_last_picked_dt VARCHAR(256)   
            ,rpk_first_sealed_dt VARCHAR(256)   
            ,rpk_last_sealed_dt VARCHAR(256)   
            ,rpk_first_prim_comp_dt VARCHAR(256)   
            ,rpk_last_prim_comp_dt VARCHAR(256)   
            ,ptb_released_dt VARCHAR(256)   
            ,ptb_first_picked_dt VARCHAR(256)   
            ,ptb_last_picked_dt VARCHAR(256)   
            ,ptb_first_prim_comp_dt VARCHAR(256)   
            ,ptb_last_prim_comp_dt VARCHAR(256)   
            ,cnv_first_assign_dt VARCHAR(256)   
            ,cnv_last_assign_dt VARCHAR(256)   
            ,cnv_first_sec_comp_dt VARCHAR(256)   
            ,cnv_last_sec_comp_dt VARCHAR(256)   
            ,cnv_first_palletize_dt VARCHAR(256)   
            ,cnv_last_palletize_dt VARCHAR(256)   
            ,cnv_first_staged_dt VARCHAR(256)   
            ,cnv_last_staged_dt VARCHAR(256)   
            ,cnv_first_loaded_dt VARCHAR(256)   
            ,cnv_last_loaded_dt VARCHAR(256)   
            ,first_bundled_dt VARCHAR(256)   
            ,last_bundled_dt VARCHAR(256)   
            ,first_pallet_specials_in_dt VARCHAR(256)   
            ,last_pallet_specials_in_dt VARCHAR(256)   
            ,first_pallet_specials_out_dt VARCHAR(256)   
            ,last_pallet_specials_out_dt VARCHAR(256)   
            ,consign_to_id VARCHAR(256)   
            ,load_status VARCHAR(256)   
            ,live_load_flag VARCHAR(256)   
            ,trans_mode VARCHAR(256)   
            ,trailer_type VARCHAR(256)   
            ,lc_scac VARCHAR(256)   
            ,number_stops VARCHAR(256)   
            ,lc_depart_dt VARCHAR(256)   
            ,lc_ctrl_user_id VARCHAR(256)   
            ,lc_ctrl_dt VARCHAR(256)   
            ,lc_bol_number VARCHAR(256)   
            ,trailer_id VARCHAR(256)   
            ,pro_number VARCHAR(256)   
            ,seal_number VARCHAR(256)   
            ,insert_dt VARCHAR(256)   
            ,first_plan_dt VARCHAR(256)   
            ,last_plan_dt VARCHAR(256)   
            ,lc_loaded_dt VARCHAR(256)   
            ,sealed_dt VARCHAR(256)   
            ,lc_documented_dt VARCHAR(256)   
            ,lc_completion_dt VARCHAR(256)   
            ,originating_supplier VARCHAR(256)   
            ,co_load_id VARCHAR(256)   
            ,staging_location VARCHAR(256)   
            ,specials_location VARCHAR(256)   
            ,left_right VARCHAR(256)   
            ,first_wave_dt VARCHAR(256)   
            ,first_wave_number VARCHAR(256)   
            ,last_wave_dt VARCHAR(256)   
            ,last_wave_number VARCHAR(256)   
            ,transfer_load_flag VARCHAR(256)   
            ,transfer_scac VARCHAR(256)   
            ,transfer_in_flag VARCHAR(256)   
            ,trailer_checkout_flag VARCHAR(256)   
            ,reported VARCHAR(256)   
            ,switch_load_flag VARCHAR(256)   
            ,building_number VARCHAR(256)   
            ,cust_load_ref VARCHAR(256)   
            ,auto_planned_flag VARCHAR(256)   
            ,planned_pickup_dt VARCHAR(256)   
            ,original_pickup_dt VARCHAR(256)   
            ,pickup_reason_code VARCHAR(256)   
            ,lc_facility_number VARCHAR(256)   
            ,customer_code VARCHAR(256)   
            ,lc_address1 VARCHAR(256)   
            ,lc_address2 VARCHAR(256)   
            ,lc_address3 VARCHAR(256)   
            ,lc_address4 VARCHAR(256)   
            ,lc_zip_code VARCHAR(256)   
            ,comments VARCHAR(256)   
            ,dangerous_goods_placard VARCHAR(256)   
            ,planned_notif_dt VARCHAR(256)   
            ,documented_notif_dt VARCHAR(256)   
            ,staging_assign_user_id VARCHAR(256)   
            ,staging_assign_ctrl_dt VARCHAR(256)   
            ,pallet_count VARCHAR(256)   
            ,proforma_invoice_sent VARCHAR(256)   
            ,proforma_invoice_sent_dt VARCHAR(256)   
            ,load_signed VARCHAR(256)   
            ,load_signed_dt VARCHAR(256)   
            ,carrier_name VARCHAR(256)   
            ,etl_crte_user VARCHAR(256)   
            ,etl_crte_ts TIMESTAMP WITHOUT TIME ZONE   
        )
        CLUSTER BY (sku)
    "
}%}

{{ config(materialized = "ephermeral") }}
{% do run_query(table_metadata.table_definition) %}