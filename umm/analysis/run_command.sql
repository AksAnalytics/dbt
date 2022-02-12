-- Scenario - 5 : Processing of Deltas (Insert & Updates)
-- to run the first time as full load
-- dbt run --vars '{"process_dt": "2021-10-02", "stage_name": "landing_stage", "source_name": "customers"}'  --model tag:"customers" --full-refresh

-- to run the second time as incremental
-- dbt run --vars '{"process_dt": "2021-10-03", "stage_name": "landing_stage", "source_name": "customers"}'  --model tag:"customers"

-- Scenario - 2 : Processing of Deltas (Insert & Updates) & Delete file
-- to run the first time as full load
-- dbt run --vars '{"process_dt": "2021-10-02", "stage_name": "landing_stage", "source_name": "products"}'  --model tag:"products" --full-refresh

-- to run the second time as incremental
-- dbt run --vars '{"process_dt": "2021-10-03", "stage_name": "landing_stage", "source_name": "products"}'  --model tag:"products"

-- to generate documentation
-- dbt docs generate --var '{"stage_name":"", "file_name":"", "process_dt":""}'

-- ##create an environment var - pointing to your SBD rehydrate profile for DEV
-- export DEV_REHYDRATE= <user_dev_prefered_location>/.dbt_sbd_rehydrate

-- ##create an environment var - pointing to your SBD rehydrate profile for TEST
-- export TEST_REHYDRATE= <user_test_prefered_location>/.dbt_sbd_rehydrate

-- ##create an environment var - pointing to your SBD rehydrate profile for PROD
-- export PROD_REHYDRATE= <user_prod_prefered_location>/.dbt_sbd_rehydrate

-- ## test connection to Snowflake SBD with the rehydrate profile
-- dbt debug --profiles-dir=$DEV_REHYDRATE

-- ### Seed project/repo/ files to snowflake with full refresh
-- dbt seed --full-refresh --profiles-dir=$DEV_REHYDRATE

-- ### Run tests
-- dbt test --profiles-dir=$DEV_REHYDRATE

-- ## Get the project dependencies
-- dbt deps --profiles-dir=$DEV_REHYDRATE

-- ## Generate document
-- dbt docs generate --profiles-dir=$DEV_REHYDRATE

-- ## Serve the dbt document
-- dbt docs serve --profiles-dir=$DEV_REHYDRATE

-- ### Compile
-- dbt compile --profiles-dir=$DEV_REHYDRATE

-- ####to generate documentation
-- dbt docs generate --var '{"stage_name":"", "file_name":"", "process_dt":""}' --profiles-dir=$DEV_REHYDRATE

-- #### To check source freshness for older version of DBT - enabled for landing table
-- dbt source snapshot-freshness --profiles-dir=$SBD_PROFILE

-- #### To check source freshness for newer version of DBT >0.21.0
-- dbt source freshness --profiles-dir=$SBD_PROFILE

-- ### commands to run the rehydrate data ingestion framework with dbt
-- ####Scenario - 5 : Processing of Deltas (Insert & Updates)
-- ####to run the first time as full load
-- dbt run --vars '{"process_dt": "2021-10-02", "stage_name": "landing_stage", "source_name": "customers"}'  --model tag:"customers" --full-refresh  --profiles-dir=$DEV_REHYDRATE

-- ####to run the second time as incremental
-- dbt run --vars '{"process_dt": "2021-10-03", "stage_name": "landing_stage", "source_name": "customers"}'  --model tag:"customers" --profiles-dir=$DEV_REHYDRATE

-- ####Scenario - 2 : Processing of Deltas (Insert & Updates) & Delete file
-- ####to run the first time as full load
-- dbt run --vars '{"process_dt": "2021-10-02", "stage_name": "landing_stage", "source_name": "products"}'  --model tag:"products" --full-refresh --profiles-dir=$DEV_REHYDRATE

-- ####to run the second time as incremental
-- dbt run --vars '{"process_dt": "2021-10-03", "stage_name": "landing_stage", "source_name": "products"}'  --model tag:"products" --profiles-dir=$DEV_REHYDRATE

-- dbt run -m tag:zone0 --vars '{"skip_populate_into_z0": true, "load_latest_full_load": "true"}'

-- dbt run -m tag:zone0 --vars '{"skip_copy_into_z0": true, "load_latest_full_load": "true"}' --full-refresh

-- dbt run -m tag:quarantine --vars '{"skip_dataset_validation_checks": "data type check"}' --full-refresh

-- dbt run -m tag:validation --full-refresh

-- dbt run -m tag:zone1 tag:zone2 tag:zone3 --full-refresh
