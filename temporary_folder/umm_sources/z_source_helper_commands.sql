-- generate docs for "bods"
dbt run-operation generate_source --args '{"schema_name": "bods", "database_name": "dev_raw", "generate_columns": true, "include_descriptions": true}' --profiles-dir=$SBD_DEV

-- generate docs for "sapp10"
dbt run-operation generate_source --args '{"schema_name": "sapp10", "database_name": "dev_raw", "generate_columns": true, "include_descriptions": true}' --profiles-dir=$SBD_DEV

-- generate docs for "sapc11"
dbt run-operation generate_source --args '{"schema_name": "sapc11", "database_name": "dev_raw", "generate_columns": true, "include_descriptions": true}' --profiles-dir=$SBD_DEV

-- generate docs for "sftpgtsi"
dbt run-operation generate_source --args '{"schema_name": "sftpgtsi", "database_name": "dev_raw", "generate_columns": true, "include_descriptions": true}' --profiles-dir=$SBD_DEV

-- generate docs for "lawsonmac"
dbt run-operation generate_source --args '{"schema_name": "lawsonmac", "database_name": "dev_raw", "generate_columns": true, "include_descriptions": true}' --profiles-dir=$SBD_DEV

