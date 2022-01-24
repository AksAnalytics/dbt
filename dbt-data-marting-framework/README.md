 ### The DBT Project Structure 

├── dbt_project.yml
├── overview.md (links to the ERD)
└── models
    ├── onboarding -> Only used for the creating tables (crt models) and minor transformations (src+stg) in snowflake 
        ├── edw_stage
        ├── edw_consolidate
        ├── otif
            ├── 00_otif_crt_tables.yml (Documentation for all the crt models with refs to Doc Blocks)
            ├── 01_otif_crt_tables.md  (A series of Doc Blocks for all the crt models)
            ├── crt_table_name_B.sql
            ├── crt_table_name_A.sql
            
    
    ├── staging -> Tables that exist already from the RAW database and minor transformations are made
        ├──  00_sources.yml
        ├──  01_sources.md
        ├──  shipment
        ├──  orders
            ├──  stg_systemA_order_related_zone3_table.sql
            ├──  stg_systemB_order_related_zone3_table.sql

    ├── intermediate -> any transformation that will be referenced and/or reused downstream by models in marts folder (see below). Some of these intermediate models could exist simply for performance benefits. A storage unit for standardized logic.
        ├── 00_intermediate.yml
        ├── 01_intermediate.md
        ├── int_consolidate_orders_from_any_sources.sql
        ├── int_consolidate_shipment_from_any_sources.sql
        ├── int_aggregation.sql
        ├── int_filtered_lookup_table.sql
        


    └── marts -> The final models that actual mean something to the business 
        ├── globalpl
        ├── umm
        ├── otif                 
            ├── view_whatever_view.sql







Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
