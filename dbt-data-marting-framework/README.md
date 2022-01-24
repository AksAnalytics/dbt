 ### The DBT Project Structure 

```
├── dbt_project.yml
├── overview.md (links to the ERD)
└── models
    ├── onboarding
        ├── edw_stage
        ├── edw_consolidate
        └── otif
            ├── 00_otif_crt_tables.yml (Documentation for all the crt models with refs to Doc Blocks)
            ├── 01_otif_crt_tables.md  (A series of Doc Blocks for all the crt models)
            ├── crt_table_name_B.sql
            └── crt_table_name_A.sql
            
    ├── staging
        ├──  00_sources.yml
        ├──  01_sources.md
        ├──  shipment
        └──  orders
            ├──  stg_systemA_order_related_zone3_table.sql
            └──  stg_systemB_order_related_zone3_table.sql

    ├── intermediate 
        ├── 00_intermediate.yml
        ├── 01_intermediate.md
        ├── int_consolidate_orders_from_any_sources.sql
        ├── int_consolidate_shipment_from_any_sources.sql
        ├── int_aggregation.sql
        └── int_filtered_lookup_table.sql
        
    └── marts 
        ├── globalpl
        ├── umm
        └── otif                 
            ├── view_whatever_view.sql
```

#### The Onboarding Folder
This folder only contains "crt_" prefixed models which are used for creating tables that don't already exist in Snowflake. For a detailed explantion on crt_models see INSERT LINK. 

#### The Staging Folder
This folder contains "stg_" prefixed models which are staging models from tables that were created in the RAW Database, specifically Zone 3 "_current" suffixed tables. These staging models read directly from the source table and should only perform light transformations on the data. It is a dbt best practice not to reference the source table directly and instead using a staging model. The models in this folder are further subdivided into folders based on their business dimension. For example, all staging models that are related to orders go in the "orders" folder. 

#### The Intermediate Folder
This folder contains "int_" prefixed models which are models that sit between the raw data and the meaningful data. Any transformation that will be referenced and/or reused downstream by models in marts folder (see below) go in this intermediate folder. Unlike the other folders, the intermediate folder has no substructure and it simply acts as a storage unit for standardized logic that can be reused over and over.

#### The Marts Folder
This folder contains the final models that are meaninful to the business and the data consumers. They are the models that typically sit at the endpoints of different paths in the DAG, making references to models in any one of the previous folders. 


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
