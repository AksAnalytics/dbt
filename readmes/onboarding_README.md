### The Onboarding Folder
This folder only contains "crt_" prefixed models which are used for creating tables in Snowflake. We use the crt models to dump all the tables that we got from the Redshift extract. The purpose of crt_ models is to simply create the tables in the warehouse so that they exist when converting the Redshift stored procedures.

*Note: Any table that already exists won't be create and any table that is created by a dbt model will be overwritten.*

There are two methods for creating crt_ models: 
1. Use the crt_template.txt in the **utils** folder to manually create the model *(Instructions included in the file)*
2. Run the data-mart-dumper.py script in the **utils** folder to generate all the files *(Instructions included in the file)*