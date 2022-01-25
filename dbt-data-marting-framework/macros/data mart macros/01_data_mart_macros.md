{% docs create_data_mart_table %}

When this macro is called it creates a table directly inside of Snowflake if the table doesn't already exist. It creates the table based on the metadata that is passed as an argument into the table_metadata parameter. The macro is used inside of every "crt_" prefixed model found in the models/onboarding/* folders.

Because the macro uses the IF NOT EXISTS flag, if you change the value of the "table_definition" key inside of the table_metadata argument it won't be reflected in Snowflake. To make changes to the table that you already created you need to add values to the "full_refresh_ddl_statements" key and then run the model with a full refresh. 

*Recommendation: Any changes you make to the table structure via the the "full_refresh_ddl_statement" key, such as ADD COLUMN, you should those changes inside the "table_definition" key. Remember, because of the IF NOT EXISTS flag the things in the table_definition will be ignored.  

[run_alter_statement_commands](#!/macro/macro.snowflake_training.run_alter_statement_commands)

{% enddocs %}


{% docs run_alter_statement_commands %}

:param alter_statement_array A comma seperated array of statements that will be executed directly against the warehouse. 

{% enddocs %}
