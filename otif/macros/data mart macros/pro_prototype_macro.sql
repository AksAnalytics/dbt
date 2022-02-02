{% macro create_data_mart_procedure_new(action) %}
-- There are two things that need to be done in dbt
-- 1) The stored procedure needs to be created or altered
-- 2) The stored procedure needs to be executed

-- Because of 2, there needs to be some type of model that can be run, being that you can't run a macro.
-- So the question then is, how should you run a stored procedure? Do you put all the models in a folder and run the folder, do you add tags and run the tag, do you pass a variable at the command line, etc.
-- CAN YOU EVEN PASS A VARIABLE USING dbt cloud???

-- I don't think a ref() works in a stored procedure (in terms of documentation). 1) dbt doesn't put stored procedures in documentation and 2) like creating tables I am going to be executing the stored procedure against the warehosue directly. For example, with the tables, without the light transformation/staging model portion of the crt_models there wouldn't be anything for dbt to do except execute the statement against the warehouse. WHAT HAPPENS IF I REMOVE THE STAGING PORTION????
    --> In a model you NEED to put something because dbt is going to try a CREATE OR REPLACE TABLE {name} AS (). If () is empty that is an error.
    --> Maybe you can use a SELECT 1 with a ephemeral materialization so that the pre-hook executes what it needs to execute and the SELECT 1 doesn't show up in the database?
        --> You can't use SELECT 1 because you get a 'Missing column specification' error
        --> Ephemeral models don't get executed against the database

-- A MODEL NEEDS TO SELECT SOMETHING
    --> Maybe use a custom materialization

-- LOOK AT RUN_OPEARTION

    {% set procedure_metadata = {
        "procedure_name": "insert_procedure_prototype",
        "procedure_arguments" : "()",
        "prcedure_body": "
            returns string not null
            language javascript
            as 
            $$
            var cmd = `INSERT INTO otif_prototype values ('denzel is amazing', 1)`
            var sql = snowflake.createStatement({sqlText: cmd});
            var result = sql.execute();
            return 'yes'
            $$;
        ",
        "alter_procedure_ddl_statements": [
            "ALTER PROCEDURE insert_procedure_prototype() SET COMMENT = 'testing comment'"
        ]
    }%}

    {% if action|upper == 'CREATE' %}
        {% set procedure_ddl_statement %}
            CREATE OR REPLACE PROCEDURE {{ procedure_metadata.procedure_name }} {{procedure_metadata.procedure_arguments}} {{ procedure_metadata.prcedure_body }}
        {% endset %}
        {% do run_query(procedure_ddl_statement) %}

    {% elif action|upper == 'ALTER' %}
        {{ run_alter_statement_commands(procedure_metadata.alter_procedure_ddl_statements) }}

    {% elif action|upper == 'CALL' %}
        {% set procedure_ddl_statement %} CALL {{ procedure_metadata.procedure_name }} {{ procedure_metadata.procedure_arguments }} {% endset %}
        {% do run_query(procedure_ddl_statement) %}
    {% endif %}

{%- endmacro -%}

