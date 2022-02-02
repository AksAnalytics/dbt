{% macro create_data_mart_table(table_metadata) %}
    
    -- Handling Tranisent Table Configuration
    {% if table_metadata.transient_table|upper != 'TRUE' %}
      {% set transient_table_flag = false %} 
    {% else %}
        {% set transient_table_flag = true %}
    {% endif %}

    -- Create The Table in Snowflake
    {% if transient_table_flag -%}
        CREATE TRANSIENT TABLE IF NOT EXISTS {{ table_metadata.table_name }}
    {% else -%}
        CREATE TABLE IF NOT EXISTS {{ table_metadata.table_name }}
    {% endif -%}
    {{ table_metadata.table_definition }}
    
    -- This gets run first when the full refresh runs. Which means the table must already exist in the warehouse. This is fine because these ddl statements only should be used after the fact. 
    {% if flags.FULL_REFRESH %}
        {{ run_alter_statement_commands(table_metadata.full_refresh_ddl_statements) }}
    {% endif %}

{% endmacro %}


{% macro run_alter_statement_commands(alter_statement_array) %}
    {% for alter_statement in alter_statement_array %}
      {% do run_query(alter_statement) %}
    {% endfor %}
{% endmacro %}
