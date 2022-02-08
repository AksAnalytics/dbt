{% macro source_view_rank_column() %}
{% if target.name == 'dev' %}
where {{column_name}} >= dateadd('day', -{{dev_days_of_data}}, current_timestamp)
{% endif %}
{% endmacro %}