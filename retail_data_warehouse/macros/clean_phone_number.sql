{% macro clean_phone_number(column_name) %}
    regexp_replace({{ column_name }}, '[^0-9]', '')
{% endmacro %}