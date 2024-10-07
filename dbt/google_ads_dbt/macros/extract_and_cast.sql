{% macro extract_and_cast(json_field, json_path, data_type) %}
    CAST(JSON_EXTRACT_SCALAR({{ json_field }}, '{{ json_path }}') AS {{ data_type }})
{% endmacro %}
