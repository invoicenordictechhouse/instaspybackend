{% macro clean_json_string(json_field) %}
    REGEXP_REPLACE(
        REGEXP_REPLACE(JSON_EXTRACT({{ json_field }}, '$.region_stats'), '^"+|"+$', ''),
        '\\\\',
        ''
    )
{% endmacro %}
