{% macro extract_audience_field(field_name) %}
  SAFE_CAST(
    JSON_VALUE(
      SAFE.PARSE_JSON(JSON_EXTRACT(raw_data, '$.audience_selection_approach_info')), 
      '$.' || '{{ field_name }}'  
    ) AS STRING
  )
{% endmacro %}
