{% macro extract_audience_fields() %}
  {% set audience_fields = [
      'demographic_info', 
      'geo_location', 
      'contextual_signals', 
      'customer_lists', 
      'topics_of_interest'
  ] %}
  
  {% for field in audience_fields %}
    {{ extract_audience_field(field) }} AS {{ field }}{% if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %}
