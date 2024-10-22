{% macro aggregate_surface(surface_name, lower_bound, upper_bound) %}
    MAX(CASE WHEN surface = '{{ surface_name }}' THEN IFNULL({{ lower_bound }}, 0) END) AS {{ surface_name | lower }}_times_shown_lower_bound,
    MAX(CASE WHEN surface = '{{ surface_name }}' THEN IFNULL({{ upper_bound }}, 0) END) AS {{ surface_name | lower }}_times_shown_upper_bound
{% endmacro %}
