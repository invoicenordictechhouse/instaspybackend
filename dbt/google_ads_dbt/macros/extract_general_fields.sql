{% macro extract_general_fields() %}
  {% set fields = {
    'advertiser_id': 'STRING',
    'creative_id': 'STRING',
    'creative_page_url': 'STRING',
    'ad_format_type': 'STRING',
    'advertiser_disclosed_name': 'STRING',
    'advertiser_legal_name': 'STRING',
    'advertiser_location': 'STRING',
    'advertiser_verification_status': 'STRING',
    'topic': 'STRING',
    'is_funded_by_google_ad_grants': 'BOOL'
  } %}

  {% for field, field_type in fields.items() %}
    {{ extract_and_cast('raw_data', '$.' ~ field, field_type) }} AS {{ field }}{% if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %}
