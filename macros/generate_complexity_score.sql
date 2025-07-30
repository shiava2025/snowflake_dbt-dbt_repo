{% macro generate_complexity_score(file_path, changes, file_type='code') %}

  {# 
    Calculate weighted complexity score based on:
    - File type (tests get higher weight)
    - File location (core vs peripheral)
    - Change size (non-linear scaling)
  #}
  
  {% set base_score = changes|float %}
  
  {% if file_type == 'test' %}
    {% set base_score = base_score * 1.5 %}
  {% elif file_type == 'config' %}
    {% set base_score = base_score * 0.7 %}
  {% endif %}
  
  {# Core files (in src/main/) get 2x multiplier #}
  {% if 'src/main/' in file_path %}
    {% set base_score = base_score * 2 %}
  {% endif %}
  
  {# Apply logarithmic scaling to reduce impact of massive changes #}
  {% set complexity_score = log(base_score + 1, 2) %}
  
  {# Round to 2 decimal places for consistency #}
  {{ return(round(complexity_score, 2)) }}

{% endmacro %}