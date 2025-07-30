{% macro generate_dynamic_where(column_names, filter_values) %}
    {#
        Dynamically generates WHERE conditions based on input arrays
        Handles NULL values and proper quoting
    #}
    {% set where_conditions = [] %}
    
    {% for column in column_names %}
        {% set filter_value = filter_values[loop.index0] %}
        {% if filter_value is not none %}
            {% if filter_value is string %}
                {% set condition = column ~ " = '" ~ filter_value ~ "'" %}
            {% else %}
                {% set condition = column ~ " = " ~ filter_value %}
            {% endif %}
            {% do where_conditions.append(condition) %}
        {% endif %}
    {% endfor %}
    
    {% if where_conditions %}
        WHERE {{ where_conditions|join(' AND ') }}
    {% endif %}
{% endmacro %}

{% macro currency_conversion(amount_column, from_currency, to_currency='USD') %}
    {#
        Dynamic currency conversion based on known exchange rates
        Handles multiple currencies with fallback to USD
    #}
    CASE 
        WHEN {{ from_currency }} = '{{ to_currency }}' THEN {{ amount_column }}
        WHEN {{ from_currency }} = 'GBP' AND '{{ to_currency }}' = 'USD' THEN {{ amount_column }} * 1.22
        WHEN {{ from_currency }} = 'EUR' AND '{{ to_currency }}' = 'USD' THEN {{ amount_column }} * 1.08
        WHEN {{ from_currency }} = 'CAD' AND '{{ to_currency }}' = 'USD' THEN {{ amount_column }} * 0.75
        WHEN {{ from_currency }} = 'AUD' AND '{{ to_currency }}' = 'USD' THEN {{ amount_column }} * 0.68
        ELSE {{ amount_column }}  -- Fallback to original amount if conversion not known
    END AS amount_{{ to_currency }}
{% endmacro %}