{% macro calculate_age(date_column, date_part='year') -%}
    {# 
        Calculates age based on date of birth 
        Supports different date parts (year, month, day)
        Handles NULL values
    #}
    CASE 
        WHEN {{ date_column }} IS NULL THEN NULL
        ELSE DATEDIFF(
            '{{ date_part }}',
            {{ date_column }},
            CURRENT_DATE()
        )
    END
{%- endmacro %}

{% macro fiscal_quarter(date_column) -%}
    {#
        Custom fiscal quarter calculation (starting April 1)
        Returns fiscal quarter and year as string
    #}
    CASE 
        WHEN MONTH({{ date_column }}) BETWEEN 4 AND 6 THEN 'Q1-' || YEAR({{ date_column }})
        WHEN MONTH({{ date_column }}) BETWEEN 7 AND 9 THEN 'Q2-' || YEAR({{ date_column }})
        WHEN MONTH({{ date_column }}) BETWEEN 10 AND 12 THEN 'Q3-' || YEAR({{ date_column }})
        WHEN MONTH({{ date_column }}) BETWEEN 1 AND 3 THEN 'Q4-' || (YEAR({{ date_column }}) - 1)
        ELSE NULL
    END
{%- endmacro %}