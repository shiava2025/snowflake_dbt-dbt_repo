{% macro detect_metric_anomalies(
    model_relation,
    metric_column,
    group_columns=[],
    lookback_days=28,
    z_threshold=3.0
) %}

  {#
    Detects anomalies using:
    - Rolling averages and standard deviations
    - Configurable Z-score thresholds
    - Optional grouping (e.g., by team or repo)
    
    Args:
      model_relation: Relation to analyze
      metric_column: Column with metric values
      group_columns: List of columns to group by
      lookback_days: Days to consider for baseline
      z_threshold: Z-score cutoff for anomalies
  #}

  with baseline_stats as (
    select
      {% for col in group_columns %}
        {{ col }},
      {% endfor %}
      avg({{ metric_column }}) as mean_val,
      stddev({{ metric_column }}) as stddev_val,
      min({{ metric_column }}) as min_val,
      max({{ metric_column }}) as max_val
    from {{ model_relation }}
    where date(metric_date) >= date_sub(current_date(), interval {{ lookback_days }} day)
    {% if group_columns %}
      group by {% for col in group_columns %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
    {% endif %}
  ),

  current_values as (
    select
      {% for col in group_columns %}
        m.{{ col }},
      {% endfor %}
      m.metric_date,
      m.{{ metric_column }},
      b.mean_val,
      b.stddev_val,
      (m.{{ metric_column }} - b.mean_val) / nullif(b.stddev_val, 0) as z_score
    from {{ model_relation }} m
    join baseline_stats b on
      {% for col in group_columns %}
        m.{{ col }} = b.{{ col }} and
      {% endfor %}
      true
    where date(m.metric_date) = current_date()
  )

  select
    *,
    case
      when abs(z_score) > {{ z_threshold }} then true
      else false
    end as is_anomaly,
    '{{ metric_column }}' as metric_name
  from current_values
  where abs(z_score) > {{ z_threshold }}

{% endmacro %}