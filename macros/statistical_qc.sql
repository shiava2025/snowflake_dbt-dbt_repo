{% macro statistical_qc() %}

 {% set get_checks_query %}
   SELECT * FROM {{ ref( 'stats_qc_checks').render() }}
   --WHERE ACTIVE_CHECK = 'Y'
 {% endset %}
 {% set checks = run_query(get_checks_query) %}
  --{# this if statement ensures that the macro and model are able to run correctly
    --even when there are no checks in stats_qc_checks or the target is ci, which runs
    --with an empty flag #}
 {% if target.name == 'ci' or not checks %}
     SELECT
       NULL AS control_type,
       NULL AS control_group,
       NULL AS table_name,
       NULL AS metric_column,
       NULL AS I_ID,
       NULL AS qc_group1,
       NULL AS qc_group2,
       NULL AS qc_group3,
       NULL AS metric_date,
       NULL as reporting_date,
       NULL AS current_value,
       NULL AS historical_value,
       NULL AS historical_avg,
       NULL AS historical_stddev,
       NULL AS pct_change,
       NULL AS z_score,
       NULL AS fail_above_threshold,
       NULL AS warn_above_threshold,
       NULL AS fail_below_threshold,
       NULL AS warn_below_threshold,
       NULL AS qc_status,
       NULL AS check_id,
       NULL AS processed_at
    where 1=0
{% else %}
 {% for check in checks %}
   {% if loop.first %}
   WITH
   {% else %}
   ,
   {% endif %}
   {{ check.T_CNTL_TYP }}_{{ check.T_CNTL_GRP }}_{{ check.I_ID }}_results AS (
     WITH latest_extract AS (
       SELECT MAX({{ check.T_DATE_FIELD }}) AS run_date
       FROM {{ check.T_DB }}.{{ check.T_SCHEMA }}.{{ check.T_TABLE_VIEW_NAME }} le
     ),
     time_periods AS (
       SELECT
         le.run_date AS d_qc, run_date, 
         DATEADD(DAY, -{{ check.I_SAMPLE_SIZE }}, le.run_date) AS d_sample_start,
         DATEADD(DAY, -1, le.run_date) AS d_sample_end
       FROM latest_extract le
     ),
     daily_aggregates AS (
       SELECT
         {% if check.T_GROUP_1 != 'N' %}{{ check.T_GROUP_1 }}{% if check.T_GROUP_2 != 'N' or check.T_GROUP_3 != 'N' %},{% endif %}{% endif %}
         {% if check.T_GROUP_2 != 'N' %}{{ check.T_GROUP_2 }}{% if check.T_GROUP_3 != 'N' %},{% endif %}{% endif %}
         {% if check.T_GROUP_3 != 'N' %}{{ check.T_GROUP_3 }}{% endif %}
         ,{% if check.T_METRIC_COLUMN == 'RECORD_COUNT' %}COUNT(*){% else %}SUM({{ check.T_METRIC_COLUMN }}){% endif %} AS daily_value
       FROM time_periods td
       JOIN {{ check.T_DB }}.{{ check.T_SCHEMA }}.{{ check.T_TABLE_VIEW_NAME }} dw
         ON dw.{{ check.T_DATE_FIELD }} BETWEEN td.d_sample_start AND td.d_sample_end
       WHERE {{ check.T_WHERE_CLAUSE }}
       GROUP BY {{ check.T_DATE_FIELD }}
         {% if check.T_GROUP_1 != 'N' %}, {{ check.T_GROUP_1 }}{% endif %}
         {% if check.T_GROUP_2 != 'N' %}, {{ check.T_GROUP_2 }}{% endif %}
         {% if check.T_GROUP_3 != 'N' %}, {{ check.T_GROUP_3 }}{% endif %}
     ),
     data_threshold AS (
       SELECT
         {% if check.T_GROUP_1 != 'N' %} CAST({{ check.T_GROUP_1 }} AS VARCHAR) AS qc_group{% else %} CAST(NULL AS VARCHAR) AS qc_group{% endif %}
         {% if check.T_GROUP_2 != 'N' %}, CAST({{ check.T_GROUP_2 }} AS VARCHAR) AS qc_group2{% else %}, CAST(NULL AS VARCHAR) AS qc_group2{% endif %}
         {% if check.T_GROUP_3 != 'N' %}, CAST({{ check.T_GROUP_3 }} AS VARCHAR) AS qc_group3{% else %}, CAST(NULL AS VARCHAR) AS qc_group3{% endif %}
         ,SUM(daily_value) AS a_amt_p
         ,AVG(daily_value) AS a_amt_avg
         ,STDDEV(daily_value) AS a_amt_sd
       FROM daily_aggregates
       GROUP BY
         {% if check.T_GROUP_1 != 'N' %}{{ check.T_GROUP_1 }}{% if check.T_GROUP_2 != 'N' or check.T_GROUP_3 != 'N' %},{% endif %}{% endif %}
         {% if check.T_GROUP_2 != 'N' %}{{ check.T_GROUP_2 }}{% if check.T_GROUP_3 != 'N' %},{% endif %}{% endif %}
         {% if check.T_GROUP_3 != 'N' %}{{ check.T_GROUP_3 }}{% endif %}
     ),
     data_current AS (
       SELECT
         run_date,
         {% if check.T_GROUP_1 != 'N' %} CAST({{ check.T_GROUP_1 }} AS VARCHAR) AS qc_group{% else %} CAST(NULL AS VARCHAR) AS qc_group{% endif %}
         {% if check.T_GROUP_2 != 'N' %}, CAST({{ check.T_GROUP_2 }} AS VARCHAR) AS qc_group2{% else %}, CAST(NULL AS VARCHAR) AS qc_group2{% endif %}
         {% if check.T_GROUP_3 != 'N' %}, CAST({{ check.T_GROUP_3 }} AS VARCHAR) AS qc_group3{% else %}, CAST(NULL AS VARCHAR) AS qc_group3{% endif %}
         ,{% if check.T_METRIC_COLUMN == 'RECORD_COUNT' %}COUNT(*){% else %}SUM({{ check.T_METRIC_COLUMN }}){% endif %} AS a_amt
       FROM time_periods td
       JOIN {{ check.T_DB }}.{{ check.T_SCHEMA }}.{{ check.T_TABLE_VIEW_NAME }} dw
         ON dw.{{ check.T_DATE_FIELD }} = td.d_qc
       WHERE {{ check.T_WHERE_CLAUSE }}
       GROUP BY
         run_date,
         {% if check.T_GROUP_1 != 'N' %}{{ check.T_GROUP_1 }}{% if check.T_GROUP_2 != 'N' or check.T_GROUP_3 != 'N' %},{% endif %}{% endif %}
         {% if check.T_GROUP_2 != 'N' %}{{ check.T_GROUP_2 }}{% if check.T_GROUP_3 != 'N' %},{% endif %}{% endif %}
         {% if check.T_GROUP_3 != 'N' %}{{ check.T_GROUP_3 }}{% endif %}
     )
     SELECT
       '{{ check.T_CNTL_TYP }}' AS control_type,
       '{{ check.T_CNTL_GRP }}' AS control_group,
       '{{ check.T_TABLE_VIEW_NAME }}' AS table_name,
       '{{ check.T_METRIC_COLUMN }}' AS metric_column,
       '{{ check.I_ID }}' AS I_ID,
       CAST(COALESCE(dt.qc_group, th.qc_group) AS VARCHAR) AS qc_group1,
       CAST(COALESCE(dt.qc_group2, th.qc_group2) AS VARCHAR) AS qc_group2,
       CAST(COALESCE(dt.qc_group3, th.qc_group3) AS VARCHAR) AS qc_group3,
       CURRENT_DATE AS metric_date,
       run_date as reporting_date,
       COALESCE(dt.a_amt, 0) AS current_value,
       COALESCE(th.a_amt_p, 0) AS historical_value,
       COALESCE(th.a_amt_avg, 0) AS historical_avg,
       COALESCE(th.a_amt_sd, 0) AS historical_stddev,
       CASE
         WHEN th.a_amt_p IS NULL OR th.a_amt_p = 0 THEN NULL
         ELSE CAST(dt.a_amt AS DOUBLE) / CAST(th.a_amt_p AS DOUBLE) - 1
       END AS pct_change,
       CASE
         WHEN dt.a_amt IS NULL THEN NULL
         WHEN th.a_amt_avg IS NULL THEN NULL
         WHEN th.a_amt_sd IS NULL THEN NULL
         WHEN th.a_amt_sd = 0 THEN 0
         ELSE (CAST(dt.a_amt AS DOUBLE) - CAST(th.a_amt_avg AS DOUBLE)) / CAST(th.a_amt_sd AS DOUBLE)
       END AS z_score,
       {{ check.I_FAIL_ABOVE_TH }} AS fail_above_threshold,
       {{ check.I_WARN_ABOVE_TH }} AS warn_above_threshold,
       {{ check.I_FAIL_BELOW_TH }} AS fail_below_threshold,
       {{ check.I_WARN_BELOW_TH }} AS warn_below_threshold,
     FROM data_current dt
     LEFT JOIN data_threshold th
       ON 
        COALESCE(dt.qc_group, '') = COALESCE(th.qc_group, '')
        AND COALESCE(dt.qc_group2, '') = COALESCE(th.qc_group2, '')
        AND COALESCE(dt.qc_group3, '') = COALESCE(th.qc_group3, '')
   )
 {% endfor %}
 SELECT
   r.*,
   CASE
     WHEN r.z_score IS NULL THEN 'NO_DATA'
     WHEN r.z_score > r.fail_above_threshold THEN 'FAIL'
     WHEN r.z_score < r.fail_below_threshold THEN 'FAIL'
     WHEN r.z_score > r.warn_above_threshold OR r.z_score < r.warn_below_threshold THEN 'WARN'
     WHEN (r.current_value > 1000 OR r.historical_value > 1000) AND
          (r.pct_change <= -3 OR r.pct_change >= 3) THEN 'LARGEVAR'
     ELSE 'PASS'
   END AS qc_status,
   ROW_NUMBER() OVER (ORDER BY I_ID)  as check_id,
   CURRENT_TIMESTAMP() AS processed_at
 FROM (
   {% for check in checks %}
     {% if not loop.first %}UNION ALL{% endif %}
     SELECT * FROM {{ check.T_CNTL_TYP }}_{{ check.T_CNTL_GRP }}_{{ check.I_ID }}_results
   {% endfor %}
 ) r
{% endif %}
{% endmacro %}