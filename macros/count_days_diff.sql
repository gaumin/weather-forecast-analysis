{%- macro count_days_diff(forecast_creation_date, forecast_date) -%}

CAST(DATETIME_DIFF( {{ forecast_date }} , {{ forecast_creation_date }}, DAY) as int64)

{%- endmacro %}