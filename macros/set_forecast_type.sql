{% macro set_forecast_type(forecast_date, current_date) -%}

case 
    when {{ count_days_diff(forecast_date, current_date) }} < 2 then "Harmonie" 
    else "ECMWF"
End

{%- endmacro %}