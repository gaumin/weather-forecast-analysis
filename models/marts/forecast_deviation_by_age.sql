with

forecast as (
    select * from {{ ref('forecast_with_observation') }}
),


final as (
    select
        place_code,
        forecast_age,
        round(avg(air_temp_delta), 2) as temp_mean_deviation,
        max(air_temp_delta) as temp_mean_max,
        min(air_temp_delta) as temp_mean_min,
        stddev(air_temp_delta) as temp_stddev
    from forecast
    group by place_code, forecast_age
    order by forecast_age asc
)

select * from final