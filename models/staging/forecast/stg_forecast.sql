
with 

forecast as (
    select * from {{ source('src-forecast', 'forecast_data') }}
),


forecast_fixed_utc_time as (
    select
        TIMESTAMP_ADD(datetime(forecastTimeUtc), INTERVAL {{ var('utc_offset_hours') }} HOUR) as forecast_datetime,
        TIMESTAMP_ADD(datetime(forecastCreationTimeUtc), INTERVAL {{ var('utc_offset_hours') }} HOUR) as forecast_creation_datetime,
        code,
        airTemperature,
        date
    from forecast
),


final as (
    select
        forecast_datetime,
        date(forecast_datetime) as forecast_date,
        time(forecast_datetime) as forecast_time,
        code as place_code,
        airTemperature as air_temp,
        forecast_creation_datetime,
        date(forecast_creation_datetime) as forecast_creation_date,
        time(forecast_creation_datetime) as forecast_creation_time,
        date as loaded_date,
        ({{ count_days_diff('datetime(forecast_creation_datetime)', 'datetime(forecast_datetime)' ) }}) as forecast_age  
    from forecast_fixed_utc_time
)

select * from final