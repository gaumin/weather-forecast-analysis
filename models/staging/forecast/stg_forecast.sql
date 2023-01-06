
with 

forecast as (
    select * from {{ source('src-forecast', 'forecast-long') }}
),

final as (
    select
        datetime(forecastTimeUtc) as forecast_datetime_utc,
        TIMESTAMP_ADD(datetime(forecastTimeUtc), INTERVAL {{ var('utc_offset_hours') }} HOUR) as forecast_datetime,
        date(forecastTimeUtc) as forecast_date,
        time(datetime(forecastTimeUtc)) as forecast_time,
        code as place_code,
        airTemperature as air_temp,
        datetime(forecastCreationTimeUtc) as forecast_creation_datetime_utc,
        date(forecastCreationTimeUtc) as forecast_creation_date_utc,
        time(datetime(forecastCreationTimeUtc)) as forecast_creation_time_utc,
        date as loaded_date,
        ({{ count_days_diff('datetime(forecastCreationTimeUtc)', 'datetime(forecastTimeUtc)' ) }}) as forecast_age  
    from forecast
)

select * from final