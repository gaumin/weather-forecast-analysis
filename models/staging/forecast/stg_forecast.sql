
with 

forecast as (
    select * from {{ source('src-forecast', 'forecast_data') }}
),

forecast_with_duplicates_count as (
    select 
        *, 
        ROW_NUMBER() 
            OVER (PARTITION BY code, forecastCreationTimeUtc, forecastTimeUtc 
            ORDER BY code, forecastCreationTimeUtc, forecastTimeUtc DESC) as row_no
    FROM forecast
),


forecast_fixed_utc_time as (
    select
        TIMESTAMP_ADD(datetime(forecastTimeUtc), INTERVAL {{ var('utc_offset_hours') }} HOUR) as forecast_datetime,
        TIMESTAMP_ADD(datetime(forecastCreationTimeUtc), INTERVAL {{ var('utc_offset_hours') }} HOUR) as forecast_creation_datetime,
        code,
        airTemperature,
        windSpeed,
        windDirection,
        cloudCover,
        seaLevelPressure,
        relativeHumidity,
        totalPrecipitation,
        date
    from forecast_with_duplicates_count
    where row_no = 1   -- drop dublicates
),

final as (
    select
        forecast_datetime as fo_datetime,
        date(forecast_datetime) as fo_date,
        time(forecast_datetime) as fo_time,
        code as fo_place_code,
        airTemperature as fo_air_temp,
        windSpeed as fo_wind_speed,
        windDirection as fo_wind_dir,
        cloudCover as fo_cloud,
        seaLevelPressure as fo_pressure,
        relativeHumidity as fo_humidity,
        totalPrecipitation as fo_precipit,
        forecast_creation_datetime as fo_creation_datetime,
        date(forecast_creation_datetime) as fo_creation_date,
        time(forecast_creation_datetime) as fo_creation_time,
        date as fo_loaded_date,
        ({{ count_days_diff('datetime(forecast_creation_datetime)', 'datetime(forecast_datetime)' ) }}) as fo_age  
    from forecast_fixed_utc_time
)

select * from final