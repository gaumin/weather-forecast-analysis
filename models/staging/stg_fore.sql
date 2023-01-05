with forecast_model as (
    select
        datetime(forecastTimeUtc) as forecastDateTimeUtc,
        date(forecastTimeUtc) as forecastDate,
        time(datetime(forecastTimeUtc)) as forecastTime,
        code,
        airTemperature,
        datetime(forecastCreationTimeUtc) as forecastCreationTimeUtc,
        date(forecastCreationTimeUtc) as forecastCreationDate,
        time(datetime(forecastCreationTimeUtc)) as forecastCreationTime
    from {{ source('src-forecast', 'forecast-long') }}
),
final as (
    select
        forecastDateTimeUtc,
        forecastDate,
        forecastTime,
        code,
        airTemperature, 
        forecastCreationTimeUtc,
        forecastCreationDate,
        forecastCreationTime,
        ( {{ count_days_diff('forecastCreationTimeUtc', 'forecastDateTimeUtc') }}) as daysDiff  
            
    from forecast_model
)
select * from final