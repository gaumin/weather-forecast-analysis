with 

-- import
observations as (
    select * from {{ source('src-observation', 'observation_data') }} 
),

places as (
     select * from {{ ref('seed_places_stations')}}
),

observation_fixed_utc_time as (
    select
        TIMESTAMP_ADD(datetime(observationTimeUtc), INTERVAL {{ var('utc_offset_hours') }} HOUR) as observation_datetime,
        airTemperature,
        windSpeed,
        windDirection,
        cloudCover,
        cast(seaLevelPressure as int64) as seaLevelPressure,
        relativeHumidity,
        precipitation,
        station,
        date
    from observations
),

--transform
final as (
    select
        observation_datetime as ob_datetime,
        date(observation_datetime) as ob_date,
        time(observation_datetime) as ob_time,
        airTemperature as ob_air_temp,
        windSpeed as ob_wind_speed,
        windDirection as ob_wind_dir,
        cloudCover as ob_cloud,
        seaLevelPressure as ob_pressure,
        relativeHumidity as ob_humidity,
        Precipitation as ob_precipit,
        station as ob_station_code,
        date as ob_loaded_date
    from observation_fixed_utc_time
)

select * from final