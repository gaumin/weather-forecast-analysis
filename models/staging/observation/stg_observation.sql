with 

-- import
observations as (
    select * from {{ source('src-observation', 'observation') }} 
),

places as (
     select * from {{ ref('seed_places_stations')}}
),

observation_fixed_utc_time as (
    select
        TIMESTAMP_ADD(datetime(observationTimeUtc), INTERVAL {{ var('utc_offset_hours') }} HOUR) as observation_datetime,
        airTemperature,
        station,
        date
    from observations
),

--transform
final as (
    select
        observation_datetime,
        date(observation_datetime) as observartion_date,
        time(observation_datetime) as observartion_time,
        airTemperature as air_temp,
        station as station_code,
        date as loaded_at_date
    from observation_fixed_utc_time
)

select * from final