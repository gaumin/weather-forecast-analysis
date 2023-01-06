with 

-- import
observations as (
    select * from {{ source('src-observation', 'observation') }} 
),

places as (
     select * from {{ ref('seed_places_stations')}}
),

--transform
final as (
    select
        datetime(observationTimeUtc) as observation_datetime_utc,
        date(observationTimeUtc) as observartion_date_utc,
        time(datetime(observationTimeUtc)) as observartion_time,
        airTemperature as air_temp,
        station as station_code,
        date as loaded_at_date
    from observations
)
select * from final