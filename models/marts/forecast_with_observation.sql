with

forecast as (
    select * from {{ ref('stg_forecast') }}
),

observation as (
    select * from {{ ref('stg_observation') }}
),

places as (
    select * from {{ ref('str_seed_places_with_station') }}
),

-- transform
observation_place_joined as (
    select *
    from observation as ob
    join places as pl
        on ob.station_code = pl.station_code
),

final as (
    select 
        forecast_date,
        f.place_code,
        f.air_temp as f_air_temp,
        o.air_temp as o_air_temp,
        forecast_age,
        forecast_creation_datetime_utc,
        f.loaded_date
    from forecast as f
    left join observation_place_joined as o
        on f.forecast_datetime = o.observation_datetime_utc
    where o.air_temp is not null
)

-- final
select * from final
