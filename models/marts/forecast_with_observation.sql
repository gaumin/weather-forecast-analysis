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
        on ob.ob_station_code = pl.station_code
),

final as (
    select 
        fo_datetime,
        fo_place_code,
        fo_air_temp,
        ob_air_temp,
        round(ABS(fo_air_temp - ob_air_temp), 1) as air_temp_delta,
        fo_wind_speed,
        ob_wind_speed,
        round(ABS(fo_wind_speed - ob_wind_speed), 1) as wind_speed_delta,
        fo_wind_dir,
        ob_wind_dir,
        round(ABS(fo_wind_dir - ob_wind_dir), 1) as wind_dir_delta,
        fo_cloud,
        ob_cloud,
        round(ABS(fo_cloud - ob_cloud), 1) as cloud_delta,
        fo_pressure,
        ob_pressure,
        round(ABS(fo_pressure - ob_pressure), 1) as pressure_delta,
        fo_humidity,
        ob_humidity,
        round(ABS(fo_humidity - ob_humidity), 1) as humidity_delta,
        fo_precipit,
        ob_precipit,
        round(ABS(fo_precipit - ob_precipit), 1) as precipit_delta,
        fo_age,
        fo_creation_datetime,
        fo_loaded_date,
        ob_datetime
    from forecast as f
    left join observation_place_joined as o
        on f.fo_datetime = o.ob_datetime
    where o.ob_air_temp is not null
)

-- final
select * from final
