with

lastest_forecast as (
    select * from {{ ref('stg_forecast') }}
),

latest_creation_date as (
    select max(fo_creation_datetime) as latest
    from lastest_forecast
),

final as (
    select 
        fo_datetime,
        fo_date,
        fo_time,
        fo_place_code,
        fo_air_temp,
        fo_wind_speed,
        fo_wind_dir,
        fo_cloud,
        fo_pressure,
        fo_humidity,
        fo_precipit,
        fo_creation_datetime,		
        fo_creation_date,		
        fo_creation_time,
        fo_loaded_date
    from lastest_forecast
    where fo_creation_datetime = (select latest from latest_creation_date)
)

select * from final
