with

forecast as (
    select * from {{ ref('forecast_with_observation') }}
),


final as (
    select
        fo_place_code,
        fo_age,
        round(avg(air_temp_delta), 2) as air_temp_dev,    
        round(avg(wind_speed_delta), 1) as wind_speed_dev,
        round(avg(wind_dir_delta), 1) as wind_dir_dev,
        round(avg(cloud_delta), 1) as cloud_dev,
        round(avg(pressure_delta), 1) as pressure_dev,
        round(avg(humidity_delta), 1) as humidity_dev,
        round(avg(precipit_delta), 1) as precipit_dev
    from forecast
    group by fo_place_code, fo_age
    order by fo_age asc
)

select * from final