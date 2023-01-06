with

stations as (
    select * from {{ ref('seed_stations') }}
),

final as (
    select 
        code as station_code,
        name as station_name,
        latitude as station_latitude,
        longitude as station_longitude   
    from stations
)

select * from final