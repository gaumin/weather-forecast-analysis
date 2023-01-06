with

place_and_station as (
    select * from {{ ref('seed_places_stations') }}
),

final as (
    select 
        code as place_code,
        station as station_code, 
    from place_and_station
)

select * from final