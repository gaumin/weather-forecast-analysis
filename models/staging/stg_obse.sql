with observation_model as (
    select
        datetime(observationTimeUtc) as observationTimeUtc,
        date(observationTimeUtc) as observartionDate,
        time(datetime(observationTimeUtc)) as observartionTime,
        obse.station as station,
        code as placeCode,
        airTemperature as airTemp,
        date
    from {{ source('src-observation', 'observation') }} as obse
    LEFT JOIN {{ ref('dim_places_stations')}} as place
    ON obse.station = place.station
),
final as (
    select
        *      
    from observation_model
)
select * from final