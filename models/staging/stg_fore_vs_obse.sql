
{{ config(materialized='table') }}


with merged_forecast as (
    SELECT * 
    FROM {{ ref("stg_fore")}} as f
    LEFT JOIN  {{ ref('stg_obse')}} as o
    ON  f.forecastDateTimeUtc = o.observationTimeUtc 
    AND f.code = o.placeCode
    where airTemp is not null
)

SELect 
    forecastDate,
    forecastTime,
    forecastCreationTimeUtc,
    airTemperature,
    airTemp as airTempReal,
    placeCode, 
    station,
    daysDiff,
    observartionDate,
    observartionTime
from merged_forecast
order by 
    forecastCreationTimeUtc,
    observartionDate
