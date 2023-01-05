with forecast_table as (
    Select 
        distinct(forecastCreationTimeUtc) as RecordsCount
        from {{ source('src-forecast', 'forecast-long') }}
        order by 1
)

select * from forecast_table