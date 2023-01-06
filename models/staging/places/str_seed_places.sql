with

places as (
    select * from {{ ref('seed_places')}}
),

final as (
    select 
        code as place_code,
        name as place_name,
        administrativeDivision as municipality,
        countryCode as country_code
    from places
    where countryCode = 'LT'
)

select * from final