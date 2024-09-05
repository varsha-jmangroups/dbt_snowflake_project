with promotions_data as (
    select
        *
    from {{ source('rawsource','PROMOTIONS')}}
)

select * from promotions_data