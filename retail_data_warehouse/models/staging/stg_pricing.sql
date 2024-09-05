with pricing_data as (
    select
        *
    from {{ source('rawsource','PRICING')}}
)

select * from pricing_data