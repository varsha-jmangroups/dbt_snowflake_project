with products_data as (
    select
        *
    from {{ source('rawsource','PRODUCTS')}}
)

select * from products_data