with orders_data as(
    select
        *
    from {{ source('rawsource','ORDERS')}}
)

select * from orders_data