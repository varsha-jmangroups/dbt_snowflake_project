with stores_data as (
    select
        *
    from {{ source('rawsource','STORES')}}
)

select * from stores_data