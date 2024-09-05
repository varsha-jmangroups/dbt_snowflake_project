with suppliers_data as (
    select
        *
    from {{ source('rawsource','SUPPLIERS')}}
)

select * from suppliers_data