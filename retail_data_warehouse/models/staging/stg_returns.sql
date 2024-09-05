with returns_data as (
    select
        *
    from {{ source('rawsource','RETURNS')}}
)

select * from returns_data