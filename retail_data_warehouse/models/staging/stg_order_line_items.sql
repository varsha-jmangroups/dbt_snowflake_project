with order_line_items_data as (
    select * from {{ source('rawsource','ORDER_LINE_ITEMS')}}
)

select * from order_line_items_data