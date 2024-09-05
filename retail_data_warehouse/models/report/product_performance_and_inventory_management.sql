with product_info as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.cost,
        o.order_id,
        oli.unit_price,
        oli.quantity,
        o.order_date
    from {{ ref('stg_products') }} p
    join {{ ref('stg_order_line_items') }} oli on p.product_id = oli.product_id
    join {{ ref('stg_orders') }} o on oli.order_id = o.order_id
),
product_summary as (
    select
        product_id,
        product_name,
        category,
        sum(quantity) as total_units_sold,
        sum(unit_price * quantity) as total_sales_amount,
        avg(unit_price) as average_selling_price,
        min(order_date) as first_purchase_date,
        max(order_date) as recent_purchase_date
    from product_info
    group by product_id, product_name, category
),
return_summary as (
    select
        oli.product_id,
        sum(case when o.order_status = 'Returned' then oli.quantity else 0 end) as total_returns
    from {{ ref('stg_order_line_items') }} oli
    join {{ ref('stg_orders') }} o on oli.order_id = o.order_id
    group by oli.product_id
),
product_performance as (
    select
        ps.product_id,
        ps.product_name,
        ps.category,
        ps.total_units_sold,
        ps.total_sales_amount,
        ps.average_selling_price,
        coalesce(rs.total_returns, 0) as total_returns,
        ps.first_purchase_date,
        ps.recent_purchase_date,
        ps.total_sales_amount - (ps.total_units_sold * p.cost) as gross_margin
    from product_summary ps
    left join return_summary rs on ps.product_id = rs.product_id
    left join {{ ref('stg_products') }} p on ps.product_id = p.product_id
)
select * from product_performance
