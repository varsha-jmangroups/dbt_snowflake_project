with total_employees as (
    select
        STORE_ID,
        COUNT(DISTINCT EMPLOYEE_ID) AS TOTAL_EMPLOYEES
    from {{ ref('stg_empoyees') }} 
    group by STORE_ID
),
order_summary as (
    select
        o.STORE_ID,
        COUNT(DISTINCT o.ORDER_ID) AS TOTAL_ORDERS,
        SUM(oli.QUANTITY) AS TOTAL_UNITS_SOLD,
        SUM(oli.TOTAL_PRICE) AS TOTAL_SALES_AMOUNT
    from {{ ref('stg_order_line_items') }} oli
    join {{ ref('stg_orders') }} o on oli.ORDER_ID = o.ORDER_ID
    group by o.STORE_ID
),
total_customers as (
    select
        STORE_ID,
        COUNT(DISTINCT CUSTOMER_ID) AS TOTAL_CUSTOMERS
    from {{ ref('stg_orders') }}
    group by STORE_ID
),
top_selling_product as (
    select
        o.STORE_ID,
        p.PRODUCT_NAME AS TOP_SELLING_PRODUCT
    from {{ ref('stg_order_line_items') }} oli
    join {{ ref('stg_orders') }} o on oli.ORDER_ID = o.ORDER_ID
    join {{ ref('stg_products') }} p on oli.PRODUCT_ID = p.PRODUCT_ID
    group by o.STORE_ID, p.PRODUCT_NAME
    having COUNT(DISTINCT o.ORDER_ID) > 1
),
returns_summary as (
    select
        o.STORE_ID,
        SUM(CASE WHEN o.ORDER_STATUS = 'Returned' THEN oli.TOTAL_PRICE ELSE 0 END) AS TOTAL_RETURNS_AMOUNT
    from {{ ref('stg_order_line_items') }} oli
    join {{ ref('stg_orders') }} o on oli.ORDER_ID = o.ORDER_ID
    group by o.STORE_ID
),
avg_order_delivery_time as (
    select
        o.STORE_ID,
        AVG(DATEDIFF(day, o.ORDER_DATE, o.DELIVERY_DATE)) AS AVG_ORDER_DELIVERY_TIME
    from {{ ref('stg_orders') }} o
    group by o.STORE_ID
)

select
    s.STORE_ID,
    s.STORE_NAME,
    s.LOCATION,
    COALESCE(te.TOTAL_EMPLOYEES, 0) AS TOTAL_EMPLOYEES,
    COALESCE(os.TOTAL_UNITS_SOLD, 0) AS TOTAL_UNITS_SOLD,
    COALESCE(tc.TOTAL_CUSTOMERS, 0) AS TOTAL_CUSTOMERS,
    COALESCE(os.TOTAL_ORDERS, 0) AS TOTAL_ORDERS,
    COALESCE(rs.TOTAL_RETURNS_AMOUNT, 0) AS RETURNED_ORDER_COUNTS,
    COALESCE(os.TOTAL_SALES_AMOUNT, 0) AS TOTAL_SALES_AMOUNT,
    COALESCE(os.TOTAL_SALES_AMOUNT / NULLIF(os.TOTAL_ORDERS, 0), 0) AS AVERAGE_ORDER_VALUE,
    COALESCE(tp.TOP_SELLING_PRODUCT, '-') AS TOP_SELLING_PRODUCT,
    COALESCE(rs.TOTAL_RETURNS_AMOUNT, 0) AS TOTAL_RETURNS_AMOUNT,
    s.STORE_TYPE,
    COALESCE(ad.AVG_ORDER_DELIVERY_TIME, 0) AS AVERAGE_ORDER_DELIVER_TIME
from {{ ref('stg_stores') }} s
left join total_employees te on s.STORE_ID = te.STORE_ID
left join order_summary os on s.STORE_ID = os.STORE_ID
left join total_customers tc on s.STORE_ID = tc.STORE_ID
left join top_selling_product tp on s.STORE_ID = tp.STORE_ID
left join returns_summary rs on s.STORE_ID = rs.STORE_ID
left join avg_order_delivery_time ad on s.STORE_ID = ad.STORE_ID
