with customer_info as (
    select
        CUSTOMER_ID,
        FIRST_NAME || ' ' || LAST_NAME AS CUSTOMER_NAME,
        GENDER,
        DATE_OF_BIRTH,
        MEMBERSHIP_LEVEL
    from {{ ref('stg_customers') }}
)
, age_group as (
    select
        CUSTOMER_ID,
        CUSTOMER_NAME,
        GENDER,
        CASE
            WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE) < 18 THEN 'below 18'
            WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE) BETWEEN 18 AND 30 THEN '18-30'
            WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE) BETWEEN 31 AND 45 THEN '31-45'
            WHEN DATEDIFF(year, DATE_OF_BIRTH, CURRENT_DATE) BETWEEN 46 AND 60 THEN '46-60'
            ELSE 'above 60'
        END AS AGE_GROUP,
        MEMBERSHIP_LEVEL
    from customer_info
)
, order_summary as (
    select
        o.CUSTOMER_ID,
        COUNT(DISTINCT o.ORDER_ID) AS TOTAL_ORDERS,
        AVG(oli.TOTAL_PRICE) AS AVERAGE_ORDER_VALUE,
        SUM(CASE WHEN o.ORDER_STATUS = 'Returned' THEN oli.TOTAL_PRICE ELSE 0 END) AS TOTAL_RETURN_AMOUNT
    from {{ ref('stg_order_line_items') }} oli
    join {{ ref('stg_orders') }} o on oli.ORDER_ID = o.ORDER_ID
    group by o.CUSTOMER_ID
)
, number_of_returns as (
    select
        o.CUSTOMER_ID,
        COUNT(oli.ORDER_ID) AS NUMBER_OF_RETURNS
    from {{ ref('stg_order_line_items') }} oli
    join {{ ref('stg_orders') }} o on oli.ORDER_ID = o.ORDER_ID
    where o.ORDER_STATUS = 'Returned'
    group by o.CUSTOMER_ID
)
, last_order as (
    select
        CUSTOMER_ID,
        MAX(ORDER_DATE) AS LAST_ORDER_DATE
    from {{ ref('stg_orders') }}
    group by CUSTOMER_ID
)
, customer_lifetime as (
    select
        o.CUSTOMER_ID,
        SUM(oli.TOTAL_PRICE) AS CLV,
        DATEDIFF(year, MIN(o.ORDER_DATE), CURRENT_DATE) AS YEARS_WITH_COMPANY
    from {{ ref('stg_order_line_items') }} oli
    join {{ ref('stg_orders') }} o on oli.ORDER_ID = o.ORDER_ID
    group by o.CUSTOMER_ID
)
, total_discounts as (
    select
        o.CUSTOMER_ID,
        SUM(p.DISCOUNTED_PRICE - p.PRICE) AS TOTAL_DISCOUNT_USED
    from {{ ref('stg_order_line_items') }} oli
    join {{ ref('stg_orders') }} o on oli.ORDER_ID = o.ORDER_ID
    join {{ ref('stg_pricing') }} p on oli.PRODUCT_ID = p.PRODUCT_ID
    group by o.CUSTOMER_ID
)
, churn_status as (
    select
        CUSTOMER_ID,
        CASE
            WHEN MAX(ORDER_DATE) < DATEADD(year, -1, CURRENT_DATE) THEN 'Churned'
            ELSE 'Active'
        END AS CUSTOMER_CHURN
    from {{ ref('stg_orders') }}
    group by CUSTOMER_ID
)
select
    ci.CUSTOMER_ID,
    ci.CUSTOMER_NAME,
    ci.GENDER,
    ag.AGE_GROUP,
    ci.MEMBERSHIP_LEVEL,
    os.TOTAL_ORDERS,
    os.AVERAGE_ORDER_VALUE,
    COALESCE(nr.NUMBER_OF_RETURNS, 0) AS NUMBER_OF_RETURNS,
    lo.LAST_ORDER_DATE,
    clv.CLV AS CUSTOMER_LIFETIME_VALUE,
    clv.YEARS_WITH_COMPANY,
    COALESCE(td.TOTAL_DISCOUNT_USED, 0) AS TOTAL_DISCOUNT_USED,
    COALESCE(os.TOTAL_RETURN_AMOUNT, 0) AS TOTAL_RETURN_AMOUNT,
    cs.CUSTOMER_CHURN
from customer_info ci
left join age_group ag on ci.CUSTOMER_ID = ag.CUSTOMER_ID
left join order_summary os on ci.CUSTOMER_ID = os.CUSTOMER_ID
left join number_of_returns nr on ci.CUSTOMER_ID = nr.CUSTOMER_ID
left join last_order lo on ci.CUSTOMER_ID = lo.CUSTOMER_ID
left join customer_lifetime clv on ci.CUSTOMER_ID = clv.CUSTOMER_ID
left join total_discounts td on ci.CUSTOMER_ID = td.CUSTOMER_ID
left join churn_status cs on ci.CUSTOMER_ID = cs.CUSTOMER_ID
