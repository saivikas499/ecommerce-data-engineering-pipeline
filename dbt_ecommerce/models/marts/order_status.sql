WITH base AS (
    SELECT 
        o.order_id, 
        c.name AS customer_name, 
        o.order_date, 
        s.status,
        s.shipped_at, 
        s.delivered_at,
        -- Fixed: start_time (shipped_at) comes before end_time (delivered_at)
        DATEDIFF('hour', s.shipped_at, s.delivered_at) AS delivery_hours
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
    LEFT JOIN {{ ref('stg_shipments') }} s ON o.order_id = s.order_id
)

SELECT 
    *,
    CASE 
        WHEN status = 'Shipped' AND delivery_hours > 48 THEN 'Delayed'
        ELSE status 
    END AS final_status
FROM base