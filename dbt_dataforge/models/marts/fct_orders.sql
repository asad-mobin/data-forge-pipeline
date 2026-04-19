SELECT order_id, customer_id, order_status, purchase_ts, approved_ts, carrier_ts, delievered_ts, estimated_ts,
        DATEDIFF('day', purchase_ts, approved_ts) AS days_to_approval,
        DATEDIFF('day', approved_ts, delievered_ts) AS delivery_time,
        DATEDIFF('day', purchase_ts, delievered_ts) AS total_time,

        CASE
            WHEN delievered_ts IS NULL THEN 'not_delivered'
            ELSE 'delivered' END AS delivery_status
FROM {{ ref('stg_orders')}}
