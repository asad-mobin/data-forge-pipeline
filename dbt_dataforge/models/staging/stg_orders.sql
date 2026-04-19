SELECT * FROM (
    SELECT order_id, customer_id, order_status,
            TO_TIMESTAMP(purchase_timestamp) AS purchase_ts,
            TO_TIMESTAMP(approved_at) AS approved_ts,
            TO_TIMESTAMP(delievered_carrier_date) AS carrier_ts,
            TO_TIMESTAMP(delievered_customer_date) AS delievered_ts,
            TO_TIMESTAMP(estimated_delivery_date) AS estimated_ts,

            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY purchase_timestamp DESC) AS rn
FROM {{ source('bronze', 'orders')}}
)
WHERE rn = 1

