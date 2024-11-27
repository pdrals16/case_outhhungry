INSERT INTO transactions (id, customer_id, product_id, city, created_at)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (id) DO UPDATE
SET
    customer_id = EXCLUDED.customer_id,
    product_id = EXCLUDED.product_id,
    city = EXCLUDED.city,
    created_at = EXCLUDED.created_at