INSERT INTO transactions (id_transaction, id_customer, id_product, nm_city, dt_created_at)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (id_transaction) DO UPDATE
SET
    id_customer = EXCLUDED.id_customer,
    id_product = EXCLUDED.id_product,
    nm_city = EXCLUDED.nm_city,
    dt_created_at = EXCLUDED.dt_created_at;