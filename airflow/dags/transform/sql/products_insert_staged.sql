INSERT INTO products (id, description, category, ean, price, created_at)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO UPDATE
SET
    description = EXCLUDED.description,
    category = EXCLUDED.category,
    ean = EXCLUDED.ean,
    price = EXCLUDED.price,
    created_at = EXCLUDED.created_at;