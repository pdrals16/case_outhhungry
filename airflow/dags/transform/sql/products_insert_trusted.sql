INSERT INTO products (id_product, nm_description, nm_category, cd_ean, vl_price, dt_created_at)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (id_product) DO UPDATE
SET
    nm_description = EXCLUDED.nm_description,
    nm_category = EXCLUDED.nm_category,
    cd_ean = EXCLUDED.cd_ean,
    vl_price = EXCLUDED.vl_price,
    dt_created_at = EXCLUDED.dt_created_at;