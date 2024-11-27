INSERT INTO customers (id_customer, nm_first_name, nm_last_name, nm_email, cd_cep, cd_ddd, cd_phone, dt_created_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id_customer) DO UPDATE
SET
    nm_first_name = EXCLUDED.nm_first_name,
    nm_last_name = EXCLUDED.nm_last_name,
    nm_email = EXCLUDED.nm_email,
    cd_cep = EXCLUDED.cd_cep,
    cd_ddd = EXCLUDED.cd_ddd,
    cd_phone = EXCLUDED.cd_phone,
    dt_created_at = EXCLUDED.dt_created_at;