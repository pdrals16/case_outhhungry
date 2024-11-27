INSERT INTO customers (id, first_name, last_name, email, cep, ddd, phone, created_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO UPDATE
SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    email = EXCLUDED.email,
    cep = EXCLUDED.cep,
    ddd = EXCLUDED.ddd,
    phone = EXCLUDED.phone,
    created_at = EXCLUDED.created_at;