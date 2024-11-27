WITH unnested_table as (
    SELECT
        unnest(customers) customers
    FROM
        read_json_auto('data/ouch_hungry/{reference_date}/{source_file}')
)
SELECT
    customers.id as id,
    customers.first_name as first_name,
    customers.last_name as last_name,
    customers.email as email,
    customers.cep as cep,
    customers.ddd as ddd,
    customers.phone as phone,
    customers.created_at as created_at
FROM
    unnested_table