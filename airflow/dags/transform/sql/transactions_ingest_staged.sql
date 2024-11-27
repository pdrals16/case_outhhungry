WITH unnested_table as (
    SELECT
        unnest(transactions) transactions
    FROM
        read_json_auto('data/ouch_hungry/{reference_date}/{source_file}')
)
SELECT
    transactions.id as id,
    transactions.customer_id as customer_id,
    transactions.product_id as product_id,
    transactions.city as city,
    transactions.created_at as created_at
FROM
    unnested_table