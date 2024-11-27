WITH unnested_table as (
    SELECT
        unnest(products) products
    FROM
        read_json_auto('data/ouch_hungry/{reference_date}/{source_file}')
)
SELECT
    products.id as id,
    products.description as description,
    products.category as category,
    products.ean as ean,
    products.price as price,
    products.created_at as created_at
FROM
    unnested_table